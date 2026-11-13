/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink;

import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * POC: single-table and dynamic-sink index-dv on one harness: same loading, phases, update volume,
 * key distribution, parallelism and checkpoint waits. Runs only when {@code TP_KEYS} is set.
 *
 * <p>Variants ({@code TP_VARIANTS}): {@code single} (IcebergSink), {@code single-prefix}
 * (IcebergSink with the dynamic sink's table and branch key prefix), {@code dynamic-1} and {@code
 * dynamic-3} (DynamicIcebergSink over one or three tables holding {@code TP_KEYS} keys in total).
 */
@Timeout(value = 3600)
class IndexDvThroughputComparison {
  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  private static final int PARALLELISM = 2;
  private static final int MAX_PARALLELISM = 128;
  private static final long CHECKPOINT_INTERVAL_MS = 200L;
  private static final int LOAD_CHUNK = 250_000;
  private static final int WARM_KEYS_PER_TABLE = 64;
  private static final String[] PHASES = {"warm", "random", "clustered"};
  private static final Schema SCHEMA =
      new Schema(
          ImmutableList.of(
              NestedField.required(1, "id", IntegerType.get()),
              NestedField.optional(2, "data", StringType.get())),
          ImmutableSet.of(1));
  private static final TypeInformation<Row> INPUT_TYPE =
      Types.ROW(Types.STRING, Types.INT, Types.STRING);

  @TempDir private Path checkpointDir;
  @TempDir private Path loadDir;
  private MiniCluster cluster;
  private InMemoryReporter reporter;

  @BeforeEach
  void startCluster() throws Exception {
    Configuration config =
        new Configuration(MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG);
    config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("1024m"));
    config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
    config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
    config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toUri().toString());
    this.reporter = InMemoryReporter.create();
    reporter.addToConfiguration(config);
    this.cluster =
        new MiniCluster(
            new MiniClusterConfiguration.Builder()
                .setConfiguration(config)
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(PARALLELISM * 2)
                .build());
    cluster.start();
    TestStreamEnvironment.setAsContext(cluster, PARALLELISM);
  }

  @AfterEach
  void stopCluster() throws Exception {
    IndexDvKeySelector.setTestKeyPrefix(null);
    TestStreamEnvironment.unsetAsContext();
    cluster.close();
  }

  @Test
  void compare() throws Exception {
    String keysEnv = System.getenv("TP_KEYS");
    assumeThat(keysEnv).isNotNull();
    int keys = Integer.parseInt(keysEnv);
    int rounds = Integer.parseInt(System.getenv().getOrDefault("TP_ROUNDS", "2"));
    List<String> variants =
        ImmutableList.copyOf(
            System.getenv()
                .getOrDefault("TP_VARIANTS", "single,single-prefix,dynamic-1,dynamic-3")
                .split(","));
    for (int round = 1; round <= rounds; round++) {
      for (String variant : variants) {
        runVariant(variant, round, keys);
      }
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void runVariant(String variant, int round, int totalKeys) throws Exception {
    int tableCount = "dynamic-3".equals(variant) ? 3 : 1;
    int keysPerTable = totalKeys / tableCount;
    int updates =
        Integer.parseInt(
            System.getenv().getOrDefault("TP_UPDATES", Integer.toString(totalKeys / 10)));
    List<String> tables = Lists.newArrayList();
    for (int i = 0; i < tableCount; i++) {
      String name = "tp_" + variant.replace('-', '_') + "_" + round + "_" + i;
      tables.add(name);
      load(name, keysPerTable);
    }

    Phases.reset();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(PARALLELISM);
    env.setMaxParallelism(MAX_PARALLELISM);
    env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);
    DataStream<Row> input =
        env.addSource(new Phases(tables, keysPerTable, updates), INPUT_TYPE)
            .uid("source")
            .setParallelism(1);
    if (variant.startsWith("single")) {
      if ("single-prefix".equals(variant)) {
        IndexDvKeySelector.setTestKeyPrefix(prefix("default." + tables.get(0)));
      }

      DataStream<RowData> rows =
          input
              .map(new ToRowData(), InternalTypeInfo.of(FlinkSchemaUtil.convert(SCHEMA)))
              .setParallelism(1);
      IcebergSink.forRowData(rows)
          .tableLoader(
              TableLoader.fromCatalog(
                  CATALOG_EXTENSION.catalogLoader(), TableIdentifier.of(DATABASE, tables.get(0))))
          .writeParallelism(PARALLELISM)
          .equalityFieldColumns(ImmutableList.of("id"))
          .upsert(true)
          .uidSuffix("tp")
          .set(FlinkWriteOptions.UPSERT_MODE.key(), "index-dv")
          .append();
    } else {
      DynamicIcebergSink.forInput(input)
          .generator(new Generator())
          .catalogLoader(CATALOG_EXTENSION.catalogLoader())
          .immediateTableUpdate(true)
          .writeParallelism(PARALLELISM)
          .uidPrefix("tp")
          .set(FlinkWriteOptions.UPSERT_MODE.key(), "index-dv")
          .append();
    }

    JobClient job = env.executeAsync("throughput " + variant);
    OperatorSampler sampler = new OperatorSampler(reporter, job.getJobID());
    Thread samplerThread = new Thread(sampler, "operator-sampler");
    samplerThread.start();
    while (!Phases.EMITTED_ALL.get()) {
      if (job.getJobStatus().get().isTerminalState()) {
        job.getJobExecutionResult().get();
        throw new IllegalStateException("Job ended early");
      }

      Thread.sleep(20);
    }

    sampler.stop();
    samplerThread.join();
    job.cancel().get();
    IndexDvKeySelector.setTestKeyPrefix(null);

    boolean rowsOk = true;
    for (String table : tables) {
      rowsOk &= countRows(table) == keysPerTable;
    }

    for (int phase = 0; phase < PHASES.length; phase++) {
      long millis = Phases.PHASE_MILLIS.get(phase);
      int records = phase == 0 ? WARM_KEYS_PER_TABLE * tableCount : updates;
      System.out.printf(
          "TP variant=%s round=%d phase=%s keys=%d tables=%d records=%d ms=%d perS=%d"
              + " fromEpochMs=%d toEpochMs=%d rowsOk=%s%n",
          variant,
          round,
          PHASES[phase],
          totalKeys,
          tableCount,
          records,
          millis,
          records * 1000L / Math.max(1, millis),
          Phases.PHASE_START_EPOCH_MS.get(phase),
          Phases.PHASE_START_EPOCH_MS.get(phase) + millis,
          rowsOk);
    }

    sampler.print(variant, round);
    assertThat(rowsOk).isTrue();
  }

  private static byte[] prefix(String table) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeUTF(table);
      out.writeUTF(SnapshotRef.MAIN_BRANCH);
    }

    return bytes.toByteArray();
  }

  private void load(String name, int keys) throws IOException {
    Table table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TableIdentifier.of(DATABASE, name),
                SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
    GenericAppenderHelper appender = new GenericAppenderHelper(table, FileFormat.PARQUET, loadDir);
    for (int start = 0; start < keys; start += LOAD_CHUNK) {
      List<Record> records = Lists.newArrayList();
      for (int id = start; id < Math.min(keys, start + LOAD_CHUNK); id++) {
        Record record = GenericRecord.create(table.schema());
        record.setField("id", id);
        record.setField("data", "initial-" + id);
        records.add(record);
      }

      appender.appendToTable(records);
    }
  }

  private static long countRows(String name) throws IOException {
    Table table = CATALOG_EXTENSION.catalog().loadTable(TableIdentifier.of(DATABASE, name));
    long rows = 0;
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).select("id").build()) {
      for (Record ignored : records) {
        rows++;
      }
    }

    return rows;
  }

  private static class ToRowData implements MapFunction<Row, RowData> {
    @Override
    public RowData map(Row row) {
      return GenericRowData.ofKind(
          row.getKind(), row.getField(1), StringData.fromString((String) row.getField(2)));
    }
  }

  private static class Generator implements DynamicRecordGenerator<Row> {
    @Override
    public void generate(Row row, Collector<DynamicRecord> out) {
      DynamicRecord record =
          new DynamicRecord(
              TableIdentifier.of(DATABASE, (String) row.getField(0)),
              SnapshotRef.MAIN_BRANCH,
              SCHEMA,
              GenericRowData.ofKind(
                  row.getKind(), row.getField(1), StringData.fromString((String) row.getField(2))),
              PartitionSpec.unpartitioned(),
              DistributionMode.NONE,
              PARALLELISM * 2);
      record.setUpsertMode(true);
      record.setEqualityFields(ImmutableSet.of("id"));
      out.collect(record);
    }
  }

  /**
   * Waits for a checkpoint, warms every table on every subtask, then emits a random and a clustered
   * phase of upserts over all keys, each in two batches followed by two completed checkpoints. A
   * phase is timed from its first record to its last checkpoint.
   */
  private static class Phases implements SourceFunction<Row>, CheckpointListener {
    private static final AtomicBoolean EMITTED_ALL = new AtomicBoolean();
    private static final AtomicLongArray PHASE_MILLIS = new AtomicLongArray(3);
    private static final AtomicLongArray PHASE_START_EPOCH_MS = new AtomicLongArray(3);

    private final List<String> tables;
    private final int keysPerTable;
    private final int updates;
    private final AtomicInteger completedCheckpoints = new AtomicInteger();
    private volatile boolean running = true;

    Phases(List<String> tables, int keysPerTable, int updates) {
      this.tables = tables;
      this.keysPerTable = keysPerTable;
      this.updates = updates;
    }

    static void reset() {
      EMITTED_ALL.set(false);
      for (int i = 0; i < 3; i++) {
        PHASE_MILLIS.set(i, -1);
        PHASE_START_EPOCH_MS.set(i, -1);
      }
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
      awaitCheckpoints(1);
      start(0);
      long start = System.nanoTime();
      synchronized (ctx.getCheckpointLock()) {
        for (String table : tables) {
          for (int id = 0; id < WARM_KEYS_PER_TABLE; id++) {
            ctx.collect(Row.ofKind(RowKind.UPDATE_AFTER, table, id, "warm-" + id));
          }
        }
      }

      awaitCheckpoints(2);
      PHASE_MILLIS.set(0, (System.nanoTime() - start) / 1_000_000);

      Random random = new Random(42);
      int totalKeys = keysPerTable * tables.size();
      int clusteredStart = totalKeys / 2 - updates / 2;
      for (int phase = 1; phase <= 2 && running; phase++) {
        start(phase);
        start = System.nanoTime();
        int perBatch = updates / 2;
        for (int batch = 0; batch < 2 && running; batch++) {
          synchronized (ctx.getCheckpointLock()) {
            for (int i = 0; i < perBatch; i++) {
              // the same global key sequence for every variant, mapped onto its tables
              int global =
                  phase == 1 ? random.nextInt(totalKeys) : clusteredStart + batch * perBatch + i;
              String table = tables.get(global % tables.size());
              ctx.collect(
                  Row.ofKind(
                      RowKind.UPDATE_AFTER,
                      table,
                      global / tables.size(),
                      "p" + phase + "-" + global));
            }
          }

          awaitCheckpoints(2);
        }

        PHASE_MILLIS.set(phase, (System.nanoTime() - start) / 1_000_000);
      }

      EMITTED_ALL.set(true);
      while (running) {
        Thread.sleep(10);
      }
    }

    private static void start(int phase) {
      PHASE_START_EPOCH_MS.set(phase, System.currentTimeMillis());
    }

    private void awaitCheckpoints(int count) throws InterruptedException {
      int target = completedCheckpoints.get() + count;
      while (running && completedCheckpoints.get() < target) {
        Thread.sleep(2);
      }
    }

    @Override
    public void cancel() {
      this.running = false;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      completedCheckpoints.incrementAndGet();
    }
  }

  /**
   * Samples busy and back-pressured time per task (operator chain) every 200 ms while the random
   * and clustered phases run.
   */
  private static final class OperatorSampler implements Runnable {
    private final InMemoryReporter reporter;
    private final JobID jobId;
    private final Map<String, double[]> busy = Maps.newTreeMap();
    private final Map<String, double[]> backPressured = Maps.newTreeMap();
    private final Map<String, double[]> idle = Maps.newTreeMap();
    private volatile boolean running = true;

    private OperatorSampler(InMemoryReporter reporter, JobID jobId) {
      this.reporter = reporter;
      this.jobId = jobId;
    }

    @Override
    public void run() {
      while (running) {
        boolean inUpdates =
            Phases.PHASE_START_EPOCH_MS.get(1) > 0 && Phases.PHASE_MILLIS.get(2) < 0;
        if (inUpdates) {
          sample();
        }

        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          return;
        }
      }
    }

    private void sample() {
      try {
        for (Map.Entry<MetricGroup, Map<String, Metric>> group :
            reporter.getMetricsByGroup().entrySet()) {
          Map<String, String> variables = group.getKey().getAllVariables();
          if (!jobId.toString().equals(variables.get("<job_id>"))
              || variables.containsKey("<operator_name>")) {
            continue;
          }

          String task = variables.getOrDefault("<task_name>", "?");
          // Flink's view updater refreshes these every 5 s; updating them here skews them
          for (Map.Entry<String, Metric> entry : group.getValue().entrySet()) {
            Map<String, double[]> target =
                switch (entry.getKey()) {
                  case "busyTimeMsPerSecond" -> busy;
                  case "backPressuredTimeMsPerSecond" -> backPressured;
                  case "idleTimeMsPerSecond" -> idle;
                  default -> null;
                };
            if (target == null) {
              continue;
            }

            if (entry.getValue() instanceof Gauge<?> gauge
                && gauge.getValue() instanceof Number number
                && !Double.isNaN(number.doubleValue())) {
              double[] stats = target.computeIfAbsent(task, unused -> new double[3]);
              stats[0] += number.doubleValue();
              stats[1] += 1;
              stats[2] = Math.max(stats[2], number.doubleValue());
            }
          }
        }
      } catch (RuntimeException e) {
        // shortcut: the registry is read while tasks register metrics; skip the sample
      }
    }

    private void stop() {
      this.running = false;
    }

    private void print(String variant, int round) {
      for (Map.Entry<String, double[]> entry : busy.entrySet()) {
        double[] bp = backPressured.getOrDefault(entry.getKey(), new double[3]);
        double[] idleStats = idle.getOrDefault(entry.getKey(), new double[3]);
        System.out.printf(
            "TPOP variant=%s round=%d task=\"%s\" busyAvg=%.0f busyMax=%.0f backPressuredAvg=%.0f"
                + " idleAvg=%.0f%n",
            variant,
            round,
            entry.getKey(),
            entry.getValue()[0] / Math.max(1, entry.getValue()[1]),
            entry.getValue()[2],
            bp[0] / Math.max(1, bp[1]),
            idleStats[0] / Math.max(1, idleStats[1]));
      }
    }
  }
}
