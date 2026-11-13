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
package org.apache.iceberg.flink.sink.dynamic;

import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.Stream;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * POC: rough numbers for the dynamic sink's index-dv mode on RocksDB. Runs only when its
 * environment variables are set; laptop numbers.
 */
@Timeout(value = 3600)
class DynamicIndexDvBenchmark {
  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  private static final int PARALLELISM = 2;
  private static final int MAX_PARALLELISM = 128;
  private static final int LOAD_CHUNK = 250_000;
  private static final int WARM_KEYS_PER_TABLE = 64;
  private static final TypeInformation<Row> INPUT_TYPE =
      Types.ROW(Types.STRING, Types.INT, Types.STRING, Types.INT);

  @TempDir private Path checkpointDir;
  @TempDir private Path savepointDir;
  @TempDir private Path loadDir;
  private MiniCluster cluster;

  @BeforeEach
  void startCluster() throws Exception {
    Configuration config =
        new Configuration(MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG);
    config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("1024m"));
    config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
    config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
    config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toUri().toString());
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
    TestStreamEnvironment.unsetAsContext();
    cluster.close();
  }

  /**
   * {@code DYN_BENCH_KEYS} keys in each of {@code DYN_BENCH_TABLES} tables (default 3), then random
   * and clustered upserts of a tenth of all keys each, through the dynamic sink.
   */
  @Test
  void dynamicTables() throws Exception {
    String keysEnv = System.getenv("DYN_BENCH_KEYS");
    assumeThat(keysEnv).isNotNull();
    int keys = Integer.parseInt(keysEnv);
    int tables = Integer.parseInt(System.getenv().getOrDefault("DYN_BENCH_TABLES", "3"));
    int updates = keys * tables / 10;
    List<String> names = Lists.newArrayList();
    long loadStart = System.nanoTime();
    for (int i = 0; i < tables; i++) {
      String name = "bench_" + i;
      names.add(name);
      load(name, keys);
    }

    long loadMillis = (System.nanoTime() - loadStart) / 1_000_000;
    PhasedUpserts.reset();
    DynamicIndexDvDataWriter.BOOTSTRAPPED_KEYS.set(0);
    DynamicIndexDvDataWriter.TABLE_BOOTSTRAPS.set(0);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(PARALLELISM);
    env.setMaxParallelism(MAX_PARALLELISM);
    env.enableCheckpointing(1000L);
    DynamicIcebergSink.forInput(
            env.addSource(new PhasedUpserts(names, keys, updates), INPUT_TYPE)
                .uid("source")
                .setParallelism(1))
        .generator(new TestDynamicIcebergSinkIndexDv.Generator(""))
        .catalogLoader(CATALOG_EXTENSION.catalogLoader())
        .immediateTableUpdate(true)
        .writeParallelism(PARALLELISM)
        .uidPrefix("bench")
        .set(FlinkWriteOptions.UPSERT_MODE.key(), "index-dv")
        .append();
    JobClient job = env.executeAsync("dynamic index-dv benchmark");
    while (!PhasedUpserts.EMITTED_ALL.get()) {
      if (job.getJobStatus().get().isTerminalState()) {
        job.getJobExecutionResult().get();
        throw new IllegalStateException("Job ended early");
      }

      Thread.sleep(50);
    }

    Path nativeDir = savepointDir.resolve("native");
    long savepointStart = System.nanoTime();
    String nativeSavepoint =
        job.triggerSavepoint(nativeDir.toString(), SavepointFormatType.NATIVE)
            .get(600, TimeUnit.SECONDS);
    long savepointMillis = (System.nanoTime() - savepointStart) / 1_000_000;
    long nativeBytes = directoryBytes(Path.of(nativeSavepoint.replace("file:", "")));
    job.cancel().get(60, TimeUnit.SECONDS);

    boolean rowsOk = true;
    for (String name : names) {
      rowsOk &= countRows(name) == keys;
    }

    System.out.printf(
        "DYNBENCH tables=%d keysPerTable=%d updatesPerPhase=%d loadMs=%d warmMs=%d"
            + " tableBootstraps=%d bootstrappedKeys=%d randomMs=%d randomPerS=%d clusteredMs=%d"
            + " clusteredPerS=%d nativeSavepointMs=%d nativeSavepointBytes=%d bytesPerKey=%.1f"
            + " rowsOk=%s%n",
        tables,
        keys,
        updates,
        loadMillis,
        PhasedUpserts.PHASE_MILLIS.get(0),
        DynamicIndexDvDataWriter.TABLE_BOOTSTRAPS.get(),
        DynamicIndexDvDataWriter.BOOTSTRAPPED_KEYS.get(),
        PhasedUpserts.PHASE_MILLIS.get(1),
        updates * 1000L / Math.max(1, PhasedUpserts.PHASE_MILLIS.get(1)),
        PhasedUpserts.PHASE_MILLIS.get(2),
        updates * 1000L / Math.max(1, PhasedUpserts.PHASE_MILLIS.get(2)),
        savepointMillis,
        nativeBytes,
        nativeBytes / (double) ((long) keys * tables),
        rowsOk);
    assertThat(rowsOk).isTrue();
  }

  /**
   * State bytes per key in RocksDB for different key encodings: {@code KEY_BENCH_KEYS} keys in a
   * keyed {@code ValueState<Long>}, measured as the size of a native savepoint.
   */
  @Test
  void keyEncodingStateSize() throws Exception {
    String keysEnv = System.getenv("KEY_BENCH_KEYS");
    assumeThat(keysEnv).isNotNull();
    int keys = Integer.parseInt(keysEnv);
    for (String encoding :
        ImmutableList.of("int", "int-table-id", "int-no-table", "long", "uuid", "uuid-hash64")) {
      KeySource.EMITTED_ALL.set(false);
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(PARALLELISM);
      env.setMaxParallelism(MAX_PARALLELISM);
      env.enableCheckpointing(1000L);
      env.addSource(
              new KeySource(encoding, keys), TypeInformation.of(SerializedEqualityValues.class))
          .setParallelism(1)
          .keyBy(key -> key, TypeInformation.of(SerializedEqualityValues.class))
          .process(new StoreLocation())
          .setParallelism(PARALLELISM);
      JobClient job = env.executeAsync("key encoding " + encoding);
      while (!KeySource.EMITTED_ALL.get()) {
        Thread.sleep(50);
      }

      Thread.sleep(2000);
      Path dir = savepointDir.resolve("keys-" + encoding);
      String savepoint =
          job.triggerSavepoint(dir.toString(), SavepointFormatType.NATIVE)
              .get(600, TimeUnit.SECONDS);
      long bytes = directoryBytes(Path.of(savepoint.replace("file:", "")));
      job.cancel().get(60, TimeUnit.SECONDS);
      System.out.printf(
          "KEYBENCH encoding=%s keys=%d rawKeyBytes=%d nativeSavepointBytes=%d bytesPerKey=%.1f%n",
          encoding,
          keys,
          KeySource.encode(encoding, 1).data().length,
          bytes,
          bytes / (double) keys);
    }
  }

  private void load(String name, int keys) throws IOException {
    Table table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TableIdentifier.of(DATABASE, name),
                TestDynamicIcebergSinkIndexDv.SCHEMA_V1,
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

  private static long directoryBytes(Path dir) throws IOException {
    try (Stream<Path> paths = Files.walk(dir)) {
      return paths.filter(Files::isRegularFile).mapToLong(p -> p.toFile().length()).sum();
    }
  }

  /**
   * Waits for a checkpoint, warms every table on every subtask, then emits a random and a clustered
   * phase of upserts, each in two batches separated by two completed checkpoints.
   */
  private static class PhasedUpserts implements SourceFunction<Row>, CheckpointListener {
    private static final AtomicBoolean EMITTED_ALL = new AtomicBoolean();
    private static final AtomicLongArray PHASE_MILLIS = new AtomicLongArray(3);

    private final List<String> tables;
    private final int keys;
    private final int updates;
    private final AtomicInteger completedCheckpoints = new AtomicInteger();
    private volatile boolean running = true;

    PhasedUpserts(List<String> tables, int keys, int updates) {
      this.tables = tables;
      this.keys = keys;
      this.updates = updates;
    }

    static void reset() {
      EMITTED_ALL.set(false);
      for (int i = 0; i < 3; i++) {
        PHASE_MILLIS.set(i, -1);
      }
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
      awaitCheckpoints(1);
      long start = System.nanoTime();
      synchronized (ctx.getCheckpointLock()) {
        for (String table : tables) {
          for (int id = 0; id < WARM_KEYS_PER_TABLE; id++) {
            ctx.collect(Row.ofKind(RowKind.UPDATE_AFTER, table, id, "warm-" + id, 1));
          }
        }
      }

      awaitCheckpoints(2);
      PHASE_MILLIS.set(0, (System.nanoTime() - start) / 1_000_000);

      Random random = new Random(keys);
      for (int phase = 1; phase <= 2 && running; phase++) {
        start = System.nanoTime();
        int perBatch = updates / 2;
        for (int batch = 0; batch < 2 && running; batch++) {
          synchronized (ctx.getCheckpointLock()) {
            for (int i = 0; i < perBatch; i++) {
              int seq = batch * perBatch + i;
              String table =
                  tables.get(phase == 1 ? random.nextInt(tables.size()) : seq % tables.size());
              int id =
                  phase == 1
                      ? random.nextInt(keys)
                      : keys / 2 - updates / (2 * tables.size()) + seq / tables.size();
              ctx.collect(Row.ofKind(RowKind.UPDATE_AFTER, table, id, "p" + phase + "-" + seq, 1));
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

    private void awaitCheckpoints(int count) throws InterruptedException {
      int target = completedCheckpoints.get() + count;
      while (running && completedCheckpoints.get() < target) {
        Thread.sleep(5);
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

  /** Emits each key once in the given encoding, then idles until cancelled. */
  private static class KeySource implements SourceFunction<SerializedEqualityValues> {
    private static final AtomicBoolean EMITTED_ALL = new AtomicBoolean();
    private static final byte[] TABLE_ID =
        DynamicIndexDvKeys.prefix(DynamicIndexDvKeys.tableId(UUID.randomUUID(), "main"));

    /** Key format 1: the table name and branch in front of the equality key. */
    private static SerializedEqualityValues namePrefixed(SerializedEqualityValues key) {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(bytes)) {
        out.writeUTF("default.events");
        out.writeUTF("main");
        out.write(key.data());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      return new SerializedEqualityValues(bytes.toByteArray());
    }

    private static final StructType INT_KEY =
        StructType.of(NestedField.required(1, "id", IntegerType.get()));
    private static final StructType LONG_KEY =
        StructType.of(NestedField.required(1, "id", LongType.get()));
    private static final StructType STRING_KEY =
        StructType.of(NestedField.required(1, "id", StringType.get()));

    private final String encoding;
    private final int keys;
    private volatile boolean running = true;

    KeySource(String encoding, int keys) {
      this.encoding = encoding;
      this.keys = keys;
    }

    static SerializedEqualityValues encode(String encoding, int id) {
      StructLikeSerializer serializer = new StructLikeSerializer();
      return switch (encoding) {
        case "int" -> namePrefixed(serializer.serializeKey(new Single(id), INT_KEY));
        case "int-no-table" -> serializer.serializeKey(new Single(id), INT_KEY);
        case "int-table-id" ->
            DynamicIndexDvKeys.compose(TABLE_ID, serializer.serializeKey(new Single(id), INT_KEY));
        case "long" -> namePrefixed(serializer.serializeKey(new Single((long) id << 20), LONG_KEY));
        case "uuid" -> namePrefixed(serializer.serializeKey(new Single(uuid(id)), STRING_KEY));
        case "uuid-hash64" ->
            new SerializedEqualityValues(
                Arrays.copyOf(
                    Hashing.murmur3_128()
                        .hashBytes(
                            namePrefixed(serializer.serializeKey(new Single(uuid(id)), STRING_KEY))
                                .data())
                        .asBytes(),
                    8));
        default -> throw new IllegalArgumentException(encoding);
      };
    }

    private static String uuid(int id) {
      return new UUID(new Random(id).nextLong(), id).toString();
    }

    @Override
    public void run(SourceContext<SerializedEqualityValues> ctx) throws Exception {
      for (int id = 0; id < keys && running; id++) {
        SerializedEqualityValues key = encode(encoding, id);
        synchronized (ctx.getCheckpointLock()) {
          ctx.collect(key);
        }
      }

      EMITTED_ALL.set(true);
      while (running) {
        Thread.sleep(10);
      }
    }

    @Override
    public void cancel() {
      this.running = false;
    }
  }

  /** A one-field struct for key serialization. */
  private record Single(Object value) implements StructLike {
    @Override
    public int size() {
      return 1;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(value);
    }

    @Override
    public <T> void set(int pos, T newValue) {
      throw new UnsupportedOperationException();
    }
  }

  /** Stores a packed (file id, position) per key, like the index. */
  private static class StoreLocation
      extends KeyedProcessFunction<SerializedEqualityValues, SerializedEqualityValues, Long> {
    private transient ValueState<Long> location;
    private transient long counter;

    @Override
    public void open(OpenContext openContext) {
      this.location =
          getRuntimeContext().getState(new ValueStateDescriptor<>("location", Types.LONG));
    }

    @Override
    public void processElement(SerializedEqualityValues key, Context ctx, Collector<Long> out)
        throws IOException {
      location.update(((long) (counter / 1000) << 32) | (counter % 1000));
      counter++;
    }
  }
}
