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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.OperatingSystemMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.Stream;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;
import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.checkpoint.TaskStateStats;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.maintenance.api.FlinkMaintenanceConfig;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * POC: rough local scale benchmark of the index-dv and equality-delete upsert modes on RocksDB with
 * incremental checkpoints. Runs only when {@code INDEX_DV_BENCH_KEYS} is set to the number of keys.
 */
@Timeout(value = 7200)
class IndexDvScaleBenchmark {
  static {
    // The converter logs one INFO line per newly indexed key; at millions of keys the test output
    // stream, not the job, dominates wall time and heap.
    System.setProperty(
        "org.slf4j.simpleLogger.log.org.apache.iceberg.flink.maintenance.operator."
            + "EqualityConvertPKIndex",
        "warn");
  }

  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  private static final int PARALLELISM = 2;
  private static final int ROUNDS = 2;
  private static final int BATCHES_PER_PHASE = 2;
  private static final long CHECKPOINT_INTERVAL_MS = 1000L;
  private static final long TARGET_FILE_SIZE = 8L * 1024 * 1024;

  @TempDir private Path checkpointDir;
  @TempDir private Path savepointDir;
  private MiniCluster cluster;
  private InMemoryReporter reporter;

  @BeforeEach
  void startCluster() throws Exception {
    Configuration config =
        new Configuration(MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG);
    String managedMb = System.getenv().getOrDefault("INDEX_DV_BENCH_MANAGED_MB", "1024");
    config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse(managedMb + "m"));
    config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
    config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
    config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toUri().toString());
    config.set(WebOptions.CHECKPOINTS_HISTORY_SIZE, 1000);
    for (String metric :
        ImmutableList.of("block-cache-usage", "estimate-live-data-size", "size-all-mem-tables")) {
      config.set(
          ConfigOptions.key("state.backend.rocksdb.metrics." + metric)
              .booleanType()
              .noDefaultValue(),
          true);
    }

    this.reporter = InMemoryReporter.create();
    reporter.addToConfiguration(config);
    this.cluster =
        new MiniCluster(
            new MiniClusterConfiguration.Builder()
                .setConfiguration(config)
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(PARALLELISM)
                .build());
    cluster.start();
    TestStreamEnvironment.setAsContext(cluster, PARALLELISM);
  }

  @AfterEach
  void stopCluster() throws Exception {
    TestStreamEnvironment.unsetAsContext();
    cluster.close();
  }

  @Test
  void compareAtScale() throws Exception {
    String keysEnv = System.getenv("INDEX_DV_BENCH_KEYS");
    assumeThat(keysEnv).isNotNull();
    int keys = Integer.parseInt(keysEnv);
    int updates = keys / 10;
    String modesEnv = System.getenv("INDEX_DV_BENCH_MODES");
    List<String> modes =
        modesEnv != null
            ? ImmutableList.copyOf(modesEnv.split(","))
            : ImmutableList.of("equality-delete", "index-dv");
    String roundsEnv = System.getenv("INDEX_DV_BENCH_ROUNDS");
    int rounds = roundsEnv != null ? Integer.parseInt(roundsEnv) : ROUNDS;
    for (int round = 1; round <= rounds; round++) {
      for (String mode : modes) {
        runRound(round, mode, keys, updates);
      }
    }
  }

  private void runRound(int round, String mode, int keys, int updates) throws Exception {
    String name = "scale_" + mode.replace('-', '_') + "_" + round;
    Table table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TableIdentifier.of(DATABASE, name),
                SimpleDataUtil.SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(
                    TableProperties.FORMAT_VERSION,
                    "3",
                    TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
                    String.valueOf(TARGET_FILE_SIZE)));

    long loadStart = System.nanoTime();
    loadAppendOnly(name, keys);
    long loadMs = (System.nanoTime() - loadStart) / 1_000_000;

    IndexDvDataWriter.LAST_BOOTSTRAP_MILLIS.set(-1);
    IndexDvDataWriter.BOOTSTRAPPED_KEYS.set(0);
    long snapshotBefore = table.currentSnapshot().snapshotId();
    UpdatePhasesSource.reset();
    JobID jobId = runUpserts(name, mode, keys, updates);
    table.refresh();

    List<AbstractCheckpointStats> checkpoints = completedCheckpoints(jobId);
    printCheckpoints(jobId, mode, round, checkpoints);
    System.out.printf(
        "PHASES round=%d mode=%s randomStartAtMs=%d clusteredStartAtMs=%d%n",
        round,
        mode,
        UpdatePhasesSource.PHASE_START_EPOCH_MS.get(0)
            - (checkpoints.isEmpty() ? 0 : checkpoints.get(0).getTriggerTimestamp()),
        UpdatePhasesSource.PHASE_START_EPOCH_MS.get(1)
            - (checkpoints.isEmpty() ? 0 : checkpoints.get(0).getTriggerTimestamp()));
    long[] durations =
        checkpoints.stream()
            .mapToLong(AbstractCheckpointStats::getEndToEndDuration)
            .sorted()
            .toArray();
    long[] incremental =
        checkpoints.stream()
            .mapToLong(AbstractCheckpointStats::getCheckpointedSize)
            .sorted()
            .toArray();
    long fullState =
        checkpoints.stream().mapToLong(AbstractCheckpointStats::getStateSize).max().orElse(0);

    List<Integer> dvsPerCommit = Lists.newArrayList();
    List<Integer> eqDeletesPerCommit = Lists.newArrayList();
    for (Snapshot snapshot : table.snapshots()) {
      if (snapshot.snapshotId() != snapshotBefore && snapshot.sequenceNumber() > 1) {
        int added = Integer.parseInt(snapshot.summary().getOrDefault("added-delete-files", "0"));
        int addedDvs = Integer.parseInt(snapshot.summary().getOrDefault("added-dvs", "0"));
        if (added > 0) {
          dvsPerCommit.add(addedDvs);
          eqDeletesPerCommit.add(added - addedDvs);
        }
      }
    }

    long planStart = System.nanoTime();
    int dataFiles = 0;
    int eqDeleteRefs = 0;
    int dvRefs = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        dataFiles++;
        eqDeleteRefs +=
            (int)
                task.deletes().stream()
                    .filter(d -> d.content() == FileContent.EQUALITY_DELETES)
                    .count();
        dvRefs +=
            (int)
                task.deletes().stream()
                    .filter(d -> d.content() == FileContent.POSITION_DELETES)
                    .count();
      }
    }

    long planMs = (System.nanoTime() - planStart) / 1_000_000;
    long scanStart = System.nanoTime();
    long rows = countRows(table);
    long scanMs = (System.nanoTime() - scanStart) / 1_000_000;
    assertThat(rows).isEqualTo(keys);

    System.out.printf(
        "SCALE round=%d mode=%s keys=%d updates=%d loadMs=%d bootstrapMs=%d bootstrappedKeys=%d "
            + "randomPhaseMs=%d clusteredPhaseMs=%d checkpoints=%d ckptMedianMs=%d ckptMaxMs=%d "
            + "incrMedianBytes=%d incrMaxBytes=%d fullStateMaxBytes=%d dvsPerCommit=%s "
            + "eqDeletesPerCommit=%s planMs=%d scanMs=%d dataFiles=%d eqDeleteRefs=%d dvRefs=%d%n",
        round,
        mode,
        keys,
        updates,
        loadMs,
        IndexDvDataWriter.LAST_BOOTSTRAP_MILLIS.get(),
        IndexDvDataWriter.BOOTSTRAPPED_KEYS.get(),
        UpdatePhasesSource.PHASE_MILLIS.get(0),
        UpdatePhasesSource.PHASE_MILLIS.get(1),
        checkpoints.size(),
        median(durations),
        durations.length == 0 ? 0 : durations[durations.length - 1],
        median(incremental),
        incremental.length == 0 ? 0 : incremental[incremental.length - 1],
        fullState,
        dvsPerCommit,
        eqDeletesPerCommit,
        planMs,
        scanMs,
        dataFiles,
        eqDeleteRefs,
        dvRefs);
    CATALOG_EXTENSION.catalog().dropTable(TableIdentifier.of(DATABASE, name), true);
  }

  /**
   * Compares four sink modes on CPU, memory, backpressure, checkpoints, savepoints, throughput and
   * read cost. Runs only when {@code INDEX_DV_RESOURCE_KEYS} is set; modes come from {@code
   * INDEX_DV_RESOURCE_MODES} (A append, B equality deletes, C converter, D index-dv).
   */
  @Test
  void resourceComparison() throws Exception {
    String keysEnv = System.getenv("INDEX_DV_RESOURCE_KEYS");
    assumeThat(keysEnv).isNotNull();
    int keys = Integer.parseInt(keysEnv);
    List<String> modes =
        ImmutableList.copyOf(
            System.getenv().getOrDefault("INDEX_DV_RESOURCE_MODES", "A,B,C,D").split(","));
    int rounds = Integer.parseInt(System.getenv().getOrDefault("INDEX_DV_RESOURCE_ROUNDS", "2"));
    for (int round = 1; round <= rounds; round++) {
      for (String mode : modes) {
        runResourceRound(round, mode, keys, keys / 10);
      }
    }
  }

  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:MethodLength"})
  private void runResourceRound(int round, String mode, int keys, int updates) throws Exception {
    String name = "res_" + mode.toLowerCase(Locale.ROOT) + "_" + round;
    Table table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TableIdentifier.of(DATABASE, name),
                SimpleDataUtil.SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(
                    TableProperties.FORMAT_VERSION,
                    "3",
                    TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
                    String.valueOf(TARGET_FILE_SIZE)));
    List<String> lines = Lists.newArrayList();

    PhaseMeter load = PhaseMeter.start(null, null);
    loadAppendOnly(name, keys);
    lines.add(load.stop("load", updates));

    PhasedSource.reset();
    IndexDvDataWriter.LAST_BOOTSTRAP_MILLIS.set(-1);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(PARALLELISM);
    env.setMaxParallelism(128);
    env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);
    DataStream<Row> input =
        env.addSource(new PhasedSource(keys, updates), TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO)
            .setParallelism(1);
    IcebergSink.Builder builder =
        IcebergSink.forRow(input, SimpleDataUtil.FLINK_SCHEMA)
            .tableLoader(loader(name))
            .writeParallelism(PARALLELISM)
            .uidSuffix("res");
    if (!"A".equals(mode)) {
      builder.equalityFieldColumns(ImmutableList.of("id")).upsert(true);
    }

    if ("C".equals(mode)) {
      builder
          .convertEqualityDeletes()
          .set(FlinkMaintenanceConfig.RATE_LIMIT, "1")
          .set(FlinkMaintenanceConfig.LOCK_CHECK_DELAY, "1");
    } else if ("D".equals(mode)) {
      builder.set(FlinkWriteOptions.UPSERT_MODE.key(), "index-dv");
    }

    builder.append();

    PhaseMeter startup = PhaseMeter.start(reporter, null);
    JobClient job = env.executeAsync("resources " + name);
    startup.attach(job.getJobID());
    awaitFlag(job, PhasedSource.READY);
    lines.add(startup.stop("startup+bootstrap", 0));

    for (int phase = 1; phase <= 2; phase++) {
      PhaseMeter meter = PhaseMeter.start(reporter, job.getJobID());
      PhasedSource.GO.set(phase);
      int donePhase = phase;
      while (PhasedSource.DONE.get() < donePhase) {
        failIfTerminal(job);
        Thread.sleep(20);
      }

      lines.add(
          meter.stop(
              phase == 1 ? "random" : "clustered",
              updates,
              PhasedSource.PHASE_MILLIS.get(phase - 1)));
    }

    // Let the commit of the last phase checkpoint land before reading the table.
    Thread.sleep(3 * CHECKPOINT_INTERVAL_MS);
    long conversionMs = -1;
    if ("C".equals(mode)) {
      long start = System.currentTimeMillis();
      while (liveEqualityDeletes(table) > 0 && System.currentTimeMillis() - start < 120_000) {
        failIfTerminal(job);
        Thread.sleep(500);
      }

      conversionMs = System.currentTimeMillis() - start;
    }

    String savepoints = "";
    for (SavepointFormatType format :
        ImmutableList.of(SavepointFormatType.CANONICAL, SavepointFormatType.NATIVE)) {
      Path dir = savepointDir.resolve(name + "-" + format.name().toLowerCase(Locale.ROOT));
      long start = System.nanoTime();
      String path;
      try {
        path = job.triggerSavepoint(dir.toString(), format).get(600, TimeUnit.SECONDS);
        savepoints +=
            String.format(
                " %sMs=%d %sBytes=%d",
                format.name().toLowerCase(Locale.ROOT),
                (System.nanoTime() - start) / 1_000_000,
                format.name().toLowerCase(Locale.ROOT),
                directoryBytes(Path.of(path.replace("file:", ""))));
      } catch (Exception e) {
        savepoints += " " + format.name().toLowerCase(Locale.ROOT) + "=FAILED";
      }
    }

    job.cancel().get(60, TimeUnit.SECONDS);
    while (!job.getJobStatus().get().isTerminalState()) {
      Thread.sleep(50);
    }

    List<AbstractCheckpointStats> checkpoints = Lists.newArrayList();
    for (AbstractCheckpointStats stats : completedCheckpoints(job.getJobID())) {
      if (!stats.getProperties().isSavepoint()) {
        checkpoints.add(stats);
      }
    }

    long[] durations =
        checkpoints.stream()
            .mapToLong(AbstractCheckpointStats::getEndToEndDuration)
            .sorted()
            .toArray();
    long[] incremental =
        checkpoints.stream()
            .mapToLong(AbstractCheckpointStats::getCheckpointedSize)
            .sorted()
            .toArray();
    long fullState =
        checkpoints.stream().mapToLong(AbstractCheckpointStats::getStateSize).max().orElse(0);

    table.refresh();
    long planStart = System.nanoTime();
    int dataFiles = 0;
    int eqRefs = 0;
    int dvRefs = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        dataFiles++;
        for (DeleteFile delete : task.deletes()) {
          if (delete.content() == FileContent.EQUALITY_DELETES) {
            eqRefs++;
          } else {
            dvRefs++;
          }
        }
      }
    }

    long planMs = (System.nanoTime() - planStart) / 1_000_000;
    long scanStart = System.nanoTime();
    long rows = countRows(table);
    long scanMs = (System.nanoTime() - scanStart) / 1_000_000;

    for (String line : lines) {
      System.out.printf("RES round=%d mode=%s %s%n", round, mode, line);
    }

    System.out.printf(
        "RESSUM round=%d mode=%s keys=%d updates=%d bootstrapMs=%d checkpoints=%d ckptMedianMs=%d "
            + "ckptMaxMs=%d incrMedianBytes=%d incrMaxBytes=%d fullStateMaxBytes=%d%s "
            + "conversionWaitMs=%d planMs=%d scanMs=%d rows=%d expectedRows=%d dataFiles=%d "
            + "eqDeleteRefs=%d dvRefs=%d%n",
        round,
        mode,
        keys,
        updates,
        IndexDvDataWriter.LAST_BOOTSTRAP_MILLIS.get(),
        checkpoints.size(),
        median(durations),
        durations.length == 0 ? 0 : durations[durations.length - 1],
        median(incremental),
        incremental.length == 0 ? 0 : incremental[incremental.length - 1],
        fullState,
        savepoints,
        conversionMs,
        planMs,
        scanMs,
        rows,
        "A".equals(mode) ? keys + 2L * updates : keys,
        dataFiles,
        eqRefs,
        dvRefs);
    CATALOG_EXTENSION.catalog().dropTable(TableIdentifier.of(DATABASE, name), true);
  }

  private static int liveEqualityDeletes(Table table) throws IOException {
    table.refresh();
    int count = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        for (DeleteFile delete : task.deletes()) {
          if (delete.content() == FileContent.EQUALITY_DELETES) {
            count++;
          }
        }
      }
    }

    return count;
  }

  private static void awaitFlag(JobClient job, AtomicBoolean flag) throws Exception {
    while (!flag.get()) {
      failIfTerminal(job);
      Thread.sleep(20);
    }
  }

  private static void failIfTerminal(JobClient job) throws Exception {
    if (job.getJobStatus().get().isTerminalState()) {
      job.getJobExecutionResult().get();
      throw new IllegalStateException("Job ended early");
    }
  }

  private static long directoryBytes(Path dir) throws IOException {
    try (Stream<Path> paths = Files.walk(dir)) {
      return paths.filter(Files::isRegularFile).mapToLong(p -> p.toFile().length()).sum();
    }
  }

  /**
   * Measures one phase: process CPU, heap pool peaks, sampled RSS, and sampled busy, back-pressure
   * and RocksDB gauges of the job.
   */
  private static final class PhaseMeter {
    private final long wallStart = System.nanoTime();
    private final long cpuStart = processCpuNanos();
    private final InMemoryReporter reporter;
    private volatile JobID jobId;
    private final Thread sampler;
    private volatile boolean sampling = true;
    private volatile long maxRssKb = -1;
    private final Map<String, Double> maxGauges = Maps.newConcurrentMap();
    private final Map<String, Double> sumGauges = Maps.newConcurrentMap();
    private final AtomicInteger samples = new AtomicInteger();

    private PhaseMeter(InMemoryReporter reporter, JobID jobId) {
      this.reporter = reporter;
      this.jobId = jobId;
      for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
        pool.resetPeakUsage();
      }

      this.sampler = new Thread(this::sample, "phase-meter");
      sampler.setDaemon(true);
      sampler.start();
    }

    static PhaseMeter start(InMemoryReporter reporter, JobID jobId) {
      return new PhaseMeter(reporter, jobId);
    }

    void attach(JobID id) {
      this.jobId = id;
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private void sample() {
      while (sampling) {
        maxRssKb = Math.max(maxRssKb, rssKb());
        if (reporter != null && jobId != null) {
          Map<String, Double> perSample = Maps.newHashMap();
          try {
            for (Map.Entry<MetricGroup, Map<String, Metric>> group :
                reporter.getMetricsByGroup().entrySet()) {
              if (!jobId.toString().equals(group.getKey().getAllVariables().get("<job_id>"))
                  || group
                      .getKey()
                      .getAllVariables()
                      .getOrDefault("<task_name>", "")
                      .contains("Source")) {
                continue;
              }

              for (Map.Entry<String, Metric> entry : group.getValue().entrySet()) {
                String category = category(entry.getKey());
                if (category != null && entry.getValue() instanceof View view) {
                  view.update();
                }

                if (category != null
                    && entry.getValue() instanceof Gauge<?> gauge
                    && gauge.getValue() instanceof Number number
                    && !Double.isNaN(number.doubleValue())) {
                  boolean sum =
                      category.endsWith("LiveDataBytes") || category.endsWith("MemTableBytes");
                  perSample.merge(category, number.doubleValue(), sum ? Double::sum : Math::max);
                }
              }
            }
          } catch (RuntimeException e) {
            // shortcut: the metric registry is read while tasks register metrics; skip the sample
            perSample.clear();
          }

          perSample.forEach(
              (key, value) -> {
                maxGauges.merge(key, value, Math::max);
                sumGauges.merge(key, value, Double::sum);
              });
          samples.incrementAndGet();
        }

        try {
          Thread.sleep(250);
        } catch (InterruptedException e) {
          return;
        }
      }
    }

    private static String category(String identifier) {
      if (identifier.endsWith("backPressuredTimeMsPerSecond")) {
        return "backPressuredMs/s";
      } else if (identifier.endsWith("busyTimeMsPerSecond")) {
        return "busyMs/s";
      } else if (identifier.endsWith("block-cache-usage")) {
        return "rocksdbBlockCacheBytes";
      } else if (identifier.endsWith("estimate-live-data-size")) {
        return "rocksdbLiveDataBytes";
      } else if (identifier.endsWith("size-all-mem-tables")) {
        return "rocksdbMemTableBytes";
      }

      return null;
    }

    String stop(String phase, int records) {
      return stop(phase, records, -1);
    }

    String stop(String phase, int records, long phaseMillis) {
      this.sampling = false;
      try {
        sampler.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      long wallMs = (System.nanoTime() - wallStart) / 1_000_000;
      long cpuMs = (processCpuNanos() - cpuStart) / 1_000_000;
      long heapPeak = 0;
      for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
        if (pool.getType() == MemoryType.HEAP && pool.getPeakUsage() != null) {
          heapPeak += pool.getPeakUsage().getUsed();
        }
      }

      StringBuilder gauges = new StringBuilder();
      int count = Math.max(1, samples.get());
      for (String key : Sets.newTreeSet(maxGauges.keySet())) {
        gauges.append(
            String.format(
                " %s.max=%.0f %s.avg=%.0f",
                key, maxGauges.get(key), key, sumGauges.get(key) / count));
      }

      long throughput = phaseMillis > 0 && records > 0 ? records * 1000L / phaseMillis : -1;
      return String.format(
          "phase=%s wallMs=%d phaseMs=%d cpuMs=%d avgCores=%.2f heapPeakMb=%d rssMaxMb=%d "
              + "throughputPerS=%d%s",
          phase,
          wallMs,
          phaseMillis,
          cpuMs,
          wallMs > 0 ? (double) cpuMs / wallMs : 0,
          heapPeak / (1024 * 1024),
          maxRssKb > 0 ? maxRssKb / 1024 : -1,
          throughput,
          gauges);
    }

    private static long processCpuNanos() {
      OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
      return os instanceof com.sun.management.OperatingSystemMXBean sun
          ? sun.getProcessCpuTime()
          : -1;
    }

    private static long rssKb() {
      try {
        Process ps =
            new ProcessBuilder(
                    "ps", "-o", "rss=", "-p", String.valueOf(ProcessHandle.current().pid()))
                .redirectErrorStream(true)
                .start();
        String out = new String(ps.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        ps.waitFor(2, TimeUnit.SECONDS);
        return Long.parseLong(out);
      } catch (Exception e) {
        return -1;
      }
    }
  }

  /**
   * Signals readiness after the first completed checkpoint, then emits each update phase when the
   * test asks for it. A phase is done once a checkpoint taken after its last record completes.
   */
  private static class PhasedSource
      implements SourceFunction<Row>, CheckpointListener, CheckpointedFunction {
    private static final int CHUNK = 20_000;
    private static final AtomicBoolean READY = new AtomicBoolean();
    private static final AtomicInteger GO = new AtomicInteger();
    private static final AtomicInteger DONE = new AtomicInteger();
    private static final AtomicLongArray PHASE_MILLIS = new AtomicLongArray(2);

    private final int keys;
    private final int updates;
    private volatile boolean running = true;
    private volatile boolean emitted = false;
    private volatile long targetCheckpoint = -1;
    private volatile boolean phaseDone = false;

    PhasedSource(int keys, int updates) {
      this.keys = keys;
      this.updates = updates;
    }

    static void reset() {
      READY.set(false);
      GO.set(0);
      DONE.set(0);
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
      while (running && !READY.get()) {
        Thread.sleep(5);
      }

      Random random = new Random(keys);
      int clusteredStart = keys / 2 - updates / 2;
      for (int phase = 1; phase <= 2 && running; phase++) {
        while (running && GO.get() < phase) {
          Thread.sleep(5);
        }

        long start = System.nanoTime();
        this.emitted = false;
        this.targetCheckpoint = -1;
        this.phaseDone = false;
        for (int from = 0; from < updates && running; from += CHUNK) {
          synchronized (ctx.getCheckpointLock()) {
            for (int i = from; i < Math.min(updates, from + CHUNK); i++) {
              int id = phase == 1 ? random.nextInt(keys) : clusteredStart + i;
              ctx.collect(Row.ofKind(RowKind.UPDATE_AFTER, id, "p" + phase + "-" + i));
            }
          }
        }

        this.emitted = true;
        while (running && !phaseDone) {
          Thread.sleep(2);
        }

        PHASE_MILLIS.set(phase - 1, (System.nanoTime() - start) / 1_000_000);
        DONE.set(phase);
      }

      while (running) {
        Thread.sleep(10);
      }
    }

    @Override
    public void cancel() {
      this.running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) {
      if (emitted && targetCheckpoint < 0) {
        this.targetCheckpoint = context.getCheckpointId();
      }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      READY.set(true);
      if (targetCheckpoint >= 0 && checkpointId >= targetCheckpoint) {
        this.phaseDone = true;
      }
    }
  }

  private void loadAppendOnly(String name, int keys) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(PARALLELISM);
    DataStream<Row> input =
        env.fromSequence(0, keys - 1)
            .map(i -> Row.of((int) (long) i, "initial-" + i))
            .returns(TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO);
    FlinkSink.forRow(input, SimpleDataUtil.FLINK_SCHEMA)
        .tableLoader(loader(name))
        .resolvedSchema(SimpleDataUtil.FLINK_SCHEMA)
        .writeParallelism(PARALLELISM)
        .append();
    env.execute("load " + name);
  }

  private JobID runUpserts(String name, String mode, int keys, int updates) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(PARALLELISM);
    env.setMaxParallelism(128);
    env.enableCheckpointing(CHECKPOINT_INTERVAL_MS);
    DataStream<Row> input =
        env.addSource(
                new UpdatePhasesSource(keys, updates), TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO)
            .setParallelism(1);
    if ("iceberg".equals(System.getenv("INDEX_DV_BENCH_SINK"))) {
      IcebergSink.forRow(input, SimpleDataUtil.FLINK_SCHEMA)
          .tableLoader(loader(name))
          .writeParallelism(PARALLELISM)
          .equalityFieldColumns(ImmutableList.of("id"))
          .upsert(true)
          .set(FlinkWriteOptions.UPSERT_MODE.key(), mode)
          .append();
    } else {
      FlinkSink.forRow(input, SimpleDataUtil.FLINK_SCHEMA)
          .tableLoader(loader(name))
          .resolvedSchema(SimpleDataUtil.FLINK_SCHEMA)
          .writeParallelism(PARALLELISM)
          .equalityFieldColumns(ImmutableList.of("id"))
          .upsert(true)
          .set(FlinkWriteOptions.UPSERT_MODE.key(), mode)
          .append();
    }

    JobClient job = env.executeAsync("upserts " + name);
    job.getJobExecutionResult().get();
    return job.getJobID();
  }

  private List<AbstractCheckpointStats> completedCheckpoints(JobID jobId) throws Exception {
    List<AbstractCheckpointStats> completed = Lists.newArrayList();
    for (AbstractCheckpointStats stats :
        cluster
            .getArchivedExecutionGraph(jobId)
            .get()
            .getCheckpointStatsSnapshot()
            .getHistory()
            .getCheckpoints()) {
      if (stats.getStatus() == CheckpointStatsStatus.COMPLETED) {
        completed.add(stats);
      }
    }

    return completed;
  }

  private void printCheckpoints(
      JobID jobId, String mode, int round, List<AbstractCheckpointStats> checkpoints)
      throws Exception {
    ArchivedExecutionGraph graph = cluster.getArchivedExecutionGraph(jobId).get();
    long first = checkpoints.isEmpty() ? 0 : checkpoints.get(0).getTriggerTimestamp();
    for (AbstractCheckpointStats stats : checkpoints) {
      StringBuilder vertices = new StringBuilder();
      for (TaskStateStats task : stats.getAllTaskStateStats()) {
        TaskStateStats.TaskStateStatsSummary summary = task.getSummaryStats();
        vertices.append(
            String.format(
                " [%s sync=%d async=%d align=%d startDelay=%d incr=%d]",
                graph.getJobVertex(task.getJobVertexId()).getName(),
                summary.getSyncCheckpointDurationStats().getMaximum(),
                summary.getAsyncCheckpointDurationStats().getMaximum(),
                summary.getAlignmentDurationStats().getMaximum(),
                summary.getCheckpointStartDelayStats().getMaximum(),
                task.getCheckpointedSize()));
      }

      System.out.printf(
          "CKPT round=%d mode=%s id=%d atMs=%d durMs=%d incr=%d full=%d%s%n",
          round,
          mode,
          stats.getCheckpointId(),
          stats.getTriggerTimestamp() - first,
          stats.getEndToEndDuration(),
          stats.getCheckpointedSize(),
          stats.getStateSize(),
          vertices);
    }
  }

  private static TableLoader loader(String name) {
    return TableLoader.fromCatalog(
        CATALOG_EXTENSION.catalogLoader(), TableIdentifier.of(DATABASE, name));
  }

  private static long median(long[] sorted) {
    return sorted.length == 0 ? 0 : sorted[sorted.length / 2];
  }

  private static long countRows(Table table) throws IOException {
    long count = 0;
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record ignored : records) {
        count++;
      }
    }

    return count;
  }

  /**
   * Waits for one completed checkpoint, so writers have bootstrapped, then emits a random update
   * phase and a clustered update phase, each in batches separated by two completed checkpoints.
   */
  private static class UpdatePhasesSource implements SourceFunction<Row>, CheckpointListener {
    private static final AtomicLongArray PHASE_MILLIS = new AtomicLongArray(2);
    private static final AtomicLongArray PHASE_START_EPOCH_MS = new AtomicLongArray(2);

    private final int keys;
    private final int updates;
    private final AtomicInteger completedCheckpoints = new AtomicInteger();
    private volatile boolean running = true;

    UpdatePhasesSource(int keys, int updates) {
      this.keys = keys;
      this.updates = updates;
    }

    static void reset() {
      PHASE_MILLIS.set(0, -1);
      PHASE_MILLIS.set(1, -1);
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
      awaitCheckpoints(1);
      Random random = new Random(keys);
      int clusteredStart = keys / 2 - updates / 2;
      for (int phase = 0; phase < 2 && running; phase++) {
        long start = System.nanoTime();
        PHASE_START_EPOCH_MS.set(phase, System.currentTimeMillis());
        int perBatch = updates / BATCHES_PER_PHASE;
        for (int batch = 0; batch < BATCHES_PER_PHASE && running; batch++) {
          synchronized (ctx.getCheckpointLock()) {
            for (int i = 0; i < perBatch; i++) {
              int id = phase == 0 ? random.nextInt(keys) : clusteredStart + batch * perBatch + i;
              ctx.collect(
                  Row.ofKind(RowKind.UPDATE_AFTER, id, "p" + phase + "-" + batch + "-" + i));
            }
          }

          awaitCheckpoints(2);
        }

        PHASE_MILLIS.set(phase, (System.nanoTime() - start) / 1_000_000);
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
}
