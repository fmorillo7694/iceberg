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
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.checkpoint.AbstractCheckpointStats;
import org.apache.flink.runtime.checkpoint.CheckpointStatsStatus;
import org.apache.flink.runtime.checkpoint.TaskStateStats;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  private static final int PARALLELISM = 2;
  private static final int ROUNDS = 2;
  private static final int BATCHES_PER_PHASE = 2;
  private static final long CHECKPOINT_INTERVAL_MS = 1000L;
  private static final long TARGET_FILE_SIZE = 8L * 1024 * 1024;

  @TempDir private Path checkpointDir;
  private MiniCluster cluster;

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
    FlinkSink.forRow(input, SimpleDataUtil.FLINK_SCHEMA)
        .tableLoader(loader(name))
        .resolvedSchema(SimpleDataUtil.FLINK_SCHEMA)
        .writeParallelism(PARALLELISM)
        .equalityFieldColumns(ImmutableList.of("id"))
        .upsert(true)
        .set(FlinkWriteOptions.UPSERT_MODE.key(), mode)
        .append();
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
