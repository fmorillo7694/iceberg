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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericAppenderHelper;
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
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * POC: failure scenarios for four sink modes. The mode comes from {@code FAILURE_MODE}: A append, B
 * upsert with equality deletes, C upsert with the equality delete converter, D index-dv. Each test
 * prints one RESULT line; nothing is asserted, so every scenario reports even when it is wrong.
 */
@Timeout(value = 240)
class TestIcebergSinkFailureScenarios {
  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  private static final int PARALLELISM = 2;
  private static final int RECORDS = 400;
  private static final int KEYS = 60;
  private static final int BATCH = 40;
  private static final long CHECKPOINT_INTERVAL_MS = 200L;
  private static final int CHECKPOINTS_TO_COMMIT = 3;

  @TempDir private Path checkpointDir;
  @TempDir private Path savepointDir;
  @TempDir private Path compactionDir;

  private String mode;
  private String staleRestore;
  private Table table;
  private final Set<String> externalRows = Sets.newHashSet();
  private final List<String> jobFailures = Lists.newArrayList();

  @BeforeEach
  void before() {
    this.mode = System.getenv().getOrDefault("FAILURE_MODE", "D").toUpperCase(Locale.ROOT);
    this.staleRestore =
        System.getenv().getOrDefault("STALE_RESTORE", "fail").toLowerCase(Locale.ROOT);
    this.table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TableIdentifier.of(DATABASE, "t_" + mode.toLowerCase(Locale.ROOT)),
                SimpleDataUtil.SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
    Control.reset();
    IndexDvDataWriter.REWRITES_APPLIED.set(0);
    IndexDvDataWriter.REBUILDS.set(0);
    IndexDvDataWriter.LAST_REBUILD_MILLIS.set(-1);
    IndexDvCommits.REBASED_DVS.set(0);
  }

  @AfterEach
  void after() {
    IcebergCommitter.setBeforeCommitAttempt(checkpointId -> {});
  }

  /** 1: failover to the latest checkpoint while input keeps flowing. */
  @Test
  void s1RestoreLatestCheckpoint() throws Exception {
    Control.failAtRecord = 150;
    runToEnd(0, null, null, 3);
    expect(report("s1-latest-checkpoint"), "OK");
  }

  /** 2: restore a retained checkpoint that is two commits behind the table. */
  @Test
  void s2RestoreCheckpointBehindTable() throws Exception {
    String behind;
    try (Cluster cluster = new Cluster()) {
      Control.holdAt = 160;
      JobClient job = start(cluster, 0, null, null, 0);
      awaitHoldAndCommits(job);
      behind = latestCheckpoint(job.getJobID());
      Control.holdAt = 280;
      awaitHoldAndCommits(job);
      cancel(job);
    }

    Control.holdAt = -1;
    runToEnd(0, behind, null, 0);
    expect(report("s2-checkpoint-behind-table"), staleVerdict());
  }

  /** 3: restore a savepoint taken before at least five more commits. */
  @Test
  void s3RestoreOldSavepoint() throws Exception {
    String savepoint;
    try (Cluster cluster = new Cluster()) {
      Control.holdAt = 120;
      JobClient job = start(cluster, 0, null, null, 0);
      awaitHoldAndCommits(job);
      savepoint = savepoint(job, false);
      int commitsBefore = snapshotCount();
      Control.holdAt = 120 + 5 * BATCH;
      awaitHoldAndCommits(job);
      System.out.printf("NOTE s3 commits after savepoint=%d%n", snapshotCount() - commitsBefore);
      cancel(job);
    }

    Control.holdAt = -1;
    runToEnd(0, savepoint, null, 0);
    expect(report("s3-old-savepoint"), staleVerdict());
  }

  /** 4: stateless restart with the same job id (apache/iceberg#18098). */
  @Test
  void s4StatelessRestartSameJobId() throws Exception {
    JobID fixed = JobID.generate();
    Control.endAt = 200;
    runToEnd(0, null, fixed, 0);
    Control.endAt = RECORDS;
    runToEnd(200, null, fixed, 0);
    expect(report("s4-stateless-same-job-id"), "OK");
  }

  /** 5a: a new job with a different id restores another job's stop-with-savepoint. */
  @Test
  void s5aOtherJobRestoresSavepoint() throws Exception {
    String savepoint;
    try (Cluster cluster = new Cluster()) {
      Control.holdAt = 200;
      JobClient job = start(cluster, 0, null, null, 0);
      awaitHoldAndCommits(job);
      savepoint = savepoint(job, true);
      awaitTerminal(job);
    }

    Control.holdAt = -1;
    runToEnd(0, savepoint, null, 0);
    expect(report("s5a-other-job-restores-savepoint"), "OK");
  }

  /** 5b: two jobs with different ids both continue from the same savepoint, one after the other. */
  @Test
  void s5bTwoJobsFromSameSavepoint() throws Exception {
    String savepoint;
    try (Cluster cluster = new Cluster()) {
      Control.holdAt = 200;
      Control.finishAtEnd = true;
      JobClient job = start(cluster, 0, null, null, 0);
      awaitHoldAndCommits(job);
      savepoint = savepoint(job, false);
      Control.holdAt = -1;
      awaitTerminal(job);
    }

    runToEnd(0, savepoint, null, 0);
    expect(report("s5b-two-jobs-same-savepoint"), staleVerdict());
  }

  /** 6a: cancel while a checkpoint is blocked, restart from the last completed checkpoint. */
  @Test
  void s6aCancelDuringCheckpointRestoreLatest() throws Exception {
    String latest = cancelDuringCheckpoint();
    runToEnd(0, latest, null, 0);
    expect(report("s6a-cancel-in-checkpoint-restore-latest"), "OK");
  }

  /** 6b: cancel while a checkpoint is blocked, restart with no state (full replay). */
  @Test
  void s6bCancelDuringCheckpointNoState() throws Exception {
    cancelDuringCheckpoint();
    runToEnd(0, null, null, 0);
    expect(report("s6b-cancel-in-checkpoint-no-state"), "OK");
  }

  /** c1: an external full rewrite lands after the latest checkpoint, then restore it. */
  @Test
  void c1RewriteAfterLatestCheckpoint() throws Exception {
    String latest = runHeldAt(200);
    compact(null);
    runToEnd(0, latest, null, 0);
    expect(report("c1-full-rewrite-after-latest", RECORDS), "OK");
  }

  /** c2: as c1, rewriting only one bucket of a bucket(id, 3) table. */
  @Test
  void c2PartialRewriteAfterLatestCheckpoint() throws Exception {
    recreateBucketed();
    String latest = runHeldAt(200);
    compact(0);
    runToEnd(0, latest, null, 0);
    expect(report("c2-partial-rewrite-after-latest", RECORDS), "OK");
  }

  /** c3a: restore a checkpoint behind the table where the only newer commit is a compaction. */
  @Test
  void c3aStaleRestoreOnlyCompactionAfter() throws Exception {
    String behind;
    try (Cluster cluster = new Cluster()) {
      Control.holdAt = 160;
      JobClient job = start(cluster, 0, null, null, 0);
      awaitHoldAndCommits(job);
      behind = latestCheckpoint(job.getJobID());
      compact(null);
      awaitCheckpoints(job, CHECKPOINTS_TO_COMMIT);
      cancel(job);
    }

    Control.holdAt = -1;
    runToEnd(0, behind, null, 0);
    expect(report("c3a-stale-only-compaction-after", RECORDS), "OK");
  }

  /** c3b: sink commits after the restored checkpoint, then a compaction rewrites those files. */
  @Test
  void c3bStaleRestoreCommitsThenCompaction() throws Exception {
    String behind = runStaleThenCancel(false);
    compact(null);
    runToEnd(0, behind, null, 0);
    expect(report("c3b-stale-commits-then-compaction", RECORDS), staleVerdict());
  }

  /** c3c: a compaction, then sink commits, both after the restored checkpoint. */
  @Test
  void c3cStaleRestoreCompactionThenCommits() throws Exception {
    String behind = runStaleThenCancel(true);
    runToEnd(0, behind, null, 0);
    expect(report("c3c-stale-compaction-then-commits", RECORDS), staleVerdict());
  }

  /** c4: rows retired by DVs stay deleted when the rewrite drops them and the job restores. */
  @Test
  void c4RewriteOfFilesWithLiveDvsKeepsDeletes() throws Exception {
    String latest = runHeldAt(200);
    int liveDvsBefore = deleteStats()[2];
    compact(null);
    Control.endAt = 200;
    runToEnd(0, latest, null, 0);
    System.out.printf("NOTE c4 live DVs rewritten by the compaction=%d%n", liveDvsBefore);
    String verdict = report("c4-rewrite-drops-dv-rows", 200);
    if ("D".equals(mode)) {
      assertThat(liveDvsBefore).isPositive();
    }

    expect(verdict, "OK");
  }

  /** c5: as c3b, then expire every snapshot but the head, so the restored one is gone. */
  @Test
  void c5StaleRestoreAfterCompactionAndExpire() throws Exception {
    String behind = runStaleThenCancel(false);
    compact(null);
    expireAllButHead();
    runToEnd(0, behind, null, 0);
    expect(report("c5-stale-compaction-expired", RECORDS), staleVerdict());
  }

  /** c5a: as c1, then expire every snapshot but the head; reported, not asserted. */
  @Test
  void c5aLatestRestoreAfterCompactionAndExpire() throws Exception {
    String latest = runHeldAt(200);
    compact(null);
    expireAllButHead();
    runToEnd(0, latest, null, 0);
    report("c5a-latest-compaction-expired", RECORDS);
  }

  /** c6: every live DV is rewritten into a new DV file between the checkpoint and the restore. */
  @Test
  void c6DvRewriteAfterLatestCheckpoint() throws Exception {
    String latest = runHeldAt(200);
    int rewritten = rewriteDvs();
    runToEnd(0, latest, null, 0);
    System.out.printf("NOTE c6 DVs rewritten=%d%n", rewritten);
    expect(report("c6-dv-rewrite-after-latest", RECORDS), "OK");
  }

  /**
   * p1: the commit of a checkpoint fails, another writer appends, and the job restores that
   * checkpoint. Its results are not in the table, so a rebuild must refuse too.
   */
  @Test
  void p1PendingResultsThenExternalAppend() throws Exception {
    String pending = runUntilCommitFails(3);
    appendExternalRow();
    runToEnd(0, pending, null, 0);
    expect(report("p1-pending-then-external-append", RECORDS), "LOUD");
  }

  /** p2: the commit of a checkpoint fails and the job restores it: committed once, nothing lost. */
  @Test
  void p2PendingResultsRestore() throws Exception {
    String pending = runUntilCommitFails(3);
    int snapshotsBefore = snapshotCount();
    runToEnd(0, pending, null, 0);
    System.out.printf("NOTE p2 snapshots before restore=%d%n", snapshotsBefore);
    expect(report("p2-pending-restore", RECORDS), "OK");
  }

  private String staleVerdict() {
    return "rebuild".equals(staleRestore) ? "OK" : "LOUD";
  }

  private String runUntilCommitFails(int commitAttempt) throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    IcebergCommitter.setBeforeCommitAttempt(
        checkpointId -> {
          if (attempts.incrementAndGet() == commitAttempt) {
            throw new IllegalStateException(
                "Injected commit failure at checkpoint " + checkpointId);
          }
        });
    try (Cluster cluster = new Cluster()) {
      JobClient job = start(cluster, 0, null, null, 0);
      awaitTerminal(job);
      System.out.printf("NOTE commit failed, job %s%n", job.getJobStatus().get());
      return latestCheckpoint(job.getJobID());
    } finally {
      IcebergCommitter.setBeforeCommitAttempt(checkpointId -> {});
    }
  }

  private void appendExternalRow() throws IOException {
    table.refresh();
    new GenericAppenderHelper(table, FileFormat.PARQUET, compactionDir)
        .appendToTable(ImmutableList.of(SimpleDataUtil.createRecord(1000, "external")));
    externalRows.add("1000:external");
  }

  private String runHeldAt(int hold) throws Exception {
    try (Cluster cluster = new Cluster()) {
      Control.holdAt = hold;
      JobClient job = start(cluster, 0, null, null, 0);
      awaitHoldAndCommits(job);
      String latest = latestCheckpoint(job.getJobID());
      cancel(job);
      Control.holdAt = -1;
      return latest;
    }
  }

  private String runStaleThenCancel(boolean compactBeforeCommits) throws Exception {
    try (Cluster cluster = new Cluster()) {
      Control.holdAt = 160;
      JobClient job = start(cluster, 0, null, null, 0);
      awaitHoldAndCommits(job);
      String behind = latestCheckpoint(job.getJobID());
      if (compactBeforeCommits) {
        compact(null);
      }

      Control.holdAt = 280;
      awaitHoldAndCommits(job);
      cancel(job);
      Control.holdAt = -1;
      return behind;
    }
  }

  private void awaitCheckpoints(JobClient job, int count) throws Exception {
    int target = Control.COMPLETED_CHECKPOINTS.get() + count;
    awaitOrFail(job, () -> Control.COMPLETED_CHECKPOINTS.get() >= target);
  }

  private void expect(String verdict, String expected) {
    if ("D".equals(mode)) {
      assertThat(verdict).isEqualTo(expected);
    }
  }

  private void recreateBucketed() {
    TableIdentifier identifier = TableIdentifier.of(DATABASE, "t_" + mode.toLowerCase(Locale.ROOT));
    CATALOG_EXTENSION.catalog().dropTable(identifier);
    this.table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                identifier,
                SimpleDataUtil.SCHEMA,
                PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).bucket("id", 3).build(),
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
  }

  /** Rewrites the live rows of every data file, or of one bucket only, like RewriteDataFiles. */
  private void compact(Integer bucket) throws IOException {
    table.refresh();
    Snapshot head = table.currentSnapshot();
    PartitionKey partitionKey = new PartitionKey(table.spec(), table.schema());
    Map<StructLike, List<Record>> liveByPartition =
        StructLikeMap.create(table.spec().partitionType());
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(table).useSnapshot(head.snapshotId()).build()) {
      for (Record record : records) {
        partitionKey.partition(record);
        if (bucket == null || bucket.equals(partitionKey.get(0, Integer.class))) {
          liveByPartition
              .computeIfAbsent(partitionKey.copy(), k -> Lists.newArrayList())
              .add(record.copy());
        }
      }
    }

    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(head.snapshotId());
    int removed = 0;
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(head.snapshotId()).planFiles()) {
      for (FileScanTask task : tasks) {
        if (bucket == null || bucket.equals(task.file().partition().get(0, Integer.class))) {
          rewrite.deleteFile(task.file());
          removed++;
        }
      }
    }

    GenericAppenderHelper appender =
        new GenericAppenderHelper(table, FileFormat.PARQUET, compactionDir);
    for (Map.Entry<StructLike, List<Record>> entry : liveByPartition.entrySet()) {
      rewrite.addFile(
          table.spec().isUnpartitioned()
              ? appender.writeFile(entry.getValue())
              : appender.writeFile(entry.getKey(), entry.getValue()));
    }

    rewrite.commit();
    System.out.printf(
        "NOTE compaction bucket=%s removedDataFiles=%d addedDataFiles=%d%n",
        bucket, removed, liveByPartition.size());
  }

  /** Rewrites every live DV into a new DV file, like rewrite-position-deletes. */
  private int rewriteDvs() throws IOException {
    table.refresh();
    DeleteLoader loader = new BaseDeleteLoader(file -> table.io().newInputFile(file));
    RewriteFiles rewrite =
        table.newRewrite().validateFromSnapshot(table.currentSnapshot().snapshotId());
    int count = 0;
    for (DeleteFile dv : deleteFiles(table.currentSnapshot())) {
      if (!ContentFileUtil.isDV(dv)) {
        continue;
      }

      List<Pair<CharSequence, Long>> positions = Lists.newArrayList();
      loader
          .loadPositionDeletes(ImmutableList.of(dv), dv.referencedDataFile())
          .forEach(pos -> positions.add(Pair.of(dv.referencedDataFile(), pos)));
      DeleteFile replacement =
          FileHelpers.writeDeleteFile(
                  table,
                  table
                      .io()
                      .newOutputFile(compactionDir.resolve("dv-" + count + ".puffin").toString()),
                  dv.partition(),
                  positions,
                  3)
              .first();
      rewrite.deleteFile(dv).addFile(replacement, dv.dataSequenceNumber());
      count++;
    }

    if (count > 0) {
      rewrite.commit();
    }

    return count;
  }

  private void expireAllButHead() {
    table.refresh();
    table.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1).retainLast(1).commit();
    table.refresh();
    System.out.printf("NOTE expired, snapshots left=%d%n", snapshotCount());
  }

  private String cancelDuringCheckpoint() throws Exception {
    try (Cluster cluster = new Cluster()) {
      Control.holdAt = 200;
      JobClient job = start(cluster, 0, null, null, 0);
      awaitHoldAndCommits(job);
      String latest = latestCheckpoint(job.getJobID());
      Control.blockSnapshots = true;
      Control.holdAt = 280;
      awaitOrFail(job, Control.SNAPSHOT_BLOCKED::get);
      cancel(job);
      Control.blockSnapshots = false;
      Control.holdAt = -1;
      String afterCancel = latestCheckpoint(job.getJobID());
      System.out.printf(
          "NOTE s6 completed checkpoint before block=%s latest after cancel=%s%n",
          latest, afterCancel);
      return afterCancel;
    }
  }

  // ---------------------------------------------------------------------------------------------

  private void runToEnd(int startOffset, String restorePath, JobID fixedJobId, int restarts)
      throws Exception {
    Control.finishAtEnd = true;
    try (Cluster cluster = new Cluster()) {
      JobClient job = start(cluster, startOffset, restorePath, fixedJobId, restarts);
      try {
        job.getJobExecutionResult().get(180, TimeUnit.SECONDS);
      } catch (Exception e) {
        jobFailures.add(rootMessage(e));
      }
    }
  }

  private JobClient start(
      Cluster cluster, int startOffset, String restorePath, JobID fixedJobId, int restarts)
      throws Exception {
    Configuration config =
        new Configuration(MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG);
    config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toUri().toString());
    config.set(
        CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
    config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 50);
    config.set(
        CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofMillis(CHECKPOINT_INTERVAL_MS));
    if (restarts > 0) {
      config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
      config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, restarts);
    } else {
      config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
    }

    if (restorePath != null) {
      config.set(StateRecoveryOptions.SAVEPOINT_PATH, restorePath);
    }

    if (fixedJobId != null) {
      config.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, fixedJobId.toHexString());
    }

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(PARALLELISM);
    env.setMaxParallelism(16);
    DataStream<Row> input =
        env.addSource(new ReplaySource(startOffset), TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO)
            .uid("source")
            .setParallelism(1)
            .map(new Faults(), TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO)
            .uid("faults")
            .setParallelism(1);
    sink(input);
    JobClient job = env.executeAsync("failure scenario " + mode);
    if (fixedJobId != null && !fixedJobId.equals(job.getJobID())) {
      jobFailures.add("fixed job id not honored: " + job.getJobID());
    }

    return job;
  }

  private void sink(DataStream<Row> input) {
    TableLoader loader =
        TableLoader.fromCatalog(
            CATALOG_EXTENSION.catalogLoader(),
            TableIdentifier.of(DATABASE, "t_" + mode.toLowerCase(Locale.ROOT)));
    IcebergSink.Builder builder =
        IcebergSink.forRow(input, SimpleDataUtil.FLINK_SCHEMA)
            .tableLoader(loader)
            .writeParallelism(PARALLELISM)
            .uidSuffix("sink");
    if (!"A".equals(mode)) {
      builder.equalityFieldColumns(ImmutableList.of("id")).upsert(true);
    }

    if ("C".equals(mode)) {
      builder
          .convertEqualityDeletes()
          .set(FlinkMaintenanceConfig.RATE_LIMIT, "1")
          .set(FlinkMaintenanceConfig.LOCK_CHECK_DELAY, "1");
    } else if ("D".equals(mode)) {
      builder
          .set(FlinkWriteOptions.UPSERT_MODE.key(), "index-dv")
          .set(FlinkWriteOptions.UPSERT_STALE_RESTORE.key(), staleRestore);
    }

    builder.append();
  }

  private void awaitHoldAndCommits(JobClient job) throws Exception {
    awaitOrFail(job, Control.HOLD_REACHED::get);
    int target = Control.COMPLETED_CHECKPOINTS.get() + CHECKPOINTS_TO_COMMIT;
    awaitOrFail(job, () -> Control.COMPLETED_CHECKPOINTS.get() >= target);
    Control.HOLD_REACHED.set(false);
  }

  private String savepoint(JobClient job, boolean stop) throws Exception {
    return (stop
            ? job.stopWithSavepoint(false, savepointDir.toString(), SavepointFormatType.CANONICAL)
            : job.triggerSavepoint(savepointDir.toString(), SavepointFormatType.CANONICAL))
        .get(60, TimeUnit.SECONDS);
  }

  private void cancel(JobClient job) throws Exception {
    job.cancel().get(60, TimeUnit.SECONDS);
    awaitTerminal(job);
  }

  private static void awaitTerminal(JobClient job) throws Exception {
    while (!job.getJobStatus().get().isTerminalState()) {
      Thread.sleep(50);
    }
  }

  private void awaitOrFail(JobClient job, BooleanSupplier condition) throws Exception {
    long deadline = System.currentTimeMillis() + 120_000;
    while (!condition.getAsBoolean()) {
      if (job.getJobStatus().get().isTerminalState()) {
        throw new IllegalStateException("Job ended early: " + job.getJobStatus().get());
      }

      if (System.currentTimeMillis() > deadline) {
        throw new IllegalStateException("Timed out waiting for the job");
      }

      Thread.sleep(20);
    }
  }

  private String latestCheckpoint(JobID jobId) {
    File[] checkpoints =
        checkpointDir
            .resolve(jobId.toHexString())
            .toFile()
            .listFiles(f -> f.getName().startsWith("chk-") && new File(f, "_metadata").exists());
    return Arrays.stream(checkpoints)
        .max(Comparator.comparingInt(f -> Integer.parseInt(f.getName().substring(4))))
        .orElseThrow()
        .getAbsolutePath();
  }

  private int snapshotCount() {
    table.refresh();
    return Lists.newArrayList(table.snapshots()).size();
  }

  // ---------------------------------------------------------------------------------------------

  private String report(String scenario) throws IOException {
    return report(scenario, RECORDS);
  }

  private String report(String scenario, int upTo) throws IOException {
    table.refresh();
    Map<String, Integer> actual = Maps.newHashMap();
    Map<Integer, Integer> perKey = Maps.newHashMap();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        actual.merge(record.getField("id") + ":" + record.getField("data"), 1, Integer::sum);
        perKey.merge((Integer) record.getField("id"), 1, Integer::sum);
      }
    }

    Set<String> expected = expectedRows(upTo);
    int actualRows = actual.values().stream().mapToInt(Integer::intValue).sum();
    int missing = (int) expected.stream().filter(row -> !actual.containsKey(row)).count();
    int unexpected = 0;
    int duplicates = 0;
    for (Map.Entry<String, Integer> entry : actual.entrySet()) {
      if (!expected.contains(entry.getKey())) {
        unexpected += entry.getValue();
      } else {
        duplicates += entry.getValue() - 1;
      }
    }

    int duplicateKeys =
        "A".equals(mode) ? 0 : (int) perKey.values().stream().filter(c -> c > 1).count();
    int[] deletes = deleteStats();
    boolean wrong =
        missing > 0
            || unexpected > 0
            || duplicates > 0
            || duplicateKeys > 0
            || deletes[3] > 0
            || deletes[4] > 0;
    String verdict = !jobFailures.isEmpty() ? "LOUD" : wrong ? "SILENT_WRONG" : "OK";
    System.out.printf(
        "RESULT scenario=%s mode=%s expected=%d actual=%d duplicates=%d unexpectedRows=%d "
            + "missing=%d duplicateKeys=%d eqDeleteFilesEver=%d eqDeleteFilesLive=%d dvsLive=%d "
            + "danglingDvs=%d multiDvFiles=%d snapshots=%d rewritesApplied=%d rebasedDvs=%d "
            + "staleRestore=%s rebuilds=%d rebuildMs=%d verdict=%s jobFailures=%s%n",
        scenario,
        mode,
        expected.size(),
        actualRows,
        duplicates,
        unexpected,
        missing,
        duplicateKeys,
        deletes[0],
        deletes[1],
        deletes[2],
        deletes[3],
        deletes[4],
        snapshotCount(),
        IndexDvDataWriter.REWRITES_APPLIED.get(),
        IndexDvCommits.REBASED_DVS.get(),
        staleRestore,
        IndexDvDataWriter.REBUILDS.get(),
        IndexDvDataWriter.LAST_REBUILD_MILLIS.get(),
        verdict,
        jobFailures);
    return verdict;
  }

  private Set<String> expectedRows(int upTo) {
    Set<String> rows = Sets.newHashSet();
    if ("A".equals(mode)) {
      for (int i = 0; i < upTo; i++) {
        rows.add(key(i) + ":v" + i);
      }
    } else {
      Map<Integer, Integer> latest = Maps.newHashMap();
      for (int i = 0; i < upTo; i++) {
        latest.put(key(i), i);
      }

      latest.forEach((k, i) -> rows.add(k + ":v" + i));
    }

    rows.addAll(externalRows);

    return rows;
  }

  /** Eq delete files ever and live, live DVs, dangling DVs, extra DVs on one data file. */
  private int[] deleteStats() throws IOException {
    Set<String> everEq = Sets.newHashSet();
    for (Snapshot snapshot : table.snapshots()) {
      for (DeleteFile file : deleteFiles(snapshot)) {
        if (file.content() == FileContent.EQUALITY_DELETES) {
          everEq.add(file.location());
        }
      }
    }

    Snapshot head = table.currentSnapshot();
    if (head == null) {
      return new int[] {everEq.size(), 0, 0, 0, 0};
    }

    Set<String> liveData = Sets.newHashSet();
    for (ManifestFile manifest : head.dataManifests(table.io())) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, table.io(), table.specs())) {
        reader.forEach(file -> liveData.add(file.location()));
      }
    }

    int liveEq = 0;
    int dvs = 0;
    int dangling = 0;
    int multiDv = 0;
    Set<String> dvTargets = Sets.newHashSet();
    for (DeleteFile file : deleteFiles(head)) {
      if (file.content() == FileContent.EQUALITY_DELETES) {
        liveEq++;
      } else if (ContentFileUtil.isDV(file)) {
        dvs++;
        if (!liveData.contains(file.referencedDataFile())) {
          dangling++;
        }

        if (!dvTargets.add(file.referencedDataFile())) {
          multiDv++;
        }
      }
    }

    return new int[] {everEq.size(), liveEq, dvs, dangling, multiDv};
  }

  private List<DeleteFile> deleteFiles(Snapshot snapshot) throws IOException {
    List<DeleteFile> files = Lists.newArrayList();
    for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        reader.forEach(file -> files.add(file.copy()));
      }
    }

    return files;
  }

  private static String rootMessage(Throwable failure) {
    Throwable root = failure;
    while (root.getCause() != null) {
      root = root.getCause();
    }

    String message = String.valueOf(root.getMessage());
    return root.getClass().getSimpleName()
        + ": "
        + message.substring(0, Math.min(160, message.length()));
  }

  private static int key(int record) {
    return record % KEYS;
  }

  // ---------------------------------------------------------------------------------------------

  /** Test-controlled switches shared by the source and fault operators of every job run. */
  private static final class Control {
    private static final AtomicBoolean HOLD_REACHED = new AtomicBoolean();
    private static final AtomicBoolean SNAPSHOT_BLOCKED = new AtomicBoolean();
    private static final AtomicBoolean FAILED = new AtomicBoolean();
    private static final AtomicInteger COMPLETED_CHECKPOINTS = new AtomicInteger();
    private static volatile int holdAt = -1;
    private static volatile int endAt = RECORDS;
    private static volatile int failAtRecord = -1;
    private static volatile boolean finishAtEnd = false;
    private static volatile boolean blockSnapshots = false;

    private Control() {}

    static void reset() {
      HOLD_REACHED.set(false);
      SNAPSHOT_BLOCKED.set(false);
      FAILED.set(false);
      COMPLETED_CHECKPOINTS.set(0);
      holdAt = -1;
      endAt = RECORDS;
      failAtRecord = -1;
      finishAtEnd = false;
      blockSnapshots = false;
    }
  }

  /**
   * Emits record i as (i % KEYS, "v" + i) from its checkpointed offset, one batch per completed
   * checkpoint, so a restore replays exactly the records after the restored offset.
   */
  private static class ReplaySource
      implements SourceFunction<Row>, CheckpointedFunction, CheckpointListener {
    private final int startOffset;
    private transient ListState<Integer> offsetState;
    private volatile int offset;
    private volatile boolean running = true;
    private final AtomicInteger completedHere = new AtomicInteger();

    ReplaySource(int startOffset) {
      this.startOffset = startOffset;
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
      while (running && offset < Control.endAt) {
        if (offset == Control.holdAt) {
          Control.HOLD_REACHED.set(true);
          while (running && offset == Control.holdAt) {
            Thread.sleep(5);
          }

          continue;
        }

        synchronized (ctx.getCheckpointLock()) {
          int end = Math.min(Control.endAt, offset + BATCH);
          if (Control.holdAt > offset) {
            end = Math.min(end, Control.holdAt);
          }

          for (int i = offset; i < end; i++) {
            ctx.collect(Row.ofKind(RowKind.INSERT, key(i), "v" + i));
          }

          this.offset = end;
        }

        int target = completedHere.get() + 1;
        while (running && completedHere.get() < target) {
          Thread.sleep(5);
        }
      }

      while (running && !Control.finishAtEnd) {
        Thread.sleep(10);
      }
    }

    @Override
    public void cancel() {
      this.running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      offsetState.update(ImmutableList.of(offset));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      this.offsetState =
          context
              .getOperatorStateStore()
              .getListState(new ListStateDescriptor<>("offset", Types.INT));
      this.offset = startOffset;
      for (Integer restored : offsetState.get()) {
        this.offset = restored;
      }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      completedHere.incrementAndGet();
      Control.COMPLETED_CHECKPOINTS.incrementAndGet();
    }
  }

  /** Fails once at a chosen record and can block snapshots, chained right after the source. */
  private static class Faults implements MapFunction<Row, Row>, CheckpointedFunction {
    @Override
    public Row map(Row row) {
      int record = Integer.parseInt(((String) row.getField(1)).substring(1));
      if (record == Control.failAtRecord && Control.FAILED.compareAndSet(false, true)) {
        throw new IllegalStateException("Injected failure at record " + record);
      }

      return row;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      while (Control.blockSnapshots) {
        Control.SNAPSHOT_BLOCKED.set(true);
        Thread.sleep(10);
      }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {}
  }

  /** One MiniCluster per job run, so a fixed job id can be submitted again. */
  private static final class Cluster implements AutoCloseable {
    private final MiniCluster miniCluster;

    Cluster() throws Exception {
      this.miniCluster =
          new MiniCluster(
              new MiniClusterConfiguration.Builder()
                  .setConfiguration(clusterConfig())
                  .setNumTaskManagers(1)
                  .setNumSlotsPerTaskManager(4)
                  .build());
      miniCluster.start();
      TestStreamEnvironment.setAsContext(miniCluster, PARALLELISM);
    }

    private static Configuration clusterConfig() {
      Configuration config =
          new Configuration(MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG);
      // Read by the cluster's checkpoint store, not from the job configuration.
      config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 50);
      return config;
    }

    @Override
    public void close() throws Exception {
      TestStreamEnvironment.unsetAsContext();
      miniCluster.close();
    }
  }
}
