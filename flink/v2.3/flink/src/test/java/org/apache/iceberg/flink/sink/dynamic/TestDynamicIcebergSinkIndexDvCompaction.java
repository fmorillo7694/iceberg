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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
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
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.sink.IndexDvCommits;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
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
 * POC: external compaction against the dynamic sink. Three tables are written in one stream and
 * every rewrite targets {@code t_a} only. The upsert mode comes from {@code DYN_COMPACTION_MODE}
 * ({@code index-dv}, the default, or {@code equality-delete} as the baseline). Each test prints one
 * RESULT line per table and asserts the expected verdict in index-dv mode only.
 */
@Timeout(value = 240)
class TestDynamicIcebergSinkIndexDvCompaction {
  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  private static final String INDEX_DV = "index-dv";
  private static final String TARGET = "t_a";
  private static final List<String> TABLES = ImmutableList.of(TARGET, "t_b", "t_c");
  private static final Schema SCHEMA = TestDynamicIcebergSinkIndexDv.SCHEMA_V1;
  private static final PartitionSpec BUCKETED =
      PartitionSpec.builderFor(SCHEMA).bucket("id", 3).build();
  private static final TypeInformation<Row> ROW_TYPE =
      Types.ROW(Types.STRING, Types.INT, Types.STRING);

  private static final int PARALLELISM = 2;
  private static final int RECORDS = 600;
  private static final int KEYS = 40;
  private static final int BATCH = 60;
  private static final int HOLD = 300;
  private static final int STALE_HOLD = 240;
  private static final int STALE_HOLD_AFTER = 420;
  private static final long CHECKPOINT_INTERVAL_MS = 200L;
  private static final int CHECKPOINTS_TO_COMMIT = 3;
  private static final long REWRITE_PAUSE_MS = 500L;

  private static final AtomicInteger REWRITES = new AtomicInteger();
  private static final List<String> REWRITE_FAILURES =
      Collections.synchronizedList(Lists.newArrayList());

  @TempDir private Path checkpointDir;
  @TempDir private Path compactionDir;

  private String mode;
  private boolean bucketed;
  private final List<String> jobFailures = Lists.newArrayList();

  @BeforeEach
  void before() {
    this.mode = System.getenv().getOrDefault("DYN_COMPACTION_MODE", INDEX_DV);
    for (String name : TABLES) {
      createTable(name, PartitionSpec.unpartitioned());
    }

    Control.reset();
    DynamicIndexDvDataWriter.REWRITES_APPLIED.set(0);
    IndexDvCommits.resetCounters();
    REWRITES.set(0);
    REWRITE_FAILURES.clear();
  }

  @AfterEach
  void after() {
    DynamicCommitter.setBeforeCommitAttempt((table, checkpoint) -> {});
    DynamicCommitter.setBeforeCommitRequests((table, checkpoint) -> {});
  }

  /** c1: a full rewrite of t_a lands after the latest checkpoint, then restore it. */
  @Test
  void c1RewriteAfterLatestCheckpoint() throws Exception {
    String latest = runHeldAt(HOLD);
    compact(null);
    runToEnd(latest, 0);
    expect(report("c1-full-rewrite-after-latest", RECORDS), "OK");
  }

  /** c2: as c1, rewriting only one bucket of t_a, partitioned by bucket(id, 3). */
  @Test
  void c2PartialRewriteAfterLatestCheckpoint() throws Exception {
    bucketTarget();
    String latest = runHeldAt(HOLD);
    compact(0);
    runToEnd(latest, 0);
    expect(report("c2-partial-rewrite-after-latest", RECORDS), "OK");
  }

  /** c3a: restore a checkpoint where the only newer commit to any table is a compaction of t_a. */
  @Test
  void c3aStaleRestoreOnlyCompactionAfter() throws Exception {
    String behind;
    try (Cluster cluster = new Cluster()) {
      Control.holdAt = STALE_HOLD;
      JobClient job = start(null, 0);
      awaitHoldAndCommits(job);
      behind = latestCheckpoint(job.getJobID());
      compact(null);
      awaitCheckpoints(job, CHECKPOINTS_TO_COMMIT);
      cancel(job);
    }

    Control.holdAt = -1;
    runToEnd(behind, 0);
    expect(report("c3a-stale-only-compaction-after", RECORDS), "OK");
  }

  /** c3b: sink commits after the restored checkpoint, then a compaction rewrites them. */
  @Test
  void c3bStaleRestoreCommitsThenCompaction() throws Exception {
    String behind = runStaleThenCancel(false);
    compact(null);
    runToEnd(behind, 0);
    expect(report("c3b-stale-commits-then-compaction", RECORDS), "LOUD");
  }

  /** c3c: a compaction, then sink commits, both after the restored checkpoint. */
  @Test
  void c3cStaleRestoreCompactionThenCommits() throws Exception {
    String behind = runStaleThenCancel(true);
    runToEnd(behind, 0);
    expect(report("c3c-stale-compaction-then-commits", RECORDS), "LOUD");
  }

  /** c4: rows retired by DVs stay deleted when the rewrite drops them and the job restores. */
  @Test
  void c4RewriteOfFilesWithLiveDvsKeepsDeletes() throws Exception {
    String latest = runHeldAt(HOLD);
    int liveDvsBefore = deleteStats(table(TARGET))[2];
    compact(null);
    Control.endAt = HOLD;
    runToEnd(latest, 0);
    System.out.printf("NOTE c4 live DVs rewritten by the compaction=%d%n", liveDvsBefore);
    String verdict = report("c4-rewrite-drops-dv-rows", HOLD);
    if (INDEX_DV.equals(mode)) {
      assertThat(liveDvsBefore).isPositive();
    }

    expect(verdict, "OK");
  }

  /** c5: as c3b, then expire every snapshot of t_a but the head. */
  @Test
  void c5StaleRestoreAfterCompactionAndExpire() throws Exception {
    String behind = runStaleThenCancel(false);
    compact(null);
    expireAllButHead();
    runToEnd(behind, 0);
    expect(report("c5-stale-compaction-expired", RECORDS), "LOUD");
  }

  /** c5a: as c1, then expire every snapshot of t_a but the head. */
  @Test
  void c5aLatestRestoreAfterCompactionAndExpire() throws Exception {
    String latest = runHeldAt(HOLD);
    compact(null);
    expireAllButHead();
    runToEnd(latest, 0);
    expect(report("c5a-latest-compaction-expired", RECORDS), "OK");
  }

  /** c6: every live DV of t_a is rewritten into a new DV file before the restore. */
  @Test
  void c6DvRewriteAfterLatestCheckpoint() throws Exception {
    String latest = runHeldAt(HOLD);
    int rewritten = rewriteDvs();
    runToEnd(latest, 0);
    System.out.printf("NOTE c6 DVs rewritten=%d%n", rewritten);
    expect(report("c6-dv-rewrite-after-latest", RECORDS), "OK");
  }

  /** k1a: a full rewrite lands after t_a's commit is requested, before its row delta is built. */
  @Test
  void k1aRewriteBeforeCommitStarts() throws Exception {
    onCommitRequests(ImmutableSet.of(3, 6), () -> compact(null));
    runToEnd(null, 0);
    expect(concurrentReport("k1a-full-rewrite-before-commit"), "OK");
  }

  /** k1b: as k1a, rewriting one bucket. */
  @Test
  void k1bPartialRewriteBeforeCommitStarts() throws Exception {
    bucketTarget();
    onCommitRequests(ImmutableSet.of(3, 6), () -> compact(0));
    runToEnd(null, 0);
    expect(concurrentReport("k1b-partial-rewrite-before-commit"), "OK");
  }

  /** k1c: as k1a, rewriting every live DV into new DV files. */
  @Test
  void k1cDvRewriteBeforeCommitStarts() throws Exception {
    onCommitRequests(ImmutableSet.of(4, 7), () -> rewriteDvs() > 0);
    runToEnd(null, 0);
    expect(concurrentReport("k1c-dv-rewrite-before-commit"), "OK");
  }

  /** k2a: a full rewrite commits right before t_a's commit attempt. */
  @Test
  void k2aRewriteRacesCommit() throws Exception {
    onCommitAttempts(ImmutableSet.of(3, 7), () -> compact(null));
    runToEnd(null, 0);
    expect(concurrentReport("k2a-full-rewrite-races-commit"), "OK");
  }

  /** k2b: as k2a, rewriting one bucket. */
  @Test
  void k2bPartialRewriteRacesCommit() throws Exception {
    bucketTarget();
    onCommitAttempts(ImmutableSet.of(3, 7), () -> compact(0));
    runToEnd(null, 0);
    expect(concurrentReport("k2b-partial-rewrite-races-commit"), "OK");
  }

  /** k2c: as k2a, rewriting every live DV into new DV files. */
  @Test
  void k2cDvRewriteRacesCommit() throws Exception {
    onCommitAttempts(ImmutableSet.of(4, 8), () -> rewriteDvs() > 0);
    runToEnd(null, 0);
    expect(concurrentReport("k2c-dv-rewrite-races-commit"), "OK");
  }

  /** k2d: a split rewrite (1 to 2 files per partition) then a full rewrite race one commit. */
  @Test
  void k2dSplitThenRewriteRacesCommit() throws Exception {
    onCommitAttempts(ImmutableSet.of(4), () -> compact(null, 2) && compact(null));
    runToEnd(null, 0);
    expect(concurrentReport("k2d-split-then-rewrite-races-commit"), "OK");
  }

  /** k3a: a full rewrite races the first attempt of every t_a commit, up to 12 times. */
  @Test
  void k3aRewriteRacesEveryCommit() throws Exception {
    AtomicLong lastCheckpoint = new AtomicLong(-1);
    DynamicCommitter.setBeforeCommitAttempt(
        targetOnly(
            checkpointId -> {
              if (lastCheckpoint.getAndSet(checkpointId) != checkpointId && REWRITES.get() < 12) {
                runRewrite(() -> compact(null));
              }
            }));
    runToEnd(null, 0);
    expect(concurrentReport("k3a-rewrite-races-every-commit"), "OK");
  }

  /** k3b: a full rewrite races six t_a attempts in a row; the job may restart twice. */
  @Test
  void k3bRewriteStarvesOneCommit() throws Exception {
    onCommitAttempts(ImmutableSet.of(3, 4, 5, 6, 7, 8), () -> compact(null));
    runToEnd(null, 2);
    expect(concurrentReport("k3b-rewrite-starves-one-commit"), "OK");
  }

  /** k4: external full rewrites of t_a every 500 ms on another thread while the job runs. */
  @Test
  void k4BackgroundRewritesWhileRunning() throws Exception {
    try (BackgroundRewrites rewrites =
        new BackgroundRewrites(() -> compact(null), REWRITE_PAUSE_MS)) {
      runToEnd(null, 0);
    }

    expect(concurrentReport("k4-background-full-rewrites"), "OK");
  }

  /** k4x: back-to-back rewrites of t_a, 3 restarts allowed. Reported, not asserted. */
  @Test
  void k4xBackToBackRewritesWhileRunning() throws Exception {
    try (BackgroundRewrites rewrites = new BackgroundRewrites(() -> compact(null), 0)) {
      runToEnd(null, 3);
    }

    String verdict = concurrentReport("k4x-back-to-back-full-rewrites");
    if (INDEX_DV.equals(mode)) {
      assertThat(verdict).isNotEqualTo("SILENT_WRONG");
    }
  }

  // ---------------------------------------------------------------------------------------------

  private String concurrentReport(String scenario) throws IOException {
    System.out.printf(
        "NOTE %s mode=%s rewritesCommitted=%d rewriteFailures=%d %s sinkRetries=%d%n",
        scenario,
        mode,
        REWRITES.get(),
        REWRITE_FAILURES.size(),
        Lists.newArrayList(REWRITE_FAILURES).stream().distinct().limit(3).toList(),
        IndexDvCommits.retries());
    return report(scenario, RECORDS);
  }

  private static BiConsumer<String, Long> targetOnly(LongConsumer hook) {
    return (name, checkpointId) -> {
      if (name.endsWith("." + TARGET)) {
        hook.accept(checkpointId);
      }
    };
  }

  /** Runs the rewrite at the given (1-based) commit requests of t_a. */
  private void onCommitRequests(Set<Integer> requests, Rewrite rewrite) {
    AtomicInteger calls = new AtomicInteger();
    DynamicCommitter.setBeforeCommitRequests(
        targetOnly(
            checkpointId -> {
              if (requests.contains(calls.incrementAndGet())) {
                runRewrite(rewrite);
              }
            }));
  }

  /** Runs the rewrite at the given (1-based) commit attempts of t_a, retries included. */
  private void onCommitAttempts(Set<Integer> attempts, Rewrite rewrite) {
    AtomicInteger calls = new AtomicInteger();
    DynamicCommitter.setBeforeCommitAttempt(
        targetOnly(
            checkpointId -> {
              if (attempts.contains(calls.incrementAndGet())) {
                runRewrite(rewrite);
              }
            }));
  }

  private static void runRewrite(Rewrite rewrite) {
    try {
      if (rewrite.run()) {
        REWRITES.incrementAndGet();
      }
    } catch (Exception e) {
      String message = String.valueOf(e.getMessage());
      REWRITE_FAILURES.add(
          e.getClass().getSimpleName()
              + ": "
              + message.substring(0, Math.min(60, message.length())));
    }
  }

  @FunctionalInterface
  private interface Rewrite {
    /** Returns whether a rewrite was committed. */
    boolean run() throws Exception;
  }

  /** Runs a rewrite in a loop on its own thread until closed. */
  private static final class BackgroundRewrites implements AutoCloseable {
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread thread;

    BackgroundRewrites(Rewrite rewrite, long pauseMillis) {
      this.thread =
          new Thread(
              () -> {
                while (running.get()) {
                  runRewrite(rewrite);
                  try {
                    Thread.sleep(pauseMillis);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                  }
                }
              },
              "background-rewrites");
      thread.start();
    }

    @Override
    public void close() throws InterruptedException {
      running.set(false);
      thread.join();
    }
  }

  private String runHeldAt(int hold) throws Exception {
    try (Cluster cluster = new Cluster()) {
      Control.holdAt = hold;
      JobClient job = start(null, 0);
      awaitHoldAndCommits(job);
      String latest = latestCheckpoint(job.getJobID());
      cancel(job);
      Control.holdAt = -1;
      return latest;
    }
  }

  private String runStaleThenCancel(boolean compactBeforeCommits) throws Exception {
    try (Cluster cluster = new Cluster()) {
      Control.holdAt = STALE_HOLD;
      JobClient job = start(null, 0);
      awaitHoldAndCommits(job);
      String behind = latestCheckpoint(job.getJobID());
      if (compactBeforeCommits) {
        compact(null);
      }

      Control.holdAt = STALE_HOLD_AFTER;
      awaitHoldAndCommits(job);
      cancel(job);
      Control.holdAt = -1;
      return behind;
    }
  }

  private void expect(String verdict, String expected) {
    if (INDEX_DV.equals(mode)) {
      assertThat(verdict).isEqualTo(expected);
    }
  }

  private void createTable(String name, PartitionSpec spec) {
    CATALOG_EXTENSION
        .catalog()
        .createTable(
            TableIdentifier.of(DATABASE, name),
            SCHEMA,
            spec,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
  }

  private void bucketTarget() {
    CATALOG_EXTENSION.catalog().dropTable(TableIdentifier.of(DATABASE, TARGET));
    createTable(TARGET, BUCKETED);
    this.bucketed = true;
  }

  private static Table table(String name) {
    return CATALOG_EXTENSION.catalog().loadTable(TableIdentifier.of(DATABASE, name));
  }

  /** Rewrites the live rows of every data file of t_a, or of one bucket only. */
  private boolean compact(Integer bucket) throws IOException {
    return compact(bucket, 1);
  }

  /** As compact, splitting each partition's live rows by key across the given number of files. */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private boolean compact(Integer bucket, int filesPerPartition) throws IOException {
    Table table = table(TARGET);
    Snapshot head = table.currentSnapshot();
    if (head == null) {
      return false;
    }

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
    int added = 0;
    for (Map.Entry<StructLike, List<Record>> entry : liveByPartition.entrySet()) {
      for (int split = 0; split < filesPerPartition; split++) {
        List<Record> rows = Lists.newArrayList();
        for (Record record : entry.getValue()) {
          if ((Integer) record.getField("id") % filesPerPartition == split) {
            rows.add(record);
          }
        }

        if (!rows.isEmpty()) {
          rewrite.addFile(
              table.spec().isUnpartitioned()
                  ? appender.writeFile(rows)
                  : appender.writeFile(entry.getKey(), rows));
          added++;
        }
      }
    }

    rewrite.commit();
    System.out.printf(
        "NOTE compaction bucket=%s removedDataFiles=%d addedDataFiles=%d%n",
        bucket, removed, added);
    return true;
  }

  /** Rewrites every live DV of t_a into a new DV file, like rewrite-position-deletes. */
  private int rewriteDvs() throws IOException {
    Table table = table(TARGET);
    if (table.currentSnapshot() == null) {
      return 0;
    }

    DeleteLoader loader = new BaseDeleteLoader(file -> table.io().newInputFile(file));
    RewriteFiles rewrite =
        table.newRewrite().validateFromSnapshot(table.currentSnapshot().snapshotId());
    int count = 0;
    for (DeleteFile dv : deleteFiles(table, table.currentSnapshot())) {
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
                      .newOutputFile(
                          compactionDir
                              .resolve("dv-" + System.nanoTime() + "-" + count + ".puffin")
                              .toString()),
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
    Table table = table(TARGET);
    table.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1).retainLast(1).commit();
    System.out.printf(
        "NOTE expired, snapshots left=%d%n", Lists.newArrayList(table(TARGET).snapshots()).size());
  }

  // ---------------------------------------------------------------------------------------------

  private void runToEnd(String restorePath, int restarts) throws Exception {
    Control.finishAtEnd = true;
    try (Cluster cluster = new Cluster()) {
      JobClient job = start(restorePath, restarts);
      try {
        job.getJobExecutionResult().get(180, TimeUnit.SECONDS);
      } catch (Exception e) {
        jobFailures.add(rootMessage(e));
      }
    }
  }

  private JobClient start(String restorePath, int restarts) throws Exception {
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

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(PARALLELISM);
    env.setMaxParallelism(16);
    DataStream<Row> input =
        env.addSource(new ReplaySource(), ROW_TYPE).uid("source").setParallelism(1);
    DynamicIcebergSink.forInput(input)
        .generator(new Generator(bucketed))
        .catalogLoader(CATALOG_EXTENSION.catalogLoader())
        .immediateTableUpdate(true)
        .writeParallelism(PARALLELISM)
        .uidPrefix("dyn")
        .set(FlinkWriteOptions.UPSERT_MODE.key(), mode)
        .append();
    return env.executeAsync("dynamic compaction scenario " + mode);
  }

  private void awaitHoldAndCommits(JobClient job) throws Exception {
    awaitOrFail(job, Control.HOLD_REACHED::get);
    awaitCheckpoints(job, CHECKPOINTS_TO_COMMIT);
    Control.HOLD_REACHED.set(false);
  }

  private void awaitCheckpoints(JobClient job, int count) throws Exception {
    int target = Control.COMPLETED_CHECKPOINTS.get() + count;
    awaitOrFail(job, () -> Control.COMPLETED_CHECKPOINTS.get() >= target);
  }

  private void cancel(JobClient job) throws Exception {
    job.cancel().get(60, TimeUnit.SECONDS);
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

  // ---------------------------------------------------------------------------------------------

  /**
   * Prints one RESULT line per table and returns the overall verdict: LOUD when the job failed,
   * SILENT_WRONG when any table is wrong, OK otherwise.
   */
  private String report(String scenario, int upTo) throws IOException {
    boolean wrong = false;
    for (String name : TABLES) {
      wrong |= reportTable(scenario, name, upTo);
    }

    String verdict = !jobFailures.isEmpty() ? "LOUD" : wrong ? "SILENT_WRONG" : "OK";
    System.out.printf(
        "RESULT scenario=%s mode=%s verdict=%s rewritesCommitted=%d rewriteFailures=%d"
            + " sinkRetries=%d rebasedDvs=%d rewritesApplied=%d jobFailures=%s%n",
        scenario,
        mode,
        verdict,
        REWRITES.get(),
        REWRITE_FAILURES.size(),
        IndexDvCommits.retries(),
        IndexDvCommits.rebasedDvs(),
        DynamicIndexDvDataWriter.REWRITES_APPLIED.get(),
        jobFailures);
    return verdict;
  }

  /** Returns whether the table is wrong. */
  private boolean reportTable(String scenario, String name, int upTo) throws IOException {
    Table table = table(name);
    Map<String, Integer> actual = Maps.newHashMap();
    Map<Integer, Integer> perKey = Maps.newHashMap();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        actual.merge(record.getField("id") + ":" + record.getField("data"), 1, Integer::sum);
        perKey.merge((Integer) record.getField("id"), 1, Integer::sum);
      }
    }

    Set<String> expected = expectedRows(name, upTo);
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

    int duplicateKeys = (int) perKey.values().stream().filter(count -> count > 1).count();
    int[] deletes = deleteStats(table);
    long replaces =
        Lists.newArrayList(table.snapshots()).stream()
            .filter(snapshot -> "replace".equals(snapshot.operation()))
            .count();
    boolean wrong =
        missing > 0
            || unexpected > 0
            || duplicates > 0
            || duplicateKeys > 0
            || deletes[3] > 0
            || deletes[4] > 0;
    System.out.printf(
        "TABLE scenario=%s mode=%s table=%s expected=%d actual=%d duplicates=%d unexpectedRows=%d"
            + " missing=%d duplicateKeys=%d eqDeleteFilesLive=%d dvsLive=%d danglingDvs=%d"
            + " multiDvFiles=%d replaceSnapshots=%d wrong=%s%n",
        scenario,
        mode,
        name,
        expected.size(),
        actualRows,
        duplicates,
        unexpected,
        missing,
        duplicateKeys,
        deletes[1],
        deletes[2],
        deletes[3],
        deletes[4],
        replaces,
        wrong);
    if (!TARGET.equals(name)) {
      assertThat(replaces).as("rewrites target %s only", TARGET).isZero();
    }

    return wrong;
  }

  private static Set<String> expectedRows(String name, int upTo) {
    Map<Integer, Integer> latest = Maps.newHashMap();
    for (int i = 0; i < upTo; i++) {
      if (tableOf(i).equals(name)) {
        latest.put(key(i), i);
      }
    }

    Set<String> rows = Sets.newHashSet();
    latest.forEach((k, i) -> rows.add(k + ":v" + i));
    return rows;
  }

  /** Eq delete files ever and live, live DVs, dangling DVs, extra DVs on one data file. */
  private static int[] deleteStats(Table table) throws IOException {
    Snapshot head = table.currentSnapshot();
    if (head == null) {
      return new int[] {0, 0, 0, 0, 0};
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
    for (DeleteFile file : deleteFiles(table, head)) {
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

    return new int[] {0, liveEq, dvs, dangling, multiDv};
  }

  private static List<DeleteFile> deleteFiles(Table table, Snapshot snapshot) throws IOException {
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

  private static String tableOf(int record) {
    return TABLES.get(record % TABLES.size());
  }

  private static int key(int record) {
    return (record / TABLES.size()) % KEYS;
  }

  // ---------------------------------------------------------------------------------------------

  /** Test-controlled switches shared by the source of every job run. */
  private static final class Control {
    private static final AtomicBoolean HOLD_REACHED = new AtomicBoolean();
    private static final AtomicInteger COMPLETED_CHECKPOINTS = new AtomicInteger();
    private static volatile int holdAt = -1;
    private static volatile int endAt = RECORDS;
    private static volatile boolean finishAtEnd = false;

    private Control() {}

    static void reset() {
      HOLD_REACHED.set(false);
      COMPLETED_CHECKPOINTS.set(0);
      holdAt = -1;
      endAt = RECORDS;
      finishAtEnd = false;
    }
  }

  /** Turns (table, id, data) rows into dynamic upsert records keyed by id. */
  private static class Generator implements DynamicRecordGenerator<Row> {
    private final boolean bucketedTarget;

    Generator(boolean bucketedTarget) {
      this.bucketedTarget = bucketedTarget;
    }

    @Override
    public void generate(Row row, Collector<DynamicRecord> out) {
      String name = (String) row.getField(0);
      DynamicRecord record =
          new DynamicRecord(
              TableIdentifier.of(DATABASE, name),
              SnapshotRef.MAIN_BRANCH,
              SCHEMA,
              GenericRowData.ofKind(
                  row.getKind(), row.getField(1), StringData.fromString((String) row.getField(2))),
              bucketedTarget && TARGET.equals(name) ? BUCKETED : PartitionSpec.unpartitioned(),
              DistributionMode.NONE,
              PARALLELISM * 2);
      record.setUpsertMode(true);
      record.setEqualityFields(ImmutableSet.of("id"));
      out.collect(record);
    }
  }

  /**
   * Emits record i as (table i % 3, (i / 3) % KEYS, "v" + i) from its checkpointed offset, one
   * batch per completed checkpoint, so a restore replays exactly the records after it.
   */
  private static class ReplaySource
      implements SourceFunction<Row>, CheckpointedFunction, CheckpointListener {
    private transient ListState<Integer> offsetState;
    private volatile int offset;
    private volatile boolean running = true;
    private final AtomicInteger completedHere = new AtomicInteger();

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
            ctx.collect(Row.ofKind(RowKind.INSERT, tableOf(i), key(i), "v" + i));
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

  /** One MiniCluster per job run. */
  private static final class Cluster implements AutoCloseable {
    private final MiniCluster miniCluster;

    Cluster() throws Exception {
      Configuration config =
          new Configuration(MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG);
      config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 50);
      this.miniCluster =
          new MiniCluster(
              new MiniClusterConfiguration.Builder()
                  .setConfiguration(config)
                  .setNumTaskManagers(1)
                  .setNumSlotsPerTaskManager(4)
                  .build());
      miniCluster.start();
      TestStreamEnvironment.setAsContext(miniCluster, PARALLELISM);
    }

    @Override
    public void close() throws Exception {
      TestStreamEnvironment.unsetAsContext();
      miniCluster.close();
    }
  }
}
