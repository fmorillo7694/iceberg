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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.maintenance.api.ExpireSnapshotsConfig;
import org.apache.iceberg.flink.maintenance.api.FlinkMaintenanceConfig;
import org.apache.iceberg.flink.maintenance.api.RewriteDataFilesConfig;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/** POC tests for the index-backed DV upsert mode of {@link FlinkSink}. */
@ExtendWith(ParameterizedTestExtension.class)
@Timeout(value = 180)
class TestIcebergSinkIndexDv {
  @RegisterExtension
  static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  private static final int MAX_PARALLELISM = 16;
  private static final String INDEX_DV = "index-dv";
  private static final String EQUALITY_DELETE = "equality-delete";

  @Parameter(index = 0)
  private int parallelism;

  @Parameter(index = 1)
  private boolean partitioned;

  @Parameter(index = 2)
  private DistributionMode distributionMode;

  @TempDir private Path savepointDir;
  @TempDir private Path compactionDir;

  @Parameters(name = "parallelism={0}, partitioned={1}, distribution={2}")
  static Object[][] parameters() {
    return new Object[][] {
      {1, false, DistributionMode.NONE},
      {2, false, DistributionMode.NONE},
      {2, true, DistributionMode.NONE},
      {2, true, DistributionMode.HASH},
      {2, false, DistributionMode.RANGE},
      {2, true, DistributionMode.RANGE},
    };
  }

  @TestTemplate
  void upsertsAcrossCheckpointsMatchEqualityDeleteMode() throws Exception {
    List<List<Row>> batches = randomChangelog(5, 40, 25, 42);
    Table eqTable = createTable("t_eq");
    Table dvTable = createTable("t_dv");

    runJob(eqTable, EQUALITY_DELETE, batches, null);
    runJob(dvTable, INDEX_DV, batches, null);

    List<String> expected = applyUpserts(Maps.newHashMap(), batches);
    assertThat(liveRows(dvTable)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(liveRows(dvTable)).containsExactlyInAnyOrderElementsOf(liveRows(eqTable));
    System.out.printf(
        "LAYOUT parallelism=%d partitioned=%s distribution=%s eqDataFiles=%s dvDataFiles=%s%n",
        parallelism,
        partitioned,
        distributionMode,
        dataFilesPerPartition(eqTable),
        dataFilesPerPartition(dvTable));
  }

  @TestTemplate
  void indexDvModeCommitsNoEqualityDeletes() throws Exception {
    List<List<Row>> batches = randomChangelog(5, 40, 25, 7);
    Table eqTable = createTable("t_eq");
    Table dvTable = createTable("t_dv");

    runJob(eqTable, EQUALITY_DELETE, batches, null);
    runJob(dvTable, INDEX_DV, batches, null);

    assertThat(countDeleteFiles(eqTable, FileContent.EQUALITY_DELETES)).isPositive();
    assertThat(countDeleteFiles(dvTable, FileContent.EQUALITY_DELETES)).isZero();
    assertThat(countDeleteFiles(dvTable, FileContent.POSITION_DELETES)).isPositive();
  }

  @TestTemplate
  void duplicateKeysInOneCheckpoint() throws Exception {
    List<List<Row>> batches =
        ImmutableList.of(
            ImmutableList.of(row("+I", 1, "a"), row("+I", 2, "x")),
            ImmutableList.of(
                row("+I", 1, "b"),
                row("+U", 1, "c"),
                row("-D", 1, "c"),
                row("+I", 1, "d"),
                row("-D", 2, "x"),
                row("+I", 2, "y"),
                row("+I", 2, "z")));
    Table table = createTable("t_dv");

    runJob(table, INDEX_DV, batches, null);

    assertThat(liveRows(table)).containsExactlyInAnyOrder("1:d", "2:z");
    assertThat(countDeleteFiles(table, FileContent.EQUALITY_DELETES)).isZero();
  }

  @TestTemplate
  void newJobBootstrapsIndexFromTable() throws Exception {
    List<List<Row>> first = randomChangelog(3, 40, 25, 11);
    List<List<Row>> second = randomChangelog(3, 40, 25, 12);
    Table table = createTable("t_dv");

    runJob(table, INDEX_DV, first, null);
    runJob(table, INDEX_DV, second, null);

    Map<Integer, String> state = Maps.newHashMap();
    applyUpserts(state, first);
    assertThat(liveRows(table)).containsExactlyInAnyOrderElementsOf(applyUpserts(state, second));
    assertThat(countDeleteFiles(table, FileContent.EQUALITY_DELETES)).isZero();
  }

  @TestTemplate
  void failoverRestoresIndexFromCheckpoint() throws Exception {
    List<List<Row>> batches = randomChangelog(5, 40, 25, 99);
    Table table = createTable("t_dv");
    FailOnce.reset(batches.get(0).size() + batches.get(1).size() + batches.get(2).size() + 1);

    runJob(table, INDEX_DV, batches, new FailOnce());

    assertThat(FailOnce.FAILED.get()).isTrue();
    assertThat(liveRows(table))
        .containsExactlyInAnyOrderElementsOf(applyUpserts(Maps.newHashMap(), batches));
    assertThat(countDeleteFiles(table, FileContent.EQUALITY_DELETES)).isZero();
  }

  @TestTemplate
  void savepointRestoreRescalesKeyedIndex() throws Exception {
    assumeThat(parallelism).isEqualTo(2);
    assumeThat(distributionMode).isNotEqualTo(DistributionMode.RANGE);
    List<List<Row>> first = randomChangelog(3, 60, 40, 21);
    List<List<Row>> second = randomChangelog(3, 60, 40, 22);
    Table table = createTable("t_dv");

    BatchesThenIdleSource.EMITTED_ALL.set(false);
    StreamExecutionEnvironment env = environment(new Configuration(), parallelism);
    DataStream<Row> input =
        env.addSource(new BatchesThenIdleSource(first), TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO)
            .uid("source");
    sink(input, table, INDEX_DV);
    JobClient job = env.executeAsync("index-dv before rescale");
    awaitOrFail(job, BatchesThenIdleSource.EMITTED_ALL::get);

    String savepoint =
        job.stopWithSavepoint(false, savepointDir.toString(), SavepointFormatType.CANONICAL)
            .get(60, TimeUnit.SECONDS);

    IndexDvDataWriter.BOOTSTRAPPED_KEYS.set(0);
    Configuration restore = new Configuration();
    restore.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
    int rescaled = parallelism * 2;
    StreamExecutionEnvironment restoredEnv = environment(restore, rescaled);
    DataStream<Row> restoredInput =
        restoredEnv
            .addSource(new BoundedTestSource<>(second), TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO)
            .uid("source");
    sink(restoredInput, table, INDEX_DV, rescaled, ImmutableMap.of(), false);
    restoredEnv.execute("index-dv after rescale");

    Map<Integer, String> state = Maps.newHashMap();
    applyUpserts(state, first);
    assertThat(liveRows(table)).containsExactlyInAnyOrderElementsOf(applyUpserts(state, second));
    assertThat(IndexDvDataWriter.BOOTSTRAPPED_KEYS.get()).isZero();
    assertThat(writerSubtasksOfLiveFiles(table)).contains(rescaled - 1);
    assertThat(countDeleteFiles(table, FileContent.EQUALITY_DELETES)).isZero();
  }

  @TestTemplate
  void flinkSinkVariantMatchesModel() throws Exception {
    assumeThat(parallelism).isEqualTo(2);
    assumeThat(distributionMode).isEqualTo(DistributionMode.NONE);
    List<List<Row>> batches = randomChangelog(5, 40, 25, 31);
    Table table = createTable("t_dv");
    StreamExecutionEnvironment env = environment(new Configuration(), parallelism);
    DataStream<Row> input =
        env.addSource(new BoundedTestSource<>(batches), TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO);
    sink(input, table, INDEX_DV, parallelism, ImmutableMap.of(), true);
    env.execute("index-dv FlinkSink variant");

    assertThat(liveRows(table))
        .containsExactlyInAnyOrderElementsOf(applyUpserts(Maps.newHashMap(), batches));
    assertThat(countDeleteFiles(table, FileContent.EQUALITY_DELETES)).isZero();
  }

  @TestTemplate
  void inJobRewriteKeepsIndexCorrect() throws Exception {
    assumeThat(parallelism).isEqualTo(2);
    assumeThat(distributionMode).isEqualTo(DistributionMode.NONE);
    runWithInJobMaintenance(maintenanceOptions(false), 61);
  }

  @TestTemplate
  void inJobRewriteExpireAndConverterTogether() throws Exception {
    assumeThat(parallelism).isEqualTo(2);
    assumeThat(distributionMode).isEqualTo(DistributionMode.NONE);
    assumeThat(partitioned).isFalse();
    runWithInJobMaintenance(maintenanceOptions(true), 62);
  }

  @TestTemplate
  void externalCompactionBetweenCheckpoints() throws Exception {
    assumeThat(parallelism).isEqualTo(2);
    assumeThat(distributionMode).isEqualTo(DistributionMode.NONE);
    assumeThat(partitioned).isFalse();
    List<List<Row>> batches = randomChangelog(6, 60, 30, 71);
    Table table = createTable("t_dv");
    IndexDvCommits.REBASED_DVS.set(0);

    PacedSource.reset(ImmutableList.of(1, 3));
    StreamExecutionEnvironment env = environment(new Configuration(), parallelism, false);
    DataStream<Row> input =
        env.addSource(new PacedSource(batches, 2), TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO);
    sink(input, table, INDEX_DV);
    JobClient job = env.executeAsync("index-dv external compaction");
    try {
      for (int batch : ImmutableList.of(1, 3)) {
        int pausedBatch = batch;
        awaitOrFail(job, () -> PacedSource.PAUSED_AT.get() == pausedBatch);
        compactAll(table);
        PacedSource.resume(batch);
      }

      job.getJobExecutionResult().get(120, TimeUnit.SECONDS);
    } finally {
      if (!job.getJobStatus().get().isTerminalState()) {
        job.cancel().get();
      }
    }

    assertThat(liveRows(table))
        .containsExactlyInAnyOrderElementsOf(applyUpserts(Maps.newHashMap(), batches));
    assertThat(countDeleteFiles(table, FileContent.EQUALITY_DELETES)).isZero();
    System.out.printf(
        "COMPACTION external rebasedDvs=%d retries=%d rewritesApplied=%d%n",
        IndexDvCommits.REBASED_DVS.get(),
        IndexDvCommits.RETRIES.get(),
        IndexDvDataWriter.REWRITES_APPLIED.get());
  }

  @TestTemplate
  void savepointRestoreAfterCompaction() throws Exception {
    assumeThat(parallelism).isEqualTo(2);
    assumeThat(distributionMode).isEqualTo(DistributionMode.NONE);
    assumeThat(partitioned).isFalse();
    List<List<Row>> first = randomChangelog(3, 60, 40, 81);
    List<List<Row>> second = randomChangelog(3, 60, 40, 82);
    Table table = createTable("t_dv");

    BatchesThenIdleSource.EMITTED_ALL.set(false);
    StreamExecutionEnvironment env = environment(new Configuration(), parallelism, false);
    DataStream<Row> input =
        env.addSource(new BatchesThenIdleSource(first), TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO)
            .uid("source");
    sink(input, table, INDEX_DV);
    JobClient job = env.executeAsync("index-dv before compaction");
    awaitOrFail(job, BatchesThenIdleSource.EMITTED_ALL::get);

    String savepoint =
        job.stopWithSavepoint(false, savepointDir.toString(), SavepointFormatType.CANONICAL)
            .get(60, TimeUnit.SECONDS);
    compactAll(table);

    Configuration restore = new Configuration();
    restore.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
    StreamExecutionEnvironment restoredEnv = environment(restore, parallelism, false);
    DataStream<Row> restoredInput =
        restoredEnv
            .addSource(new BoundedTestSource<>(second), TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO)
            .uid("source");
    sink(restoredInput, table, INDEX_DV);
    restoredEnv.execute("index-dv after compaction");

    Map<Integer, String> state = Maps.newHashMap();
    applyUpserts(state, first);
    assertThat(liveRows(table)).containsExactlyInAnyOrderElementsOf(applyUpserts(state, second));
    assertThat(countDeleteFiles(table, FileContent.EQUALITY_DELETES)).isZero();
  }

  private void runWithInJobMaintenance(Map<String, String> options, long seed) throws Exception {
    List<List<Row>> batches = randomChangelog(8, 60, 30, seed);
    Table table = createTable("t_dv");
    IndexDvCommits.REBASED_DVS.set(0);
    IndexDvCommits.RETRIES.set(0);
    IndexDvDataWriter.REWRITES_APPLIED.set(0);

    PacedSource.reset(ImmutableList.of());
    StreamExecutionEnvironment env = environment(new Configuration(), parallelism, false);
    DataStream<Row> input =
        env.addSource(new PacedSource(batches, 12), TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO);
    sink(input, table, INDEX_DV, parallelism, options, false);
    env.execute("index-dv with in-job maintenance");

    table.refresh();
    long rewrites =
        Lists.newArrayList(table.snapshots()).stream()
            .filter(snapshot -> "replace".equals(snapshot.operation()))
            .count();
    System.out.printf(
        "COMPACTION inJob partitioned=%s options=%s rewriteCommits=%d rebasedDvs=%d retries=%d "
            + "rewritesApplied=%d snapshots=%d%n",
        partitioned,
        options.keySet(),
        rewrites,
        IndexDvCommits.REBASED_DVS.get(),
        IndexDvCommits.RETRIES.get(),
        IndexDvDataWriter.REWRITES_APPLIED.get(),
        Lists.newArrayList(table.snapshots()).size());
    assertThat(rewrites).isPositive();
    assertThat(liveRows(table))
        .containsExactlyInAnyOrderElementsOf(applyUpserts(Maps.newHashMap(), batches));
    assertThat(countDeleteFiles(table, FileContent.EQUALITY_DELETES)).isZero();
  }

  private static Map<String, String> maintenanceOptions(boolean allTasks) {
    Map<String, String> options = Maps.newHashMap();
    options.put(FlinkWriteOptions.COMPACTION_ENABLE.key(), "true");
    options.put(RewriteDataFilesConfig.SCHEDULE_ON_COMMIT_COUNT, "1");
    options.put(RewriteDataFilesConfig.PREFIX + SizeBasedFileRewritePlanner.REWRITE_ALL, "true");
    options.put(FlinkMaintenanceConfig.RATE_LIMIT, "1");
    options.put(FlinkMaintenanceConfig.LOCK_CHECK_DELAY, "1");
    if (allTasks) {
      options.put(FlinkWriteOptions.EXPIRE_SNAPSHOTS_ENABLE.key(), "true");
      options.put(ExpireSnapshotsConfig.SCHEDULE_ON_COMMIT_COUNT, "3");
      options.put(ExpireSnapshotsConfig.RETAIN_LAST, "5");
      options.put(FlinkWriteOptions.CONVERT_EQUALITY_DELETES_ENABLE.key(), "true");
    }

    return options;
  }

  /**
   * Rewrites all live rows of an unpartitioned table into one data file, like a compaction,
   * retrying when a concurrent sink commit adds deletes to the files being rewritten.
   */
  private void compactAll(Table table) throws IOException {
    for (int attempt = 1; ; attempt++) {
      try {
        compactOnce(table);
        return;
      } catch (ValidationException e) {
        if (attempt >= 5) {
          throw e;
        }
      }
    }
  }

  private static void awaitOrFail(JobClient job, BooleanSupplier condition) throws Exception {
    while (!condition.getAsBoolean()) {
      if (job.getJobStatus().get().isTerminalState()) {
        job.getJobExecutionResult().get();
        throw new IllegalStateException("Job ended before the expected condition");
      }

      Thread.sleep(50);
    }
  }

  private void compactOnce(Table table) throws IOException {
    table.refresh();
    Snapshot head = table.currentSnapshot();
    List<Record> live = Lists.newArrayList();
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(table).useSnapshot(head.snapshotId()).build()) {
      records.forEach(record -> live.add(record.copy()));
    }

    RewriteFiles rewrite = table.newRewrite().validateFromSnapshot(head.snapshotId());
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().useSnapshot(head.snapshotId()).planFiles()) {
      for (FileScanTask task : tasks) {
        rewrite.deleteFile(task.file());
      }
    }

    rewrite
        .addFile(
            new GenericAppenderHelper(table, FileFormat.PARQUET, compactionDir).writeFile(live))
        .commit();
  }

  @TestTemplate
  void upsertAfterPartitionSpecChange() throws Exception {
    assumeThat(partitioned).isFalse();
    Table table = createTable("t_dv");
    runJob(table, INDEX_DV, ImmutableList.of(initialRowsForSpecChange()), null);

    table.updateSpec().addField(Expressions.bucket("id", 4)).commit();
    runJob(table, INDEX_DV, ImmutableList.of(changesAfterSpecChange()), null);

    assertThat(liveRows(table)).containsExactlyInAnyOrder("1:ccc", "3:ddd");
  }

  @TestTemplate
  void equalityDeleteModeDuplicatesAfterPartitionSpecChange() throws Exception {
    assumeThat(partitioned).isFalse();
    assumeThat(distributionMode).isEqualTo(DistributionMode.NONE);
    Table table = createTable("t_eq");
    runJob(table, EQUALITY_DELETE, ImmutableList.of(initialRowsForSpecChange()), null);

    table.updateSpec().addField(Expressions.bucket("id", 4)).commit();
    runJob(table, EQUALITY_DELETE, ImmutableList.of(changesAfterSpecChange()), null);

    // Baseline for #18748: the old row of key 1 and the deleted key 2 stay visible.
    assertThat(liveRows(table)).containsExactlyInAnyOrder("1:aaa", "1:ccc", "2:bbb", "3:ddd");
  }

  private static List<Row> initialRowsForSpecChange() {
    return ImmutableList.of(row("+I", 1, "aaa"), row("+I", 2, "bbb"));
  }

  private static List<Row> changesAfterSpecChange() {
    return ImmutableList.of(row("+I", 1, "ccc"), row("-D", 2, "bbb"), row("+I", 3, "ddd"));
  }

  private Table createTable(String name) {
    PartitionSpec spec =
        partitioned
            ? PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).bucket("id", 3).build()
            : PartitionSpec.unpartitioned();
    Table table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TableIdentifier.of(DATABASE, name),
                SimpleDataUtil.SCHEMA,
                spec,
                ImmutableMap.of(
                    TableProperties.FORMAT_VERSION,
                    "3",
                    TableProperties.WRITE_DISTRIBUTION_MODE,
                    distributionMode.modeName()));
    if (distributionMode == DistributionMode.RANGE && !partitioned) {
      table.replaceSortOrder().asc("id").commit();
    }

    return table;
  }

  private StreamExecutionEnvironment environment(Configuration extra, int envParallelism) {
    return environment(extra, envParallelism, true);
  }

  private StreamExecutionEnvironment environment(
      Configuration extra, int envParallelism, boolean restartOnce) {
    Configuration config =
        new Configuration(MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG);
    if (restartOnce) {
      config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
      config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
    } else {
      config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
    }

    config.addAll(extra);
    return StreamExecutionEnvironment.getExecutionEnvironment(config)
        .enableCheckpointing(100L)
        .setParallelism(envParallelism)
        .setMaxParallelism(MAX_PARALLELISM);
  }

  private void runJob(Table table, String upsertMode, List<List<Row>> batches, FailOnce failer)
      throws Exception {
    StreamExecutionEnvironment env = environment(new Configuration(), parallelism);
    DataStream<Row> input =
        env.addSource(new BoundedTestSource<>(batches), TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO);
    if (failer != null) {
      input = input.map(failer, TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO).setParallelism(1);
    }

    sink(input, table, upsertMode);
    env.execute("index-dv test " + upsertMode);
    table.refresh();
  }

  private void sink(DataStream<Row> input, Table table, String upsertMode) {
    sink(input, table, upsertMode, parallelism, ImmutableMap.of(), false);
  }

  private static void sink(
      DataStream<Row> input,
      Table table,
      String upsertMode,
      int writeParallelism,
      Map<String, String> options,
      boolean useFlinkSink) {
    TableLoader tableLoader =
        TableLoader.fromCatalog(
            CATALOG_EXTENSION.catalogLoader(), TableIdentifier.of(DATABASE, tableName(table)));
    if (useFlinkSink) {
      FlinkSink.forRow(input, SimpleDataUtil.FLINK_SCHEMA)
          .tableLoader(tableLoader)
          .resolvedSchema(SimpleDataUtil.FLINK_SCHEMA)
          .writeParallelism(writeParallelism)
          .equalityFieldColumns(ImmutableList.of("id"))
          .upsert(true)
          .uidPrefix("sink")
          .set(FlinkWriteOptions.UPSERT_MODE.key(), upsertMode)
          .setAll(options)
          .append();
    } else {
      IcebergSink.forRow(input, SimpleDataUtil.FLINK_SCHEMA)
          .tableLoader(tableLoader)
          .writeParallelism(writeParallelism)
          .equalityFieldColumns(ImmutableList.of("id"))
          .upsert(true)
          .uidSuffix("sink")
          .set(FlinkWriteOptions.UPSERT_MODE.key(), upsertMode)
          .setAll(options)
          .append();
    }
  }

  private static String tableName(Table table) {
    String name = table.name();
    return name.substring(name.lastIndexOf('.') + 1);
  }

  private static List<String> liveRows(Table table) throws IOException {
    table.refresh();
    List<String> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        rows.add(record.getField("id") + ":" + record.getField("data"));
      }
    }

    return rows;
  }

  private static Map<String, Integer> dataFilesPerPartition(Table table) throws IOException {
    table.refresh();
    Map<String, Integer> counts = Maps.newTreeMap();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        counts.merge(task.file().partition().toString(), 1, Integer::sum);
      }
    }

    return counts;
  }

  private static List<Integer> writerSubtasksOfLiveFiles(Table table) throws IOException {
    List<Integer> subtasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile file = task.file();
        String name = file.location().substring(file.location().lastIndexOf('/') + 1);
        subtasks.add(Integer.parseInt(name.substring(0, name.indexOf('-'))));
      }
    }

    return subtasks;
  }

  private static int countDeleteFiles(Table table, FileContent content) throws IOException {
    int count = 0;
    for (Snapshot snapshot : table.snapshots()) {
      for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
        try (ManifestReader<DeleteFile> reader =
            ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
          for (DeleteFile file : reader) {
            if (file.content() == content) {
              count++;
            }
          }
        }
      }
    }

    return count;
  }

  /** Applies changelog batches with upsert semantics and returns the resulting rows. */
  private static List<String> applyUpserts(Map<Integer, String> state, List<List<Row>> batches) {
    for (List<Row> batch : batches) {
      for (Row row : batch) {
        int id = (int) row.getField(0);
        switch (row.getKind()) {
          case INSERT, UPDATE_AFTER -> state.put(id, (String) row.getField(1));
          case DELETE -> state.remove(id);
          case UPDATE_BEFORE -> {}
        }
      }
    }

    List<String> rows = Lists.newArrayList();
    state.forEach((id, data) -> rows.add(id + ":" + data));
    return rows;
  }

  private static List<List<Row>> randomChangelog(
      int checkpoints, int rowsPerCheckpoint, int keys, long seed) {
    Random random = new Random(seed);
    List<List<Row>> batches = Lists.newArrayList();
    for (int c = 0; c < checkpoints; c++) {
      List<Row> batch = Lists.newArrayList();
      for (int i = 0; i < rowsPerCheckpoint; i++) {
        int id = random.nextInt(keys);
        String data = "v" + seed + "_" + c + "_" + i;
        int kind = random.nextInt(10);
        if (kind < 5) {
          batch.add(row("+I", id, data));
        } else if (kind < 8) {
          batch.add(row("+U", id, data));
        } else if (kind < 9) {
          batch.add(row("-U", id, data));
        } else {
          batch.add(row("-D", id, data));
        }
      }

      batches.add(batch);
    }

    return batches;
  }

  private static Row row(String kind, int id, String data) {
    RowKind rowKind =
        switch (kind) {
          case "+I" -> RowKind.INSERT;
          case "+U" -> RowKind.UPDATE_AFTER;
          case "-U" -> RowKind.UPDATE_BEFORE;
          case "-D" -> RowKind.DELETE;
          default -> throw new IllegalArgumentException("Unknown row kind: " + kind);
        };
    return Row.ofKind(rowKind, id, data);
  }

  /**
   * Emits each batch after the given number of completed checkpoints, optionally pausing after
   * chosen batches until the test resumes it, then finishes.
   */
  private static class PacedSource implements SourceFunction<Row>, CheckpointListener {
    private static final AtomicInteger PAUSED_AT = new AtomicInteger(-1);
    private static final AtomicInteger RESUMED_AT = new AtomicInteger(-1);
    private static volatile List<Integer> pauseAfter = ImmutableList.of();

    private final List<List<Row>> batches;
    private final int checkpointsBetweenBatches;
    private final AtomicInteger completedCheckpoints = new AtomicInteger();
    private volatile boolean running = true;

    PacedSource(List<List<Row>> batches, int checkpointsBetweenBatches) {
      this.batches = batches;
      this.checkpointsBetweenBatches = checkpointsBetweenBatches;
    }

    static void reset(List<Integer> batchesToPauseAfter) {
      PAUSED_AT.set(-1);
      RESUMED_AT.set(-1);
      pauseAfter = batchesToPauseAfter;
    }

    static void resume(int batch) {
      RESUMED_AT.set(batch);
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
      for (int b = 0; b < batches.size() && running; b++) {
        synchronized (ctx.getCheckpointLock()) {
          batches.get(b).forEach(ctx::collect);
        }

        int target = completedCheckpoints.get() + checkpointsBetweenBatches;
        while (running && completedCheckpoints.get() < target) {
          Thread.sleep(10);
        }

        if (pauseAfter.contains(b)) {
          PAUSED_AT.set(b);
          while (running && RESUMED_AT.get() != b) {
            Thread.sleep(10);
          }
        }
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

  /** Fails the job once, after the given number of records, to force a restore. */
  private static class FailOnce implements MapFunction<Row, Row> {
    private static final AtomicInteger SEEN = new AtomicInteger();
    private static final AtomicBoolean FAILED = new AtomicBoolean();
    private static volatile int failAt;

    static void reset(int recordCount) {
      SEEN.set(0);
      FAILED.set(false);
      failAt = recordCount;
    }

    @Override
    public Row map(Row value) {
      if (SEEN.incrementAndGet() >= failAt && FAILED.compareAndSet(false, true)) {
        throw new IllegalStateException("Injected failure to test restore");
      }

      return value;
    }
  }

  /** Emits each batch after two completed checkpoints, then idles until stopped. */
  private static class BatchesThenIdleSource implements SourceFunction<Row>, CheckpointListener {
    private static final AtomicBoolean EMITTED_ALL = new AtomicBoolean();

    private final List<List<Row>> batches;
    private final AtomicInteger completedCheckpoints = new AtomicInteger();
    private volatile boolean running = true;

    BatchesThenIdleSource(List<List<Row>> batches) {
      this.batches = batches;
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
      for (List<Row> batch : batches) {
        synchronized (ctx.getCheckpointLock()) {
          batch.forEach(ctx::collect);
        }

        int target = completedCheckpoints.get() + 2;
        while (running && completedCheckpoints.get() < target) {
          Thread.sleep(10);
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

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      completedCheckpoints.incrementAndGet();
    }
  }
}
