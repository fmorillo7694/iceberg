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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.util.ContentFileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/** POC: the index-dv upsert mode of the DynamicIcebergSink, over several tables in one stream. */
@Timeout(value = 180)
class TestDynamicIcebergSinkIndexDv {
  @RegisterExtension
  static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  private static final String INDEX_DV = "index-dv";
  private static final String EQUALITY_DELETE = "equality-delete";
  private static final int PARALLELISM = 2;
  private static final int MAX_PARALLELISM = 16;
  private static final List<String> TABLES = ImmutableList.of("t_a", "t_b", "t_c");

  static final Schema SCHEMA_V1 =
      new Schema(
          ImmutableList.of(
              NestedField.required(1, "id", IntegerType.get()),
              NestedField.optional(2, "data", StringType.get())),
          ImmutableSet.of(1));
  static final Schema SCHEMA_V2 =
      new Schema(
          ImmutableList.of(
              NestedField.required(1, "id", IntegerType.get()),
              NestedField.optional(2, "data", StringType.get()),
              NestedField.optional(3, "extra", StringType.get())),
          ImmutableSet.of(1));
  private static final TypeInformation<Row> INPUT_TYPE =
      Types.ROW(Types.STRING, Types.INT, Types.STRING, Types.INT);
  private static final TableCreator V3_TABLES =
      (catalog, identifier, schema, spec) ->
          catalog.createTable(
              identifier, schema, spec, ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

  @TempDir private Path savepointDir;
  @TempDir private Path appendDir;
  @TempDir private Path jdbcDir;

  @BeforeEach
  void before() {
    DynamicIndexDvDataWriter.TABLE_BOOTSTRAPS.set(0);
    DynamicIndexDvDataWriter.BOOTSTRAPPED_KEYS.set(0);
    DynamicIndexDvDataWriter.LEGACY_REBUILDS.set(0);
    BatchesSource.PAUSED.set(false);
    BatchesSource.RESUME.set(false);
  }

  @AfterEach
  void after() {
    DynamicIndexDvKeys.setTestTableIds(null);
    DynamicIndexDvDataWriter.setWriteLegacyTableState(false);
  }

  @Test
  void upsertsAcrossTablesMatchModelAndEqualityDeleteMode() throws Exception {
    List<List<Row>> batches = changelog(TABLES, 5, 60, 20, 11, 1);
    run(batches, INDEX_DV, "");
    run(batches, EQUALITY_DELETE, "_eq");

    Map<String, Map<Integer, String>> model = model(batches);
    for (String table : TABLES) {
      assertThat(liveRows(table)).as(table).containsExactlyInAnyOrderElementsOf(rows(model, table));
      assertThat(filesWithSeveralDvs(table)).as(table).isEmpty();
      assertThat(liveRows(table))
          .as(table)
          .containsExactlyInAnyOrderElementsOf(liveRows(table + "_eq"));
      assertThat(countDeleteFiles(table, FileContent.EQUALITY_DELETES)).as(table).isZero();
      assertThat(countDeleteFiles(table + "_eq", FileContent.EQUALITY_DELETES))
          .as(table)
          .isPositive();
    }
  }

  @Test
  void tableAppearingMidStreamIsBootstrappedEmpty() throws Exception {
    List<List<Row>> batches =
        Lists.newArrayList(changelog(ImmutableList.of("t_a", "t_b"), 2, 40, 15, 12, 1));
    batches.addAll(changelog(TABLES, 2, 60, 15, 13, 1));
    run(batches, INDEX_DV, "");

    Map<String, Map<Integer, String>> model = model(batches);
    for (String table : TABLES) {
      assertThat(liveRows(table)).as(table).containsExactlyInAnyOrderElementsOf(rows(model, table));
      assertThat(filesWithSeveralDvs(table)).as(table).isEmpty();
      assertThat(countDeleteFiles(table, FileContent.EQUALITY_DELETES)).isZero();
    }

    // Every key was written by this sink, so nothing is indexed from the tables themselves.
    assertThat(DynamicIndexDvDataWriter.BOOTSTRAPPED_KEYS.get()).isZero();
  }

  @Test
  void existingTablesAreBootstrappedOnFirstSight() throws Exception {
    List<List<Row>> first = changelog(TABLES, 3, 60, 20, 20, 1);
    List<List<Row>> second = changelog(TABLES, 3, 60, 20, 21, 1);
    run(first, EQUALITY_DELETE, "");
    int eqDeletesBefore = 0;
    for (String table : TABLES) {
      eqDeletesBefore += countDeleteFiles(table, FileContent.EQUALITY_DELETES);
    }

    run(second, INDEX_DV, "");

    Map<String, Map<Integer, String>> model = model(first);
    apply(model, second);
    for (String table : TABLES) {
      assertThat(liveRows(table)).as(table).containsExactlyInAnyOrderElementsOf(rows(model, table));
      assertThat(filesWithSeveralDvs(table)).as(table).isEmpty();
    }

    int eqDeletesAfter = 0;
    for (String table : TABLES) {
      eqDeletesAfter += countDeleteFiles(table, FileContent.EQUALITY_DELETES);
    }

    assertThat(DynamicIndexDvDataWriter.BOOTSTRAPPED_KEYS.get()).isPositive();
    assertThat(eqDeletesAfter).isEqualTo(eqDeletesBefore);
  }

  @Test
  void firstRecordAfterBootstrapUsesItsOwnKey() throws Exception {
    List<List<Row>> existing =
        ImmutableList.of(
            ImmutableList.of(
                Row.ofKind(RowKind.INSERT, "t_a", 1, "one", 1),
                Row.ofKind(RowKind.INSERT, "t_a", 2, "two", 1)));
    run(existing, EQUALITY_DELETE, "", 1);
    List<List<Row>> upserts =
        ImmutableList.of(ImmutableList.of(Row.ofKind(RowKind.INSERT, "t_a", 3, "three", 1)));
    run(upserts, INDEX_DV, "", 1);

    assertThat(DynamicIndexDvDataWriter.BOOTSTRAPPED_KEYS.get()).isEqualTo(2);
    assertThat(liveRows("t_a")).containsExactlyInAnyOrder("1:one", "2:two", "3:three");
  }

  @Test
  void failoverRestoresIndexFromCheckpoint() throws Exception {
    List<List<Row>> batches = changelog(TABLES, 5, 60, 20, 14, 1);
    FailOnce.reset(batches.get(0).size() + batches.get(1).size() + batches.get(2).size() + 1);
    StreamExecutionEnvironment env = environment(new Configuration(), PARALLELISM, true);
    DataStream<Row> input =
        env.addSource(new BatchesSource(batches, false), INPUT_TYPE)
            .uid("source")
            .map(new FailOnce(), INPUT_TYPE)
            .uid("fail-once")
            // chained to the source: a rebalance would reorder the records of a key
            .setParallelism(1);
    sink(input, INDEX_DV, "", PARALLELISM);
    env.execute("dynamic index-dv failover");

    assertThat(FailOnce.FAILED.get()).isTrue();
    Map<String, Map<Integer, String>> model = model(batches);
    for (String table : TABLES) {
      assertThat(liveRows(table)).as(table).containsExactlyInAnyOrderElementsOf(rows(model, table));
      assertThat(filesWithSeveralDvs(table)).as(table).isEmpty();
      assertThat(countDeleteFiles(table, FileContent.EQUALITY_DELETES)).isZero();
    }
  }

  @Test
  void savepointRestoreRescalesIndex() throws Exception {
    List<List<Row>> first = changelog(TABLES, 3, 60, 20, 15, 1);
    List<List<Row>> second = changelog(TABLES, 3, 60, 20, 16, 1);
    String savepoint = runUntilSavepoint(first, PARALLELISM);

    DynamicIndexDvDataWriter.BOOTSTRAPPED_KEYS.set(0);
    StreamExecutionEnvironment restoredEnv = restoredEnvironment(savepoint, PARALLELISM * 2);
    sink(
        restoredEnv.addSource(new BatchesSource(second, false), INPUT_TYPE).uid("source"),
        INDEX_DV,
        "",
        PARALLELISM * 2);
    restoredEnv.execute("dynamic index-dv after rescale");

    Map<String, Map<Integer, String>> model = model(first);
    apply(model, second);
    for (String table : TABLES) {
      assertThat(liveRows(table)).as(table).containsExactlyInAnyOrderElementsOf(rows(model, table));
      assertThat(filesWithSeveralDvs(table)).as(table).isEmpty();
      assertThat(countDeleteFiles(table, FileContent.EQUALITY_DELETES)).isZero();
    }

    assertThat(DynamicIndexDvDataWriter.BOOTSTRAPPED_KEYS.get()).isZero();
  }

  @Test
  void staleRestoreFailsOnlyForTheStaleTable() throws Exception {
    List<List<Row>> first = changelog(ImmutableList.of("t_a", "t_b"), 3, 40, 15, 17, 1);
    String savepoint = runUntilSavepoint(first, PARALLELISM);

    Table tableB = table("t_b");
    Record external = GenericRecord.create(tableB.schema());
    external.setField("id", 1000);
    external.setField("data", "external");
    new GenericAppenderHelper(tableB, FileFormat.PARQUET, appendDir)
        .appendToTable(ImmutableList.of(external));

    List<List<Row>> onlyA =
        ImmutableList.of(ImmutableList.of(Row.ofKind(RowKind.INSERT, "t_a", 1, "after", 1)));
    StreamExecutionEnvironment envA = restoredEnvironment(savepoint, PARALLELISM);
    sink(
        envA.addSource(new BatchesSource(onlyA, false), INPUT_TYPE).uid("source"),
        INDEX_DV,
        "",
        PARALLELISM);
    envA.execute("dynamic index-dv restore, fresh table only");

    Map<String, Map<Integer, String>> model = model(first);
    apply(model, onlyA);
    assertThat(liveRows("t_a")).containsExactlyInAnyOrderElementsOf(rows(model, "t_a"));

    List<List<Row>> onlyB =
        ImmutableList.of(ImmutableList.of(Row.ofKind(RowKind.INSERT, "t_b", 1, "after", 1)));
    StreamExecutionEnvironment envB = restoredEnvironment(savepoint, PARALLELISM);
    sink(
        envB.addSource(new BatchesSource(onlyB, false), INPUT_TYPE).uid("source"),
        INDEX_DV,
        "",
        PARALLELISM);
    assertThatThrownBy(() -> envB.execute("dynamic index-dv restore, stale table"))
        .rootCause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Restored index-dv state of table")
        .hasMessageContaining("t_b")
        .hasMessageContaining("Start the job without state");
  }

  @Test
  void schemaChangeAddingNonKeyColumnKeepsKeys() throws Exception {
    List<List<Row>> batches = Lists.newArrayList(changelog(TABLES, 2, 60, 15, 18, 1));
    batches.addAll(changelog(TABLES, 2, 60, 15, 19, 2));
    run(batches, INDEX_DV, "");

    Map<String, Map<Integer, String>> model = model(batches);
    for (String table : TABLES) {
      assertThat(liveRows(table)).as(table).containsExactlyInAnyOrderElementsOf(rows(model, table));
      assertThat(filesWithSeveralDvs(table)).as(table).isEmpty();
      assertThat(table(table).schema().findField("extra")).as(table).isNotNull();
      assertThat(countDeleteFiles(table, FileContent.EQUALITY_DELETES)).isZero();
    }
  }

  @Test
  void sameTableNameInTwoNamespacesKeepsSeparateIndexes() throws Exception {
    ((SupportsNamespaces) CATALOG_EXTENSION.catalog()).createNamespace(Namespace.of("other"));
    List<String> tables = ImmutableList.of("t_a", "other.t_a");
    List<List<Row>> batches = changelog(tables, 4, 60, 15, 30, 1);
    run(batches, INDEX_DV, "");

    Map<String, Map<Integer, String>> model = model(batches);
    for (String table : tables) {
      assertThat(liveRows(table)).as(table).containsExactlyInAnyOrderElementsOf(rows(model, table));
      assertThat(filesWithSeveralDvs(table)).as(table).isEmpty();
    }
  }

  @Test
  void tableRecreatedMidRunStartsAFreshIndex() throws Exception {
    List<List<Row>> before = changelog(ImmutableList.of("t_a"), 2, 40, 15, 31, 1);
    List<List<Row>> after = changelog(ImmutableList.of("t_a"), 2, 40, 15, 32, 1);
    JobClient job = startPausing(before, after, CATALOG_EXTENSION.catalogLoader());
    awaitCommitsSettled(CATALOG_EXTENSION.catalog(), identifier("t_a"));
    Set<String> oldDataFiles = dataFiles(table("t_a"));
    CATALOG_EXTENSION.catalog().dropTable(identifier("t_a"), true);
    V3_TABLES.createTable(
        CATALOG_EXTENSION.catalog(), identifier("t_a"), SCHEMA_V1, PartitionSpec.unpartitioned());
    resume(job);

    assertThat(liveRows("t_a")).containsExactlyInAnyOrderElementsOf(rows(model(after), "t_a"));
    assertThat(dvTargets(table("t_a"))).isNotEmpty().doesNotContainAnyElementsOf(oldDataFiles);
    assertThat(dataFiles(table("t_a"))).containsAll(dvTargets(table("t_a")));
  }

  @Test
  void renamedTableKeepsItsIndex() throws Exception {
    CatalogLoader jdbc =
        CatalogLoader.custom(
            "jdbc",
            ImmutableMap.of(
                CatalogProperties.URI,
                "jdbc:sqlite:" + jdbcDir.resolve("catalog.db"),
                CatalogProperties.WAREHOUSE_LOCATION,
                jdbcDir.resolve("warehouse").toString()),
            FlinkCatalogFactory.clusterHadoopConf(),
            JdbcCatalog.class.getName());
    Catalog catalog = jdbc.loadCatalog();
    ((SupportsNamespaces) catalog).createNamespace(Namespace.of(DATABASE));
    List<List<Row>> before = changelog(ImmutableList.of("r_a"), 2, 40, 15, 33, 1);
    List<List<Row>> after = changelog(ImmutableList.of("r_b"), 2, 40, 15, 34, 1);
    JobClient job = startPausing(before, after, jdbc);
    awaitCommitsSettled(catalog, identifier("r_a"));
    catalog.renameTable(identifier("r_a"), identifier("r_b"));
    resume(job);

    Map<String, Map<Integer, String>> model = model(before);
    model.put("r_b", model.remove("r_a"));
    apply(model, after);
    assertThat(liveRows(catalog.loadTable(identifier("r_b"))))
        .containsExactlyInAnyOrderElementsOf(rows(model, "r_b"));
    assertThat(DynamicIndexDvDataWriter.BOOTSTRAPPED_KEYS.get()).isZero();
  }

  @Test
  void keyFormatOneRestoreFailsLoudly() throws Exception {
    String savepoint = savepointInKeyFormatOne(changelog(TABLES, 2, 60, 15, 35, 1));

    List<List<Row>> second = changelog(TABLES, 1, 30, 15, 36, 1);
    StreamExecutionEnvironment env = restoredEnvironment(savepoint, PARALLELISM);
    sink(env.addSource(new BatchesSource(second, false), INPUT_TYPE).uid("source"), INDEX_DV);
    assertThatThrownBy(() -> env.execute("dynamic index-dv restore of key format 1"))
        .rootCause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("uses key format 1")
        .hasMessageContaining("upsert-stale-restore=rebuild");
  }

  @Test
  void keyFormatOneRestoreRebuildsFromTheHead() throws Exception {
    List<List<Row>> first = changelog(TABLES, 2, 60, 15, 37, 1);
    String savepoint = savepointInKeyFormatOne(first);

    List<List<Row>> second = changelog(TABLES, 2, 60, 15, 38, 1);
    StreamExecutionEnvironment env = restoredEnvironment(savepoint, PARALLELISM);
    sink(
        env.addSource(new BatchesSource(second, false), INPUT_TYPE).uid("source"),
        INDEX_DV,
        "",
        PARALLELISM,
        CATALOG_EXTENSION.catalogLoader(),
        ImmutableMap.of(FlinkWriteOptions.UPSERT_STALE_RESTORE.key(), "rebuild"));
    env.execute("dynamic index-dv rebuild of key format 1");

    Map<String, Map<Integer, String>> model = model(first);
    apply(model, second);
    for (String table : TABLES) {
      assertThat(liveRows(table)).as(table).containsExactlyInAnyOrderElementsOf(rows(model, table));
      assertThat(filesWithSeveralDvs(table)).as(table).isEmpty();
    }

    assertThat(DynamicIndexDvDataWriter.LEGACY_REBUILDS.get()).isPositive();
    assertThat(DynamicIndexDvDataWriter.BOOTSTRAPPED_KEYS.get()).isPositive();
  }

  @Test
  void tableIdCollisionFailsLoudly() {
    DynamicIndexDvKeys.setTestTableIds((uuid, branch) -> 42L);
    List<List<Row>> batches = changelog(ImmutableList.of("t_a", "t_b"), 1, 40, 15, 39, 1);
    assertThatThrownBy(() -> run(batches, INDEX_DV, ""))
        .rootCause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Table id collision")
        .hasMessageContaining("t_a")
        .hasMessageContaining("t_b");
  }

  // ---------------------------------------------------------------------------------------------

  /**
   * Writes a savepoint whose per-table metadata is in key format 1 and whose keys carry ids that
   * format 2 never derives, as the keys of a format 1 state never match format 2 keys.
   */
  private String savepointInKeyFormatOne(List<List<Row>> batches) throws Exception {
    DynamicIndexDvDataWriter.setWriteLegacyTableState(true);
    DynamicIndexDvKeys.setTestTableIds((uuid, branch) -> uuid.getLeastSignificantBits());
    try {
      return runUntilSavepoint(batches, PARALLELISM);
    } finally {
      DynamicIndexDvDataWriter.setWriteLegacyTableState(false);
      DynamicIndexDvKeys.setTestTableIds(null);
    }
  }

  /**
   * Starts a job that writes the first batches, waits until they are committed and returns while
   * the source pauses, so the caller can change the catalog before {@link #resume}.
   */
  private JobClient startPausing(
      List<List<Row>> first, List<List<Row>> second, CatalogLoader loader) throws Exception {
    List<List<Row>> batches = Lists.newArrayList(first);
    batches.addAll(second);
    StreamExecutionEnvironment env = environment(new Configuration(), PARALLELISM, false);
    sink(
        env.addSource(new BatchesSource(batches, false, first.size()), INPUT_TYPE).uid("source"),
        INDEX_DV,
        "",
        PARALLELISM,
        loader,
        ImmutableMap.of(FlinkDynamicSinkOptions.CACHE_REFRESH_MS.key(), "50"));
    JobClient job = env.executeAsync("dynamic index-dv with a catalog change");
    while (!BatchesSource.PAUSED.get()) {
      if (job.getJobStatus().get().isTerminalState()) {
        job.getJobExecutionResult().get();
        throw new IllegalStateException("Job ended before pausing");
      }

      Thread.sleep(20);
    }

    return job;
  }

  /** Waits until the commits of the batches before the pause have landed. */
  private static void awaitCommitsSettled(Catalog catalog, TableIdentifier identifier)
      throws InterruptedException {
    Long last = null;
    for (int stable = 0; stable < 5; ) {
      Thread.sleep(200);
      Table table = catalog.loadTable(identifier);
      Long current = table.currentSnapshot() != null ? table.currentSnapshot().snapshotId() : null;
      stable = current != null && current.equals(last) ? stable + 1 : 0;
      last = current;
    }
  }

  /** Lets every cached table UUID expire, then resumes the source and waits for the job. */
  private static void resume(JobClient job) throws Exception {
    Thread.sleep(500);
    BatchesSource.RESUME.set(true);
    job.getJobExecutionResult().get(120, TimeUnit.SECONDS);
  }

  private static Set<String> dataFiles(Table table) throws IOException {
    Set<String> paths = Sets.newHashSet();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        paths.add(task.file().location());
      }
    }

    return paths;
  }

  private static Set<String> dvTargets(Table table) throws IOException {
    Set<String> targets = Sets.newHashSet();
    for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile file : reader) {
          targets.add(file.referencedDataFile());
        }
      }
    }

    return targets;
  }

  private void run(List<List<Row>> batches, String mode, String suffix) throws Exception {
    run(batches, mode, suffix, PARALLELISM);
  }

  private void run(List<List<Row>> batches, String mode, String suffix, int parallelism)
      throws Exception {
    StreamExecutionEnvironment env = environment(new Configuration(), parallelism, false);
    sink(
        env.addSource(new BatchesSource(batches, false), INPUT_TYPE).uid("source"),
        mode,
        suffix,
        parallelism);
    env.execute("dynamic " + mode);
  }

  private String runUntilSavepoint(List<List<Row>> batches, int parallelism) throws Exception {
    BatchesSource.EMITTED_ALL.set(false);
    StreamExecutionEnvironment env = environment(new Configuration(), parallelism, false);
    sink(
        env.addSource(new BatchesSource(batches, true), INPUT_TYPE).uid("source"),
        INDEX_DV,
        "",
        parallelism);
    JobClient job = env.executeAsync("dynamic index-dv before savepoint");
    while (!BatchesSource.EMITTED_ALL.get()) {
      if (job.getJobStatus().get().isTerminalState()) {
        job.getJobExecutionResult().get();
        throw new IllegalStateException("Job ended before emitting all batches");
      }

      Thread.sleep(20);
    }

    return job.stopWithSavepoint(false, savepointDir.toString(), SavepointFormatType.CANONICAL)
        .get(60, TimeUnit.SECONDS);
  }

  private static StreamExecutionEnvironment restoredEnvironment(String savepoint, int parallelism) {
    Configuration restore = new Configuration();
    restore.set(StateRecoveryOptions.SAVEPOINT_PATH, savepoint);
    return environment(restore, parallelism, false);
  }

  static StreamExecutionEnvironment environment(
      Configuration extra, int parallelism, boolean restartOnce) {
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
        .setParallelism(parallelism)
        .setMaxParallelism(MAX_PARALLELISM);
  }

  static void sink(DataStream<Row> input, String mode, String suffix, int parallelism) {
    sink(input, mode, suffix, parallelism, CATALOG_EXTENSION.catalogLoader(), ImmutableMap.of());
  }

  private static void sink(DataStream<Row> input, String mode) {
    sink(input, mode, "", PARALLELISM);
  }

  static void sink(
      DataStream<Row> input,
      String mode,
      String suffix,
      int parallelism,
      CatalogLoader catalogLoader,
      Map<String, String> options) {
    DynamicIcebergSink.Builder<Row> builder =
        DynamicIcebergSink.forInput(input)
            .generator(new Generator(suffix))
            .catalogLoader(catalogLoader)
            .tableCreator(V3_TABLES)
            .immediateTableUpdate(true)
            .writeParallelism(parallelism)
            .uidPrefix("dyn")
            .set(FlinkWriteOptions.UPSERT_MODE.key(), mode);
    options.forEach(builder::set);
    builder.append();
  }

  /** A name with a namespace is used as is; a plain name is in {@code DATABASE}. */
  static TableIdentifier identifier(String name) {
    return name.contains(".") ? TableIdentifier.parse(name) : TableIdentifier.of(DATABASE, name);
  }

  /** Random upserts and deletes over a few keys per table, interleaved across tables. */
  static List<List<Row>> changelog(
      List<String> tables, int batches, int rowsPerBatch, int keys, long seed, int version) {
    Random random = new Random(seed);
    List<List<Row>> result = Lists.newArrayList();
    for (int batch = 0; batch < batches; batch++) {
      List<Row> rows = Lists.newArrayList();
      for (int i = 0; i < rowsPerBatch; i++) {
        String table = tables.get(random.nextInt(tables.size()));
        int id = random.nextInt(keys);
        RowKind kind =
            random.nextInt(5) == 0
                ? RowKind.DELETE
                : (random.nextBoolean() ? RowKind.INSERT : RowKind.UPDATE_AFTER);
        rows.add(Row.ofKind(kind, table, id, "s" + seed + "-b" + batch + "-" + i, version));
      }

      result.add(rows);
    }

    return result;
  }

  static Map<String, Map<Integer, String>> model(List<List<Row>> batches) {
    Map<String, Map<Integer, String>> model = Maps.newHashMap();
    apply(model, batches);
    return model;
  }

  static void apply(Map<String, Map<Integer, String>> model, List<List<Row>> batches) {
    for (List<Row> batch : batches) {
      for (Row row : batch) {
        Map<Integer, String> table =
            model.computeIfAbsent((String) row.getField(0), unused -> Maps.newHashMap());
        if (row.getKind() == RowKind.DELETE) {
          table.remove((Integer) row.getField(1));
        } else {
          table.put((Integer) row.getField(1), (String) row.getField(2));
        }
      }
    }
  }

  static List<String> rows(Map<String, Map<Integer, String>> model, String table) {
    List<String> rows = Lists.newArrayList();
    model.getOrDefault(table, Maps.newHashMap()).forEach((id, data) -> rows.add(id + ":" + data));
    return rows;
  }

  static Table table(String name) {
    return CATALOG_EXTENSION.catalog().loadTable(identifier(name));
  }

  static List<String> liveRows(String name) throws IOException {
    return liveRows(table(name));
  }

  static List<String> liveRows(Table table) throws IOException {
    List<String> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        rows.add(record.getField("id") + ":" + record.getField("data"));
      }
    }

    return rows;
  }

  /** Data files with more than one live DV at the head, which a reader cannot apply correctly. */
  static Map<String, Integer> filesWithSeveralDvs(String name) throws IOException {
    Table table = table(name);
    Map<String, Integer> dvsPerFile = Maps.newHashMap();
    if (table.currentSnapshot() == null) {
      return dvsPerFile;
    }

    for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile file : reader) {
          if (ContentFileUtil.isDV(file)) {
            dvsPerFile.merge(file.referencedDataFile(), 1, Integer::sum);
          }
        }
      }
    }

    dvsPerFile.values().removeIf(count -> count < 2);
    return dvsPerFile;
  }

  static int countDeleteFiles(String name, FileContent content) throws IOException {
    Table table = table(name);
    if (table.currentSnapshot() == null) {
      return 0;
    }

    int count = 0;
    for (ManifestFile manifest : table.currentSnapshot().deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile file : reader) {
          if (file.content() == content) {
            count++;
          }
        }
      }
    }

    return count;
  }

  /** Turns (table, id, data, schema version) rows into dynamic upsert records. */
  static class Generator implements DynamicRecordGenerator<Row> {
    private final String suffix;

    Generator(String suffix) {
      this.suffix = suffix;
    }

    @Override
    public void generate(Row row, Collector<DynamicRecord> out) {
      int version = (Integer) row.getField(3);
      Integer id = (Integer) row.getField(1);
      StringData data = StringData.fromString((String) row.getField(2));
      RowData rowData =
          version == 2
              ? GenericRowData.ofKind(
                  row.getKind(), id, data, StringData.fromString("extra-" + row.getField(2)))
              : GenericRowData.ofKind(row.getKind(), id, data);
      DynamicRecord record =
          new DynamicRecord(
              identifier(row.getField(0) + suffix),
              SnapshotRef.MAIN_BRANCH,
              version == 2 ? SCHEMA_V2 : SCHEMA_V1,
              rowData,
              PartitionSpec.unpartitioned(),
              DistributionMode.NONE,
              PARALLELISM * 2);
      record.setUpsertMode(true);
      record.setEqualityFields(ImmutableSet.of("id"));
      out.collect(record);
    }
  }

  /** Fails the job once, after the given number of records, to force a restore. */
  static class FailOnce implements MapFunction<Row, Row> {
    static final AtomicInteger SEEN = new AtomicInteger();
    static final AtomicBoolean FAILED = new AtomicBoolean();
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

  /**
   * Emits each batch after two completed checkpoints, then ends or idles until stopped. With a
   * pause, it waits for {@link #RESUME} before the batch at that index.
   */
  static class BatchesSource implements SourceFunction<Row>, CheckpointListener {
    static final AtomicBoolean EMITTED_ALL = new AtomicBoolean();
    static final AtomicBoolean PAUSED = new AtomicBoolean();
    static final AtomicBoolean RESUME = new AtomicBoolean();

    private final List<List<Row>> batches;
    private final boolean idleAtEnd;
    private final int pauseBefore;
    private final AtomicInteger completedCheckpoints = new AtomicInteger();
    private volatile boolean running = true;

    BatchesSource(List<List<Row>> batches, boolean idleAtEnd) {
      this(batches, idleAtEnd, -1);
    }

    BatchesSource(List<List<Row>> batches, boolean idleAtEnd, int pauseBefore) {
      this.batches = batches;
      this.idleAtEnd = idleAtEnd;
      this.pauseBefore = pauseBefore;
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
      awaitCheckpoints(1);
      for (int index = 0; index < batches.size(); index++) {
        List<Row> batch = batches.get(index);
        if (index == pauseBefore) {
          PAUSED.set(true);
          while (running && !RESUME.get()) {
            Thread.sleep(10);
          }
        }

        synchronized (ctx.getCheckpointLock()) {
          batch.forEach(ctx::collect);
        }

        awaitCheckpoints(2);
      }

      EMITTED_ALL.set(true);
      while (running && idleAtEnd) {
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
}
