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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkWriteOptions;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.TestFixtures;
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

/** POC tests for the index-backed DV upsert mode of {@link IcebergSink}. */
@ExtendWith(ParameterizedTestExtension.class)
@Timeout(value = 120)
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

  @Parameters(name = "parallelism={0}, partitioned={1}")
  static Object[][] parameters() {
    return new Object[][] {{1, false}, {2, false}, {2, true}};
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
    return CATALOG_EXTENSION
        .catalog()
        .createTable(
            TableIdentifier.of(DATABASE, name),
            SimpleDataUtil.SCHEMA,
            spec,
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
  }

  private void runJob(Table table, String upsertMode, List<List<Row>> batches, FailOnce failer)
      throws Exception {
    Configuration config =
        new Configuration(MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG);
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(config)
            .enableCheckpointing(100L)
            .setParallelism(parallelism)
            .setMaxParallelism(MAX_PARALLELISM);

    DataStream<Row> input =
        env.addSource(new BoundedTestSource<>(batches), TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO);
    if (failer != null) {
      input = input.map(failer, TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO).setParallelism(1);
    }

    TableLoader tableLoader =
        TableLoader.fromCatalog(
            CATALOG_EXTENSION.catalogLoader(), TableIdentifier.of(DATABASE, tableName(table)));
    IcebergSink.forRow(input, SimpleDataUtil.FLINK_SCHEMA)
        .tableLoader(tableLoader)
        .writeParallelism(parallelism)
        .equalityFieldColumns(ImmutableList.of("id"))
        .upsert(true)
        .set(FlinkWriteOptions.UPSERT_MODE.key(), upsertMode)
        .append();

    env.execute("index-dv test " + upsertMode);
    table.refresh();
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
        String data = "v" + c + "_" + i;
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
}
