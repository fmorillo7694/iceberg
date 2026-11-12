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
import java.util.Random;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
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
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * POC: rough local comparison of the equality-delete and index-dv upsert modes. Runs only when the
 * environment variable {@code INDEX_DV_BENCH} is set.
 */
@Timeout(value = 1200)
class IndexDvUpsertBenchmark {
  @RegisterExtension
  static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @RegisterExtension
  static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  private static final int ROWS = 200_000;
  private static final int LOAD_BATCHES = 5;
  private static final int UPDATES = ROWS / 5;
  private static final int UPDATE_BATCHES = 4;
  private static final int PARALLELISM = 2;
  private static final int ROUNDS = 2;

  @Test
  void compareUpsertModes() throws Exception {
    assumeThat(System.getenv("INDEX_DV_BENCH")).isNotNull();
    for (int round = 1; round <= ROUNDS; round++) {
      for (String mode : ImmutableList.of("equality-delete", "index-dv")) {
        runRound(round, mode);
      }
    }
  }

  private void runRound(int round, String mode) throws Exception {
    String name = "bench_" + mode.replace('-', '_') + "_" + round;
    Table table =
        CATALOG_EXTENSION
            .catalog()
            .createTable(
                TableIdentifier.of(DATABASE, name),
                SimpleDataUtil.SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));

    long loadMs = runJob(name, mode, loadBatches());
    long updateMs = runJob(name, mode, updateBatches(round));
    table.refresh();

    long planStart = System.nanoTime();
    int dataFiles = 0;
    int eqDeletes = 0;
    int dvs = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        dataFiles++;
        eqDeletes +=
            (int)
                task.deletes().stream()
                    .filter(d -> d.content() == FileContent.EQUALITY_DELETES)
                    .count();
        dvs +=
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

    assertThat(rows).isEqualTo(ROWS);
    System.out.printf(
        "BENCH round=%d mode=%s loadMs=%d updateMs=%d planMs=%d scanMs=%d rows=%d dataFiles=%d "
            + "eqDeleteRefs=%d dvRefs=%d%n",
        round, mode, loadMs, updateMs, planMs, scanMs, rows, dataFiles, eqDeletes, dvs);
  }

  private long runJob(String tableName, String mode, List<List<Row>> batches) throws Exception {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(200L)
            .setParallelism(PARALLELISM)
            .setMaxParallelism(16);
    DataStream<Row> input =
        env.addSource(new BoundedTestSource<>(batches), TestFlinkIcebergSinkV2Base.ROW_TYPE_INFO);
    IcebergSink.forRow(input, SimpleDataUtil.FLINK_SCHEMA)
        .tableLoader(
            TableLoader.fromCatalog(
                CATALOG_EXTENSION.catalogLoader(), TableIdentifier.of(DATABASE, tableName)))
        .writeParallelism(PARALLELISM)
        .equalityFieldColumns(ImmutableList.of("id"))
        .upsert(true)
        .set(FlinkWriteOptions.UPSERT_MODE.key(), mode)
        .append();

    long start = System.nanoTime();
    env.execute("bench " + mode);
    return (System.nanoTime() - start) / 1_000_000;
  }

  private static List<List<Row>> loadBatches() {
    List<List<Row>> batches = Lists.newArrayList();
    int perBatch = ROWS / LOAD_BATCHES;
    for (int b = 0; b < LOAD_BATCHES; b++) {
      List<Row> batch = Lists.newArrayListWithCapacity(perBatch);
      for (int i = b * perBatch; i < (b + 1) * perBatch; i++) {
        batch.add(Row.ofKind(RowKind.INSERT, i, "initial-" + i));
      }

      batches.add(batch);
    }

    return batches;
  }

  private static List<List<Row>> updateBatches(int seed) {
    Random random = new Random(seed);
    List<List<Row>> batches = Lists.newArrayList();
    int perBatch = UPDATES / UPDATE_BATCHES;
    for (int b = 0; b < UPDATE_BATCHES; b++) {
      List<Row> batch = Lists.newArrayListWithCapacity(perBatch);
      for (int i = 0; i < perBatch; i++) {
        int id = random.nextInt(ROWS);
        batch.add(Row.ofKind(RowKind.UPDATE_AFTER, id, "updated-" + b + "-" + i));
      }

      batches.add(batch);
    }

    return batches;
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
}
