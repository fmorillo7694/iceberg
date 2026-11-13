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

import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * POC: operators of the index-dv upsert mode. Rows are keyed by their equality key, so the operator
 * that owns a key's location also writes its new row; retired positions are keyed by data file and
 * written as deletion vectors. Both emit their files from the pre-barrier hook, so with aligned
 * checkpoints the files of a checkpoint reach the commit of that same checkpoint.
 */
class IndexDvTopology {
  private static final Logger LOG = LoggerFactory.getLogger(IndexDvTopology.class);

  private IndexDvTopology() {}

  static void validate(
      FlinkWriteConf conf,
      Table table,
      Set<Integer> equalityFieldIds,
      DataStream<?> input,
      int writerParallelism) {
    Preconditions.checkState(
        conf.upsertMode(), "POC: upsert-mode 'index-dv' requires upsert enabled");
    Preconditions.checkState(
        !conf.overwriteMode(), "Upsert mode 'index-dv' can't be used with overwrite");
    Preconditions.checkState(
        !equalityFieldIds.isEmpty(), "Upsert mode 'index-dv' requires equality field columns");
    Preconditions.checkState(
        TableUtil.formatVersion(table) >= 3,
        "Upsert mode 'index-dv' requires format version 3 or later, table %s is version %s",
        table.name(),
        TableUtil.formatVersion(table));
    // Unaligned barriers overtake in-flight results, so a DV could commit before its data file.
    Preconditions.checkState(
        !input.getExecutionEnvironment().getCheckpointConfig().isUnalignedCheckpointsEnabled(),
        "Upsert mode 'index-dv' requires aligned checkpoints");

    DistributionMode mode = conf.distributionMode();
    if (mode == DistributionMode.HASH && table.spec().isPartitioned()) {
      LOG.warn(
          "Upsert mode 'index-dv' distributes rows by equality fields, not by partition: each of "
              + "the {} writers can write a file for every partition in each checkpoint",
          writerParallelism);
    } else if (mode == DistributionMode.RANGE) {
      LOG.warn("Upsert mode 'index-dv' distributes rows by equality fields, not by range");
    }
  }

  static Long bootstrapSnapshotId(Table table, String branch) {
    Snapshot head = table.snapshot(branch);
    return head != null ? head.snapshotId() : null;
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  static DataStream<FlinkWriteResult> append(
      DataStream<RowData> input,
      Table table,
      TableLoader tableLoader,
      FlinkWriteConf conf,
      RowType flinkRowType,
      Set<Integer> equalityFieldIds,
      Map<String, String> writeProperties,
      Long bootstrapSnapshotId,
      int writerParallelism,
      String name,
      String uidPrefix) {
    SingleOutputStreamOperator<FlinkWriteResult> dataStream =
        input
            .keyBy(
                new IndexDvKeySelector(table.schema(), flinkRowType, equalityFieldIds),
                TypeInformation.of(SerializedEqualityValues.class))
            .transform(
                name + "-index-dv-writer",
                TypeInformation.of(FlinkWriteResult.class),
                new IndexDvDataWriter(
                    tableLoader,
                    conf.branch(),
                    flinkRowType,
                    equalityFieldIds,
                    conf.dataFileFormat(),
                    conf.targetDataFileSize(),
                    writeProperties,
                    bootstrapSnapshotId,
                    conf.upsertRebuildOnStaleRestore()))
            .setParallelism(writerParallelism);
    SingleOutputStreamOperator<FlinkWriteResult> dvStream =
        dataStream
            .getSideOutput(IndexDvDataWriter.DELETES)
            .keyBy(DVPosition::dataFilePath)
            .transform(
                name + "-index-dv-delete-writer",
                TypeInformation.of(FlinkWriteResult.class),
                new IndexDvDeleteWriter(
                    tableLoader,
                    conf.branch(),
                    bootstrapSnapshotId,
                    conf.upsertRebuildOnStaleRestore()))
            .setParallelism(writerParallelism);
    if (uidPrefix != null) {
      dataStream = dataStream.uid(uidPrefix + "-index-dv-writer");
      dvStream = dvStream.uid(uidPrefix + "-index-dv-delete-writer");
    }

    return dataStream.union(dvStream);
  }
}
