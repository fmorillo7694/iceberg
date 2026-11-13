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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SerializationUtil;

/**
 * POC: the DV writer of the dynamic sink's index-dv mode, keyed by data file. Each data file gets
 * one DV per checkpoint, merged with the last DV written for it.
 *
 * <p>shortcut: the last DV is not loaded from the table for a file this writer has not written a DV
 * for yet; the committer merges every DV with its file's live DV instead.
 */
class DynamicIndexDvDeleteWriter extends AbstractStreamOperator<byte[]>
    implements OneInputStreamOperator<DynamicDvPosition, byte[]>, BoundedOneInput {
  private final CatalogLoader catalogLoader;

  private transient Catalog catalog;
  private transient ValueState<byte[]> lastDv;
  private transient Map<TableKey, TableDvs> tables;
  private transient boolean inputEnded;

  DynamicIndexDvDeleteWriter(CatalogLoader catalogLoader) {
    this.catalogLoader = catalogLoader;
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    this.catalog = catalogLoader.loadCatalog();
    this.tables = Maps.newHashMap();
    this.lastDv =
        context
            .getKeyedStateStore()
            .getState(
                new ValueStateDescriptor<>(
                    "dynamic-index-dv-last-dv",
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
  }

  @Override
  public void processElement(StreamRecord<DynamicDvPosition> element) {
    DynamicDvPosition position = element.getValue();
    TableDvs table =
        tables.computeIfAbsent(
            new TableKey(position.tableName(), position.branch()),
            key -> new TableDvs(catalog.loadTable(TableIdentifier.parse(key.tableName()))));
    PartitionSpec spec = table.table.specs().get(position.specId());
    StructLike partition =
        table.partitions.computeIfAbsent(
            position.dataFilePath(),
            path ->
                StructLikeSerializer.decodePartition(position.partition(), spec.partitionType()));
    if (table.writer == null) {
      table.writer = new BaseDVFileWriter(table.fileFactory, this::loadPreviousDeletes);
    }

    table.writer.delete(position.dataFilePath(), position.position(), spec, partition);
  }

  private PositionDeleteIndex loadPreviousDeletes(String dataFilePath) {
    getKeyedStateBackend().setCurrentKey(dataFilePath);
    try {
      byte[] previous = lastDv.value();
      if (previous == null) {
        return null;
      }

      DeleteFile dv = SerializationUtil.deserializeFromBytes(previous);
      TableDvs table = tableOf(dataFilePath);
      return table.deleteLoader.loadPositionDeletes(ImmutableList.of(dv), dataFilePath);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private TableDvs tableOf(String dataFilePath) {
    for (TableDvs table : tables.values()) {
      if (table.partitions.containsKey(dataFilePath)) {
        return table;
      }
    }

    throw new IllegalStateException("POC: no table for data file " + dataFilePath);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    super.prepareSnapshotPreBarrier(checkpointId);
    flush();
  }

  @Override
  public void endInput() throws IOException {
    flush();
    this.inputEnded = true;
  }

  private void flush() throws IOException {
    if (inputEnded) {
      return;
    }

    for (Map.Entry<TableKey, TableDvs> entry : tables.entrySet()) {
      TableDvs table = entry.getValue();
      if (table.writer == null) {
        continue;
      }

      table.writer.close();
      DeleteWriteResult dvs = table.writer.result();
      Map<Integer, List<DeleteFile>> bySpec = Maps.newHashMap();
      for (DeleteFile dv : dvs.deleteFiles()) {
        getKeyedStateBackend().setCurrentKey(dv.referencedDataFile());
        lastDv.update(SerializationUtil.serializeToBytes(dv));
        bySpec.computeIfAbsent(dv.specId(), id -> Lists.newArrayList()).add(dv);
      }

      for (Map.Entry<Integer, List<DeleteFile>> spec : bySpec.entrySet()) {
        WriteResult.Builder result = WriteResult.builder().addDeleteFiles(spec.getValue());
        for (DeleteFile dv : spec.getValue()) {
          result.addReferencedDataFiles(dv.referencedDataFile());
        }

        output.collect(
            new StreamRecord<>(
                DynamicIndexDvDataWriter.encode(
                    new DynamicWriteResult(entry.getKey(), spec.getKey(), result.build()))));
      }

      table.writer = null;
      table.partitions.clear();
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (tables != null) {
      for (TableDvs table : tables.values()) {
        if (table.writer != null) {
          table.writer.close();
        }
      }
    }
  }

  /** The DV writer of one table for the current checkpoint. */
  private final class TableDvs {
    private final Table table;
    private final OutputFileFactory fileFactory;
    private final DeleteLoader deleteLoader;
    private final Map<String, StructLike> partitions = Maps.newHashMap();
    private BaseDVFileWriter writer;

    private TableDvs(Table table) {
      this.table = table;
      this.fileFactory =
          OutputFileFactory.builderFor(
                  table,
                  getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                  getRuntimeContext().getTaskInfo().getAttemptNumber())
              .format(FileFormat.PUFFIN)
              .build();
      this.deleteLoader = new BaseDeleteLoader(file -> table.io().newInputFile(file));
    }
  }
}
