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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.iceberg.Accessor;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.iceberg.util.StructProjection;

/**
 * POC: maps each equality key owned by one writer subtask to the data file and position of its live
 * row, and tracks the latest deletion vector of every data file it has deleted from.
 */
class KeyPositionIndex {
  private static final int FORMAT_VERSION = 1;

  record FileRef(String path, int specId, StructLike partition) {}

  record Position(FileRef file, long pos) {}

  private final Map<SerializedEqualityValues, Position> positions = Maps.newHashMap();
  private final Map<String, DeleteFile> dvs = Maps.newHashMap();

  Position remove(SerializedEqualityValues key) {
    return positions.remove(key);
  }

  void put(SerializedEqualityValues key, FileRef file, long pos) {
    positions.put(key, new Position(file, pos));
  }

  DeleteFile dv(String dataFilePath) {
    return dvs.get(dataFilePath);
  }

  void updateDv(DeleteFile dv) {
    dvs.put(dv.referencedDataFile(), dv);
  }

  int size() {
    return positions.size();
  }

  /**
   * Indexes the live rows of the branch head whose key passes {@code owns}.
   *
   * <p>shortcut: full scan of every data file on each subtask, upgrade to the incremental catch-up
   * of #18717 or to a shared index extract (#16961).
   */
  static KeyPositionIndex bootstrap(
      Table table,
      String branch,
      Schema keySchema,
      Predicate<StructLike> owns,
      StructLikeSerializer serializer)
      throws IOException {
    KeyPositionIndex index = new KeyPositionIndex();
    if (table.snapshot(branch) == null) {
      return index;
    }

    List<Types.NestedField> columns = Lists.newArrayList(keySchema.columns());
    columns.add(MetadataColumns.ROW_POSITION);
    Schema requested = new Schema(columns);
    Types.StructType keyType = keySchema.asStruct();
    InternalRecordWrapper keyWrapper = new InternalRecordWrapper(keyType);

    try (CloseableIterable<FileScanTask> tasks = table.newScan().useRef(branch).planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile file = task.file();
        for (DeleteFile delete : task.deletes()) {
          if (ContentFileUtil.isDV(delete)) {
            index.dvs.put(file.location(), delete.copy());
          } else if (delete.content() == FileContent.POSITION_DELETES) {
            // shortcut: a V2 position delete file would have to be merged into the new DV
            throw new UnsupportedOperationException(
                "POC: index-dv upsert mode does not support V2 position delete files: "
                    + delete.location());
          }
        }

        FileRef ref = new FileRef(file.location(), file.specId(), file.partition());
        GenericDeleteFilter filter =
            new GenericDeleteFilter(table.io(), task, table.schema(), requested);
        Schema readSchema = filter.requiredSchema();
        StructProjection keyProjection = StructProjection.create(readSchema, keySchema);
        Accessor<StructLike> posAccessor =
            readSchema.accessorForField(MetadataColumns.ROW_POSITION.fieldId());
        CloseableIterable<Record> records =
            FormatModelRegistry.<Record, Schema>readBuilder(
                    file.format(), Record.class, table.io().newInputFile(file.location()))
                .project(readSchema)
                .build();
        try (CloseableIterable<Record> live = filter.filter(records)) {
          for (Record record : live) {
            StructLike key = keyWrapper.wrap(keyProjection.wrap(record));
            if (owns.test(key)) {
              index.positions.put(
                  serializer.serializeKey(key, keyType),
                  new Position(ref, (long) posAccessor.get(record)));
            }
          }
        }
      }
    }

    return index;
  }

  /** shortcut: full snapshot of the index on every checkpoint, upgrade to keyed/RocksDB state. */
  byte[] serialize(Map<Integer, PartitionSpec> specs, StructLikeSerializer serializer) {
    Map<FileRef, Integer> fileIds = Maps.newHashMap();
    List<FileRef> files = Lists.newArrayList();
    for (Position position : positions.values()) {
      if (!fileIds.containsKey(position.file())) {
        fileIds.put(position.file(), files.size());
        files.add(position.file());
      }
    }

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(FORMAT_VERSION);
      out.writeInt(files.size());
      for (FileRef file : files) {
        out.writeUTF(file.path());
        out.writeInt(file.specId());
        writeBytes(
            out,
            serializer.encodePartition(file.partition(), specs.get(file.specId()).partitionType()));
      }

      out.writeInt(positions.size());
      for (Map.Entry<SerializedEqualityValues, Position> entry : positions.entrySet()) {
        writeBytes(out, entry.getKey().data());
        out.writeInt(fileIds.get(entry.getValue().file()));
        out.writeLong(entry.getValue().pos());
      }

      out.writeInt(dvs.size());
      for (DeleteFile dv : dvs.values()) {
        writeBytes(out, SerializationUtil.serializeToBytes(dv));
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize key position index", e);
    }

    return bytes.toByteArray();
  }

  static KeyPositionIndex deserialize(byte[] data, Map<Integer, PartitionSpec> specs) {
    KeyPositionIndex index = new KeyPositionIndex();
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
      int version = in.readInt();
      if (version != FORMAT_VERSION) {
        throw new IllegalStateException("Unsupported key position index version: " + version);
      }

      int fileCount = in.readInt();
      List<FileRef> files = Lists.newArrayListWithCapacity(fileCount);
      for (int i = 0; i < fileCount; i++) {
        String path = in.readUTF();
        int specId = in.readInt();
        StructLike partition =
            StructLikeSerializer.decodePartition(readBytes(in), specs.get(specId).partitionType());
        files.add(new FileRef(path, specId, partition));
      }

      int entryCount = in.readInt();
      for (int i = 0; i < entryCount; i++) {
        SerializedEqualityValues key = new SerializedEqualityValues(readBytes(in));
        FileRef file = files.get(in.readInt());
        index.positions.put(key, new Position(file, in.readLong()));
      }

      int dvCount = in.readInt();
      for (int i = 0; i < dvCount; i++) {
        DeleteFile dv = SerializationUtil.deserializeFromBytes(readBytes(in));
        index.dvs.put(dv.referencedDataFile(), dv);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to deserialize key position index", e);
    }

    return index;
  }

  private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
    out.writeInt(value.length);
    out.write(value);
  }

  private static byte[] readBytes(DataInputStream in) throws IOException {
    byte[] value = new byte[in.readInt()];
    in.readFully(value);
    return value;
  }
}
