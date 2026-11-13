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

import java.util.Map;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.StructProjection;

/** POC: keys upsert records of the dynamic sink by (table, branch, equality key). */
class DynamicIndexDvKeySelector
    implements KeySelector<DynamicRecordInternal, SerializedEqualityValues> {
  // shortcut: unbounded per (table, schema, equality fields); upgrade to an LRU for many tables
  private transient Map<String, Extractor> extractors;
  private transient StructLikeSerializer serializer;

  @Override
  public SerializedEqualityValues getKey(DynamicRecordInternal record) {
    if (extractors == null) {
      this.extractors = Maps.newHashMap();
      this.serializer = new StructLikeSerializer();
    }

    Extractor extractor =
        extractors.computeIfAbsent(
            record.tableName() + "/" + record.schema().schemaId() + "/" + record.equalityFields(),
            unused ->
                new Extractor(
                    record.schema(),
                    DynamicIndexDvKeys.keySchema(record.schema(), record.equalityFields())));
    SerializedEqualityValues key =
        serializer.serializeKey(
            extractor.projection.wrap(extractor.wrapper.wrap(record.rowData())),
            extractor.keySchema.asStruct());
    return DynamicIndexDvKeys.compose(new TableKey(record.tableName(), record.branch()), key);
  }

  private static final class Extractor {
    private final RowDataWrapper wrapper;
    private final StructProjection projection;
    private final Schema keySchema;

    private Extractor(Schema schema, Schema keySchema) {
      this.wrapper = new RowDataWrapper(FlinkSchemaUtil.convert(schema), schema.asStruct());
      this.projection = StructProjection.create(schema, keySchema);
      this.keySchema = keySchema;
    }
  }
}
