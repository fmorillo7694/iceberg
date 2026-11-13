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
import java.util.Set;
import java.util.UUID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.StructProjection;

/**
 * POC: attaches the keyed-state key, (table id, equality key), to each upsert record before the
 * key-by. The key travels with the record, so the writer uses exactly the key the record was
 * partitioned by. The table UUID behind the id is re-read from the catalog after {@code refreshMs},
 * so a dropped and re-created table gets a new id.
 */
class DynamicIndexDvKeyAssigner
    extends RichMapFunction<
        DynamicRecordInternal, Tuple2<SerializedEqualityValues, DynamicRecordInternal>> {
  private final CatalogLoader catalogLoader;
  private final long refreshMs;

  // shortcut: unbounded per (table, schema, branch); upgrade to an LRU for many tables
  private transient Map<String, TableEntry> tables;
  private transient StructLikeSerializer serializer;
  private transient Catalog catalog;

  DynamicIndexDvKeyAssigner(CatalogLoader catalogLoader, long refreshMs) {
    this.catalogLoader = catalogLoader;
    this.refreshMs = refreshMs;
  }

  @Override
  public void open(OpenContext openContext) {
    this.tables = Maps.newHashMap();
    this.serializer = new StructLikeSerializer();
    this.catalog = catalogLoader.loadCatalog();
  }

  @Override
  public Tuple2<SerializedEqualityValues, DynamicRecordInternal> map(DynamicRecordInternal record) {
    TableEntry table = tables.computeIfAbsent(record.tableName(), unused -> new TableEntry());
    long now = System.currentTimeMillis();
    if (now >= table.expiresAtMs) {
      table.refresh(catalog.loadTable(TableIdentifier.parse(record.tableName())).uuid());
      table.expiresAtMs = now + refreshMs;
    }

    Extractor extractor = table.extractor(record.schema(), record.equalityFields());
    SerializedEqualityValues key =
        serializer.serializeKey(
            extractor.projection.wrap(extractor.wrapper.wrap(record.rowData())),
            extractor.keySchema.asStruct());
    return Tuple2.of(DynamicIndexDvKeys.compose(table.prefix(record.branch()), key), record);
  }

  private static final class TableEntry {
    private final Map<Integer, Extractor> extractors = Maps.newHashMap();
    private final Map<String, byte[]> prefixes = Maps.newHashMap();
    private UUID uuid;
    private long expiresAtMs = Long.MIN_VALUE;

    private void refresh(UUID current) {
      if (!current.equals(uuid)) {
        this.uuid = current;
        extractors.clear();
        prefixes.clear();
      }
    }

    private Extractor extractor(Schema schema, Set<Integer> equalityFields) {
      Extractor extractor = extractors.get(schema.schemaId());
      if (extractor == null || !extractor.equalityFields.equals(equalityFields)) {
        extractor = new Extractor(schema, equalityFields);
        extractors.put(schema.schemaId(), extractor);
      }

      return extractor;
    }

    private byte[] prefix(String branch) {
      return prefixes.computeIfAbsent(
          branch, name -> DynamicIndexDvKeys.prefix(DynamicIndexDvKeys.tableId(uuid, name)));
    }
  }

  private static final class Extractor {
    private final Set<Integer> equalityFields;
    private final RowDataWrapper wrapper;
    private final StructProjection projection;
    private final Schema keySchema;

    private Extractor(Schema schema, Set<Integer> equalityFields) {
      this.equalityFields = ImmutableSet.copyOf(equalityFields);
      this.keySchema = DynamicIndexDvKeys.keySchema(schema, equalityFields);
      this.wrapper = new RowDataWrapper(FlinkSchemaUtil.convert(schema), schema.asStruct());
      this.projection = StructProjection.create(schema, keySchema);
    }
  }
}
