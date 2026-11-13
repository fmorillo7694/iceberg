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

import java.util.Set;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;

/** POC: keys rows by the serialized values of their equality fields, not by a hash of them. */
class IndexDvKeySelector implements KeySelector<RowData, SerializedEqualityValues> {
  // POC: a test-only prefix on every key, to compare with the dynamic sink's key encoding.
  private static volatile byte[] testKeyPrefix = null;

  private final Schema schema;
  private final RowType flinkRowType;
  private final Schema keySchema;

  private transient RowDataWrapper wrapper;
  private transient StructProjection projection;
  private transient StructLikeSerializer serializer;

  IndexDvKeySelector(Schema schema, RowType flinkRowType, Set<Integer> equalityFieldIds) {
    this.schema = schema;
    this.flinkRowType = flinkRowType;
    this.keySchema = TypeUtil.select(schema, equalityFieldIds);
  }

  @Override
  public SerializedEqualityValues getKey(RowData row) {
    if (wrapper == null) {
      this.wrapper = new RowDataWrapper(flinkRowType, schema.asStruct());
      this.projection = StructProjection.create(schema, keySchema);
      this.serializer = new StructLikeSerializer();
    }

    Types.StructType keyType = keySchema.asStruct();
    return prefixed(serializer.serializeKey(projection.wrap(wrapper.wrap(row)), keyType));
  }

  @VisibleForTesting
  static void setTestKeyPrefix(byte[] prefix) {
    testKeyPrefix = prefix;
  }

  static SerializedEqualityValues prefixed(SerializedEqualityValues key) {
    byte[] prefix = testKeyPrefix;
    if (prefix == null) {
      return key;
    }

    byte[] data = new byte[prefix.length + key.data().length];
    System.arraycopy(prefix, 0, data, 0, prefix.length);
    System.arraycopy(key.data(), 0, data, prefix.length, key.data().length);
    return new SerializedEqualityValues(data);
  }
}
