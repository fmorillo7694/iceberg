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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.ToLongBiFunction;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.hash.Hashing;
import org.apache.iceberg.types.Types;

/**
 * POC: the keyed-state key of the dynamic index-dv mode, a (table id, equality key) pair. The table
 * id is a 64-bit hash of the table UUID and the branch, so every subtask and every run derives the
 * same id without coordination; the writer detects a collision of two tables.
 */
final class DynamicIndexDvKeys {
  /** Format 1 prefixed keys with the table name and branch; format 2 with the 8-byte table id. */
  static final int KEY_FORMAT_VERSION = 2;

  static final int TABLE_ID_BYTES = Long.BYTES;

  private static volatile ToLongBiFunction<UUID, String> testTableIds = null;

  private DynamicIndexDvKeys() {}

  /**
   * The equality columns in field-id order, so the encoded key stays the same when other columns
   * are added, dropped or reordered.
   */
  static Schema keySchema(Schema schema, Set<Integer> equalityFieldIds) {
    List<Types.NestedField> fields = Lists.newArrayList();
    for (int id : equalityFieldIds) {
      Types.NestedField field = schema.findField(id);
      Preconditions.checkArgument(field != null, "Equality field %s is not in the schema", id);
      fields.add(field);
    }

    fields.sort(Comparator.comparingInt(Types.NestedField::fieldId));
    return new Schema(fields);
  }

  static long tableId(UUID tableUuid, String branch) {
    ToLongBiFunction<UUID, String> override = testTableIds;
    if (override != null) {
      return override.applyAsLong(tableUuid, branch);
    }

    return Hashing.murmur3_128()
        .newHasher()
        .putLong(tableUuid.getMostSignificantBits())
        .putLong(tableUuid.getLeastSignificantBits())
        .putString(branch, StandardCharsets.UTF_8)
        .hash()
        .asLong();
  }

  @VisibleForTesting
  static void setTestTableIds(ToLongBiFunction<UUID, String> tableIds) {
    testTableIds = tableIds;
  }

  static byte[] prefix(long tableId) {
    byte[] prefix = new byte[TABLE_ID_BYTES];
    for (int i = 0; i < TABLE_ID_BYTES; i++) {
      prefix[i] = (byte) (tableId >>> (Byte.SIZE * (TABLE_ID_BYTES - 1 - i)));
    }

    return prefix;
  }

  static long tableId(SerializedEqualityValues key) {
    byte[] data = key.data();
    long id = 0;
    for (int i = 0; i < TABLE_ID_BYTES; i++) {
      id = (id << Byte.SIZE) | (data[i] & 0xFF);
    }

    return id;
  }

  static SerializedEqualityValues compose(byte[] prefix, SerializedEqualityValues key) {
    byte[] data = key.data();
    byte[] composed = Arrays.copyOf(prefix, prefix.length + data.length);
    System.arraycopy(data, 0, composed, prefix.length, data.length);
    return new SerializedEqualityValues(composed);
  }
}
