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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

/**
 * POC: the keyed-state key of the dynamic index-dv mode, a (table, branch, equality key) triple.
 */
final class DynamicIndexDvKeys {
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

  static SerializedEqualityValues compose(TableKey table, SerializedEqualityValues key) {
    return compose(prefix(table.tableName(), table.branch()), key);
  }

  /** The encoded (table, branch) part of a key, to be reused for every key of that pair. */
  static byte[] prefix(String tableName, String branch) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeUTF(tableName);
      out.writeUTF(branch);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return bytes.toByteArray();
  }

  static SerializedEqualityValues compose(byte[] prefix, SerializedEqualityValues key) {
    byte[] data = key.data();
    byte[] composed = Arrays.copyOf(prefix, prefix.length + data.length);
    System.arraycopy(data, 0, composed, prefix.length, data.length);
    return new SerializedEqualityValues(composed);
  }
}
