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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Set;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.SnapshotUtil;

/** POC: commit-side support for the index-dv upsert mode. */
class IndexDvCommits {
  private IndexDvCommits() {}

  /**
   * Prepares a row delta whose deletion vectors delete rows of data files committed earlier: it
   * replaces their live DVs and validates against commits by others since this sink's last commit,
   * or since the snapshot the writers bootstrapped from.
   */
  static void prepare(
      Table table,
      String branch,
      RowDelta rowDelta,
      WriteResult result,
      String operatorId,
      Long bootstrapSnapshotId) {
    for (DeleteFile deleteFile : result.deleteFiles()) {
      Preconditions.checkState(
          ContentFileUtil.isDV(deleteFile),
          "Upsert mode 'index-dv' must commit only deletion vectors, found %s",
          deleteFile.location());
    }

    table.refresh();
    Snapshot head = table.snapshot(branch);
    if (head == null) {
      return;
    }

    rowDelta.validateFromSnapshot(validationStart(table, head, operatorId, bootstrapSnapshotId));
    rowDelta.validateDataFilesExist(Arrays.asList(result.referencedDataFiles()));
    rowDelta.validateDeletedFiles();
    rowDelta.validateNoConflictingDeleteFiles();

    Set<String> referenced = Sets.newHashSet();
    for (DeleteFile dv : result.deleteFiles()) {
      referenced.add(dv.referencedDataFile());
    }

    if (referenced.isEmpty()) {
      return;
    }

    // shortcut: reads every delete manifest of the head on each commit, upgrade to a DV lookup
    for (ManifestFile manifest : head.deleteManifests(table.io())) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        for (DeleteFile live : reader) {
          if (ContentFileUtil.isDV(live) && referenced.contains(live.referencedDataFile())) {
            rowDelta.removeDeletes(live.copy());
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private static long validationStart(
      Table table, Snapshot head, String operatorId, Long bootstrapSnapshotId) {
    for (Snapshot snapshot = head;
        snapshot != null;
        snapshot = snapshot.parentId() == null ? null : table.snapshot(snapshot.parentId())) {
      if (operatorId.equals(snapshot.summary().get(SinkUtil.OPERATOR_ID))) {
        return snapshot.snapshotId();
      }
    }

    if (bootstrapSnapshotId != null
        && SnapshotUtil.isAncestorOf(table, head.snapshotId(), bootstrapSnapshotId)) {
      return bootstrapSnapshotId;
    }

    return head.snapshotId();
  }
}
