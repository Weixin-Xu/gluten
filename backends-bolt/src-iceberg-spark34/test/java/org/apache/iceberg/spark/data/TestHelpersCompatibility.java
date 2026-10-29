/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iceberg.spark.data;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TestHelpersCompatibility {
  @Test
  public void deleteFilesUsesRequestedSnapshotAndDeletionVectorIdentity() throws Exception {
    DeleteFile first = deletionVector(100L);
    DeleteFile second = deletionVector(200L);
    FileScanTask task =
        proxy(
            FileScanTask.class,
            (object, method, args) -> {
              if (method.getName().equals("deletes")) {
                return Arrays.asList(first, first.copy(), second);
              }
              throw new UnsupportedOperationException(method.getName());
            });
    AtomicLong selectedSnapshot = new AtomicLong(-1L);
    TableScan scan =
        proxy(
            TableScan.class,
            (object, method, args) -> {
              if (method.getName().equals("useSnapshot")) {
                selectedSnapshot.set((Long) args[0]);
                return object;
              }
              if (method.getName().equals("planFiles")) {
                assertEquals(4321L, selectedSnapshot.get());
                return CloseableIterable.withNoopClose(Arrays.asList(task));
              }
              throw new UnsupportedOperationException(method.getName());
            });
    Table table =
        proxy(
            Table.class,
            (object, method, args) -> {
              if (method.getName().equals("newScan")) {
                return scan;
              }
              throw new UnsupportedOperationException(method.getName());
            });
    Snapshot snapshot =
        proxy(
            Snapshot.class,
            (object, method, args) -> {
              if (method.getName().equals("snapshotId")) {
                return 4321L;
              }
              throw new UnsupportedOperationException(method.getName());
            });

    // Resolve the exact binary API used by Iceberg's historical-delete tests.
    Method overload = TestHelpers.class.getMethod("deleteFiles", Table.class, Snapshot.class);
    @SuppressWarnings("unchecked")
    Set<DeleteFile> deletes = (Set<DeleteFile>) overload.invoke(null, table, snapshot);

    assertEquals(4321L, selectedSnapshot.get());
    assertEquals(2, deletes.size());
    assertEquals(
        new HashSet<>(Arrays.asList(100L, 200L)),
        deletes.stream().map(DeleteFile::contentOffset).collect(Collectors.toSet()));
  }

  private static DeleteFile deletionVector(long offset) {
    return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
        .ofPositionDeletes()
        .withPath("delete-vectors.puffin")
        .withFormat(FileFormat.PUFFIN)
        .withRecordCount(1L)
        .withFileSizeInBytes(1024L)
        .withReferencedDataFile("data.parquet")
        .withContentOffset(offset)
        .withContentSizeInBytes(64L)
        .build();
  }

  private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
    return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
  }
}
