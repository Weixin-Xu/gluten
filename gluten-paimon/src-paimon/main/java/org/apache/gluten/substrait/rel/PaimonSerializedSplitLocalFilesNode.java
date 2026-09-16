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
package org.apache.gluten.substrait.rel;

import com.google.protobuf.ByteString;
import io.substrait.proto.ReadRel;
import io.substrait.proto.ReadRel.LocalFiles.FileOrFiles.PaimonReadOptions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonSerializedSplitLocalFilesNode extends LocalFilesNode {
  private final List<byte[]> serializedSplits;

  PaimonSerializedSplitLocalFilesNode(
      Integer index,
      List<String> affinityPaths,
      List<String> preferredLocations,
      List<byte[]> serializedSplits) {
    super(
        index,
        affinityPaths,
        new ArrayList<>(Collections.nCopies(serializedSplits.size(), 0L)),
        new ArrayList<>(Collections.nCopies(serializedSplits.size(), 0L)),
        new ArrayList<>(),
        new ArrayList<>(),
        emptyPartitionColumns(serializedSplits.size()),
        new ArrayList<>(),
        ReadFileFormat.UnknownFormat,
        preferredLocations,
        new HashMap<>(),
        new ArrayList<>());
    this.serializedSplits = serializedSplits;
  }

  private static List<Map<String, String>> emptyPartitionColumns(int size) {
    List<Map<String, String>> result = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      result.add(new HashMap<>());
    }
    return result;
  }

  @Override
  protected void processFileBuilder(ReadRel.LocalFiles.FileOrFiles.Builder fileBuilder, int index) {
    PaimonReadOptions options =
        PaimonReadOptions.newBuilder()
            .setSerializedSplit(ByteString.copyFrom(serializedSplits.get(index)))
            .build();
    fileBuilder.setPaimon(options);
  }
}
