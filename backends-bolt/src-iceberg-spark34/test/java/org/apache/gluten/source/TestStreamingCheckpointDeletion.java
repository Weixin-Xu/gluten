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
package org.apache.gluten.source;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestStreamingCheckpointDeletion {
  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void deletingCommitAllowsCheckpointReplay() throws Exception {
    Configuration conf = new Configuration();
    File directory = temp.newFolder("commits");
    File commitFile = new File(directory, "1");
    Path commit = new Path(commitFile.toURI());
    FileSystem fs = commit.getFileSystem(conf);
    Path checksum = new Path(commit.getParent(), ".1.crc");
    try (FSDataOutputStream out = fs.create(commit, false)) {
      out.writeUTF("original commit");
    }
    assertTrue(fs.exists(commit));
    assertTrue("The fixture must include a Hadoop checksum sidecar", fs.exists(checksum));
    assertTrue(TestStructuredStreaming.deleteCommitFile(commitFile));
    assertFalse(fs.exists(commit));
    assertFalse("Deleting a checkpoint commit must remove its checksum", fs.exists(checksum));

    // Spark's checkpoint manager atomically renames the replayed commit into the same path.
    Path replay = new Path(commit.getParent(), ".1.replay.tmp");
    try (FSDataOutputStream out = fs.create(replay, false)) {
      out.writeUTF("replayed commit");
    }
    FileContext.getFileContext(commit.toUri(), conf).rename(replay, commit, Options.Rename.NONE);
    assertTrue(fs.exists(commit));
    assertTrue(fs.exists(checksum));
  }
}
