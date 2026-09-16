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
package org.apache.gluten.execution

import org.apache.paimon.io.DataOutputSerializer
import org.apache.paimon.table.source.DataSplit

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Arrays

object PaimonSplitSerde {
  private val Magic = -2394839472490812314L
  // Match paimon-cpp DataSplitImpl::GetFileMetaSerializer: v1's DataFileMeta08 is
  // unimplemented; v2..v8 have explicit decoders. Extend only with native support.
  private val MinimumSupportedVersion = 2
  private val MaximumSupportedVersion = 8

  def serialize(split: DataSplit): Array[Byte] = {
    val out = new DataOutputSerializer(1024)
    split.serialize(out)
    Arrays.copyOf(out.getSharedBuffer, out.length())
  }

  def version(bytes: Array[Byte]): Int = {
    require(
      bytes.length >= java.lang.Long.BYTES + java.lang.Integer.BYTES,
      "Serialized Paimon split is shorter than its header")
    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
    require(buffer.getLong() == Magic, "Serialized Paimon split has invalid magic")
    buffer.getInt()
  }

  def isSupportedVersion(bytes: Array[Byte]): Boolean = {
    val value = version(bytes)
    value >= MinimumSupportedVersion && value <= MaximumSupportedVersion
  }
}
