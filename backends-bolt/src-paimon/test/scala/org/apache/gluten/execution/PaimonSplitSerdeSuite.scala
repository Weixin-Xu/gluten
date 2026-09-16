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

import org.apache.gluten.substrait.rel.PaimonSerializedSplitLocalFilesBuilder

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.test.SharedSparkSession

import io.substrait.proto.ReadRel
import org.apache.paimon.io.DataOutputSerializer
import org.apache.paimon.spark.{PaimonInputPartition, PaimonScan}
import org.apache.paimon.table.source.DataSplit

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{Files, Paths}
import java.util.Base64

import scala.collection.JavaConverters._

class PaimonSplitSerdeSuite extends SharedSparkSession {
  private val splitFixtureResource = "/paimon-split-v8.bin"
  private val warehouse = Files.createTempDirectory("paimon-split-serde").toFile

  private def resourceBytes(resource: String): Array[Byte] = {
    val stream = Option(getClass.getResourceAsStream(resource))
      .getOrElse(fail(s"Missing test resource: $resource"))
    try {
      stream.readAllBytes()
    } finally {
      stream.close()
    }
  }

  private def regenerateSplitFixture(): Unit = {
    val fixture = Paths.get(
      sys.props("basedir"),
      "src-paimon",
      "test",
      "resources",
      splitFixtureResource.stripPrefix("/"))
    Files.createDirectories(fixture.getParent)
    val bytes = PaimonSplitSerde.serialize(plannedDataSplit())
    assert(bytes.nonEmpty)
    assert(PaimonSplitSerde.version(bytes) === 8)
    assert(PaimonSplitSerde.isSupportedVersion(bytes))
    Files.write(fixture, bytes)
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.shuffle.partitions", "1")
      .set(
        "spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
      .set("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
      .set("spark.sql.catalog.paimon.warehouse", warehouse.toURI.toString)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql("CREATE DATABASE paimon.split_serde")
    sql("USE paimon.split_serde")
    sql(
      "CREATE TABLE append_table (id INT, value STRING) USING paimon " +
        "TBLPROPERTIES ('file.format' = 'parquet')")
    sql("INSERT INTO append_table VALUES (1, 'one'), (2, 'two')")
  }

  override def afterAll(): Unit = {
    try {
      sql("USE default")
      sql("DROP DATABASE IF EXISTS paimon.split_serde CASCADE")
    } finally {
      org.apache.commons.io.FileUtils.deleteDirectory(warehouse)
      super.afterAll()
    }
  }

  private def plannedDataSplit(): DataSplit = {
    val scan = sql("SELECT * FROM append_table").queryExecution.sparkPlan.collectFirst {
      case value: BatchScanExec => value.scan.asInstanceOf[PaimonScan]
    }.getOrElse(fail("Paimon batch scan is missing"))

    scan.toBatch
      .planInputPartitions()
      .collectFirst {
        case partition: PaimonInputPartition =>
          partition.splits.collectFirst {
            case split: DataSplit => split
          }
      }
      .flatten
      .getOrElse(fail("Planned Paimon DataSplit is missing"))
  }

  test("serialized split uses the native Paimon field number") {
    val readOptionsDescriptor =
      ReadRel.LocalFiles.FileOrFiles.PaimonReadOptions.getDescriptor
    assert(readOptionsDescriptor.findFieldByName("serialized_split").getNumber === 10)
  }

  test("split serialization returns exactly the Paimon payload") {
    val split = plannedDataSplit()
    val bytes = PaimonSplitSerde.serialize(split)

    assert(bytes.nonEmpty)
    assert(PaimonSplitSerde.version(bytes) === 8)
    val reference = new DataOutputSerializer(1024)
    split.serialize(reference)
    assert(bytes.length === reference.length())
    assert(bytes.sameElements(reference.getSharedBuffer.take(reference.length())))
    assert(PaimonSplitSerde.isSupportedVersion(bytes))
  }

  test("checked-in Java split fixture has a supported version") {
    if (java.lang.Boolean.getBoolean("gluten.paimon.regenerateSplitFixture")) {
      regenerateSplitFixture()
    } else {
      val fixture = resourceBytes(splitFixtureResource)
      assert(fixture.nonEmpty)
      assert(PaimonSplitSerde.version(fixture) === 8)
      assert(PaimonSplitSerde.isSupportedVersion(fixture))
    }
  }

  test("split version parsing validates its header and compatibility range") {
    def header(magic: Long, version: Int): Array[Byte] =
      ByteBuffer
        .allocate(java.lang.Long.BYTES + java.lang.Integer.BYTES)
        .order(ByteOrder.BIG_ENDIAN)
        .putLong(magic)
        .putInt(version)
        .array()

    val magic = -2394839472490812314L
    assert(PaimonSplitSerde.isSupportedVersion(header(magic, 2)))
    assert(PaimonSplitSerde.isSupportedVersion(header(magic, 8)))
    assert(!PaimonSplitSerde.isSupportedVersion(header(magic, 1)))
    assert(!PaimonSplitSerde.isSupportedVersion(header(magic, 9)))

    val short = intercept[IllegalArgumentException] {
      PaimonSplitSerde.version(Array.fill[Byte](11)(0))
    }
    assert(short.getMessage.contains("shorter than its header"))
    val invalidMagic = intercept[IllegalArgumentException] {
      PaimonSplitSerde.version(header(0L, 8))
    }
    assert(invalidMagic.getMessage.contains("invalid magic"))
  }

  test("serialized local files preserve one binary payload per affinity path") {
    val affinityPaths = Seq("file:///paimon/split-0", "file:///paimon/split-1")
    val serializedSplits = Seq(
      Array[Byte](0, 1, 2, 3, -1),
      Array[Byte](9, 8, 0, 7, -128))

    val node = PaimonSerializedSplitLocalFilesBuilder.make(
      3,
      affinityPaths.asJava,
      Seq("executor-a").asJava,
      serializedSplits.asJava)
    val localFiles = node.toProtobuf

    assert(localFiles.getItemsCount === serializedSplits.size)
    localFiles.getItemsList.asScala.zipWithIndex.foreach {
      case (item, index) =>
        assert(item.getUriFile === affinityPaths(index))
        assert(item.getUriFile.nonEmpty)
        assert(item.hasPaimon)
        assert(item.getPaimon.getSerializedSplit.toByteArray.sameElements(serializedSplits(index)))
        assert(
          item.getPaimon.getSerializedSplit.toStringUtf8 !==
            Base64.getEncoder.encodeToString(serializedSplits(index)))
        val optionFields = item.getPaimon.getAllFields.keySet().asScala.map(_.getName)
        assert(optionFields === Set("serialized_split"))
    }
  }

  test("serialized local files reject mismatched paths and split counts") {
    val error = intercept[IllegalArgumentException] {
      PaimonSerializedSplitLocalFilesBuilder.make(
        0,
        Seq("file:///paimon/split-0").asJava,
        Seq.empty[String].asJava,
        Seq(Array[Byte](1), Array[Byte](2)).asJava)
    }
    assert(error.getMessage.nonEmpty)
  }

  test("serialized local files reject empty split payloads") {
    val error = intercept[IllegalArgumentException] {
      PaimonSerializedSplitLocalFilesBuilder.make(
        0,
        Seq("file:///paimon/split-0").asJava,
        Seq.empty[String].asJava,
        Seq(Array.emptyByteArray).asJava)
    }
    assert(error.getMessage.nonEmpty)
  }
}
