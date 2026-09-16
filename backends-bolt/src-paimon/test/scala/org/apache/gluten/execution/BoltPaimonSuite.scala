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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.test.FallbackUtil

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, GlutenQueryTest, Row}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.types.{DecimalType, DoubleType, FloatType, IntegerType, LongType, StructType}

import io.substrait.proto.ReadRel
import org.apache.paimon.spark.{PaimonInputPartition, PaimonScan}
import org.apache.paimon.table.source.DataSplit

import java.sql.Timestamp
import java.time.Instant

import scala.collection.JavaConverters._
import scala.util.Random

class BoltPaimonSuite extends WholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.readSideCharPadding", "false")
      .set("spark.gluten.paimon.native.source.enabled", "true")
      .set("spark.gluten.paimon.native.mor.source.enabled", "true")
      .set("spark.gluten.paimon.native.mor.aggregate.engine.enabled", "true")
      .set("spark.gluten.paimon.native.mor.partial.update.engine.enabled", "true")
      .set(
        "spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
      .set("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
      .set("spark.sql.catalog.paimon.warehouse", s"file://$rootPath/data-paimon")
    // .set("spark.gluten.sql.debug", "true")
  }

  protected val dbName0: String = "test"
  protected val tableName0: String = "T"

  override def beforeAll(): Unit = {
    super.beforeAll()
    assume(
      BackendsApiManager.getBackendName == "bolt",
      "Skipping suite: backend is not Bolt"
    )
    spark.sql(s"USE paimon")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS paimon.$dbName0")
  }

  override def afterAll(): Unit = {
    try {
      spark.sql(s"USE paimon")
      spark.sql(s"DROP TABLE IF EXISTS $dbName0.$tableName0")
      spark.sql("USE default")
      spark.sql(s"DROP DATABASE paimon.$dbName0 CASCADE")
    } finally {
      super.afterAll()
    }
  }

  /** Default is paimon catalog */
  override def beforeEach(): Unit = {
    super.beforeAll()
    spark.sql(s"USE paimon")
    spark.sql(s"USE paimon.$dbName0")
    spark.sql(s"DROP TABLE IF EXISTS $tableName0")
  }

  private def boltPaimonScanNode(df: DataFrame): BoltPaimonScanTransformer = {
    val plan = getExecutedPlan(df)
    plan.collectFirst {
      case value: BoltPaimonScanTransformer => value
    }.getOrElse(fail(s"BoltPaimonScanTransformer missing from:\n${df.queryExecution.executedPlan}"))
  }

  private def boltPaimonScan(df: DataFrame): BoltPaimonScanTransformer = {
    val scan = boltPaimonScanNode(df)
    val localFiles = scan.getSplitInfos.map(
      _.toProtobuf.asInstanceOf[ReadRel.LocalFiles])
    assert(localFiles.nonEmpty, "Bolt Paimon scan produced no LocalFiles")
    localFiles.foreach {
      files =>
        assert(files.getItemsCount > 0, files.toString)
        files.getItemsList.asScala.foreach {
          item =>
            assert(item.hasPaimon, item.toString)
            assert(!item.getPaimon.getSerializedSplit.isEmpty, item.toString)
        }
    }
    scan
  }

  private def assertJavaBatchScan(df: DataFrame): Unit = {
    val plan = getExecutedPlan(df)
    assert(!plan.exists(_.isInstanceOf[BoltPaimonScanTransformer]), plan.toString())
    assert(plan.exists(_.isInstanceOf[BatchScanExec]), plan.toString())
  }

  private def rawInputRows(df: DataFrame): Long = {
    df.collect()
    boltPaimonScan(df).metrics
      .get("rawInputRows")
      .map(_.value)
      .getOrElse(fail("rawInputRows metric is missing from BoltPaimonScanTransformer"))
  }

  private def runJavaQueryAndCompare(query: String): DataFrame =
    runQueryAndCompare(query, noFallBack = false)(assertJavaBatchScan)

  private def assertMultiSplitOrderAndAffinity(scan: BoltPaimonScanTransformer): Unit = {
    val paimonScan = scan.scan.asInstanceOf[PaimonScan]
    val expectedSplits = paimonScan.toBatch.planInputPartitions().toSeq.flatMap {
      case partition: PaimonInputPartition =>
        partition.splits.map(_.asInstanceOf[DataSplit])
      case partition => fail(s"Unexpected Paimon input partition: ${partition.getClass.getName}")
    }
    assert(expectedSplits.size > 1, s"Expected multiple DataSplits, found ${expectedSplits.size}")

    val actualItems = scan.getSplitInfos.flatMap {
      info =>
        info.toProtobuf
          .asInstanceOf[ReadRel.LocalFiles]
          .getItemsList
          .asScala
    }
    assert(actualItems.size == expectedSplits.size)
    expectedSplits.zip(actualItems).foreach {
      case (split, item) =>
        assert(
          java.util.Arrays.equals(
            PaimonSplitSerde.serialize(split),
            item.getPaimon.getSerializedSplit.toByteArray))
        val rawFiles = split.convertToRawFiles()
        if (rawFiles.isPresent && !rawFiles.get().isEmpty) {
          assert(item.getUriFile == rawFiles.get().get(0).path())
        }
    }
  }

  override protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val analyzedDf = df
    assertEmptyMissingInput(analyzedDf)
    GlutenQueryTest.checkAnswer(analyzedDf, expectedAnswer, checkToRDD = false)
  }

  // test for append table
  test("paimon transformer exists: append table") {
    Seq("parquet").foreach {
      format =>
        {
          val tbl_name = s"paimon_$format"
          withTable(tbl_name) {
            spark.sql(s"""
                         |create table $tbl_name (id INT, name STRING) using paimon
                         |TBLPROPERTIES (
                         |'file.format' = '$format',
                         | 'bucket' = '3',
                         | 'bucket-key' = 'id')
                         |""".stripMargin)
            (1 to 10).foreach {
              id =>
                spark.sql(s"""
                             |insert into $tbl_name values($id, '$id')
                             |""".stripMargin)
            }

            runQueryAndCompare(s"""
                                  |select * from $tbl_name;
                                  |""".stripMargin) {
              boltPaimonScan
            }

            runQueryAndCompare(s"""
                                  |select * from $tbl_name where id = 10;
                                  |""".stripMargin) {
              boltPaimonScan
            }
          }
        }
    }
  }

  test("paimon transformer exists: append partition table") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING, dt STRING)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'bucket' = '3',
                   | 'bucket-key' = 'id')
                   |PARTITIONED BY (dt)
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values(1, '1', '20250506')
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values(2, '2', '20250507')
                   |""".stripMargin)

      (1 to 10).foreach {
        id =>
          spark.sql(s"""
                       |insert into $tbl_name values($id, '$id', '20250507')
                       |""".stripMargin)
      }
      (1 to 10).foreach {
        id =>
          spark.sql(s"""
                       |insert into $tbl_name values($id, '$id', '20250506')
                       |""".stripMargin)
      }

      runQueryAndCompare(s"""
                            |select * from $tbl_name where dt = '20250506';
                            |""".stripMargin) {
        boltPaimonScan
      }

      runQueryAndCompare(s"""
                            |select * from $tbl_name where dt = '20250506';
                            |""".stripMargin) {
        boltPaimonScan
      }
    }
  }

  test("Bolt append scans preserve split order and reduce raw rows for pushed filters") {
    val table = "paimon_append_matrix"
    withTable(table) {
      spark.sql(s"""
                   |CREATE TABLE $table (id BIGINT, payload STRING) USING paimon
                   |TBLPROPERTIES (
                   |  'file.format' = 'parquet',
                   |  'bucket' = '8',
                   |  'bucket-key' = 'id')
                   |""".stripMargin)
      spark.range(0, 4096)
        .select(col("id"), concat(lit("value-"), col("id")).as("payload"))
        .writeTo(table)
        .append()

      val unfiltered = runQueryAndCompare(s"SELECT id, payload FROM $table") {
        scan => assertMultiSplitOrderAndAffinity(boltPaimonScan(scan))
      }
      runQueryAndCompare(s"SELECT payload FROM $table WHERE id = 17") {
        boltPaimonScan
      }
      val filtered = runQueryAndCompare(
        s"SELECT id FROM $table WHERE id >= 16 AND id < 32 AND length(payload) > 0") {
        boltPaimonScan
      }

      assert(rawInputRows(filtered) < rawInputRows(unfiltered))
    }
  }

  test("Bolt append scan matches Java for null and complex native types") {
    val table = "paimon_native_types"
    withTable(table) {
      spark.sql(s"""
                   |CREATE TABLE $table (
                   |  id INT,
                   |  nullable_value STRING,
                   |  amount DECIMAL(38, 18),
                   |  event_date DATE,
                   |  event_time TIMESTAMP,
                   |  payload BINARY,
                   |  numbers ARRAY<INT>,
                   |  labels MAP<STRING, INT>,
                   |  nested STRUCT<x: INT, y: STRING>)
                   |USING paimon
                   |TBLPROPERTIES ('file.format' = 'parquet')
                   |""".stripMargin)
      spark.sql(s"""
                   |INSERT INTO $table SELECT
                   |  1,
                   |  CAST(NULL AS STRING),
                   |  CAST('12345678901234567890.123456789012345678' AS DECIMAL(38, 18)),
                   |  DATE '2026-08-31',
                   |  TIMESTAMP '2026-08-31 12:34:56.123456',
                   |  unhex('00017fff'),
                   |  array(1, CAST(NULL AS INT), 3),
                   |  map('first', 10, 'second', CAST(NULL AS INT)),
                   |  named_struct('x', 7, 'y', 'nested')
                   |""".stripMargin)

      runQueryAndCompare(s"SELECT * FROM $table") {
        boltPaimonScan
      }
    }
  }

  test("Bolt Parquet scan preserves negative epoch timestamp microseconds by default") {
    val table = "paimon_negative_timestamp"
    withTable(table) {
      withSQLConf("spark.sql.session.timeZone" -> "UTC") {
        spark.sql(s"CREATE TABLE $table (id INT, event_time TIMESTAMP) USING paimon " +
          "TBLPROPERTIES ('file.format' = 'parquet', 'bucket' = '-1')")
        spark.sql(s"INSERT INTO $table VALUES " +
          "(1, TIMESTAMP '1969-12-31 23:59:59.123456'), " +
          "(2, TIMESTAMP '1970-01-01 00:00:00.654321')")

        val df = runQueryAndCompare(s"SELECT id, event_time FROM $table ORDER BY id") {
          boltPaimonScan
        }
        checkAnswer(
          df,
          Seq(
            Row(1, Timestamp.valueOf("1969-12-31 23:59:59.123456")),
            Row(2, Timestamp.valueOf("1970-01-01 00:00:00.654321"))))
      }
    }
  }

  test("Bolt Parquet scan honors millisecond timestamp unit for negative epochs") {
    val table = "paimon_millisecond_timestamp"
    withTable(table) {
      withSQLConf("spark.sql.session.timeZone" -> "UTC") {
        spark.sql(s"CREATE TABLE $table (id INT, event_time TIMESTAMP) USING paimon " +
          "TBLPROPERTIES ('file.format' = 'parquet', 'bucket' = '-1')")
        spark.sql(s"INSERT INTO $table VALUES " +
          "(1, TIMESTAMP '1969-12-31 23:59:59.123456'), " +
          "(2, TIMESTAMP '1970-01-01 00:00:00.654321')")

        withSQLConf("spark.gluten.paimon.read.timestamp-unit" -> "3") {
          val df = spark.sql(s"SELECT id, event_time FROM $table ORDER BY id")
          checkAnswer(
            df,
            Seq(
              Row(1, Timestamp.valueOf("1969-12-31 23:59:59.123")),
              Row(2, Timestamp.valueOf("1970-01-01 00:00:00.654"))))
          boltPaimonScan(df)
        }
      }
    }
  }

  test("Bolt Paimon falls back for Timestamp partition values") {
    val table = "paimon_timestamp_partition"
    withTable(table) {
      withSQLConf("spark.sql.session.timeZone" -> "UTC") {
        spark.sql(s"CREATE TABLE $table (id INT, p TIMESTAMP) USING paimon " +
          "PARTITIONED BY (p) " +
          "TBLPROPERTIES ('file.format' = 'parquet', 'bucket' = '-1')")
        spark.sql(s"INSERT INTO $table VALUES " +
          "(1, TIMESTAMP '1969-12-31 23:59:59.123456'), " +
          "(2, TIMESTAMP '2026-09-14 12:34:56.654321')")

        val df = runJavaQueryAndCompare(s"SELECT id, p FROM $table ORDER BY id")
        checkAnswer(
          df,
          Seq(
            Row(1, Timestamp.valueOf("1969-12-31 23:59:59.123456")),
            Row(2, Timestamp.valueOf("2026-09-14 12:34:56.654321"))))
      }
    }
  }

  test("Bolt append scans preserve results with predicate filtering disabled") {
    val table = "paimon_predicate_disabled"
    withTable(table) {
      spark.sql(s"CREATE TABLE $table (id INT, value STRING) USING paimon " +
        "TBLPROPERTIES ('file.format' = 'parquet')")
      spark.sql(s"INSERT INTO $table VALUES " +
        "(1, 'one'), (2, 'two'), (3, 'three'), (4, NULL), (NULL, 'two'), (NULL, NULL)")
      withSQLConf("spark.gluten.paimon.read.predicate-filter-enabled" -> "false") {
        runQueryAndCompare(s"SELECT id FROM $table WHERE id = 2") {
          boltPaimonScan
        }
        // A cross-column OR cannot be enforced by Parquet's subfield filters. The
        // full residual must survive even when both leaves translate to Paimon.
        val df = runQueryAndCompare(s"SELECT id FROM $table WHERE id = 1 OR value = 'two'") {
          boltPaimonScan
        }
        checkAnswer(df, Seq(Row(1), Row(2), Row(null)))
      }
    }
  }

  test("Bolt Paimon preserves physical field names in case insensitive scans") {
    val table = "paimon_mixed_case"
    withTable(table) {
      withSQLConf("spark.sql.caseSensitive" -> "false") {
        spark.sql(s"CREATE TABLE $table (ID INT, Payload STRUCT<MixedField: INT>) " +
          "USING paimon TBLPROPERTIES ('file.format' = 'parquet', 'bucket' = '-1')")
        spark.sql(s"INSERT INTO $table VALUES " +
          "(1, named_struct('MixedField', 10)), (2, named_struct('MixedField', 20))")
        val df = runQueryAndCompare(
          s"SELECT id, payload.mixedfield FROM $table WHERE id = 2") {
          boltPaimonScan
        }
        checkAnswer(df, Seq(Row(2, 20)))
      }
    }
  }

  test("Bolt Paimon timestamp precision follows each session configuration") {
    val table = "paimon_session_timestamp"
    withTable(table) {
      withSQLConf("spark.sql.session.timeZone" -> "UTC") {
        spark.sql(s"CREATE TABLE $table (id INT, ts TIMESTAMP) USING paimon " +
          "TBLPROPERTIES ('file.format' = 'parquet', 'bucket' = '-1')")
        spark.sql(s"INSERT INTO $table VALUES " +
          "(1, TIMESTAMP '1969-12-31 23:59:59.123456')")
        val query = s"SELECT ts FROM $table WHERE id = 1"
        // Use the Java reader's instant rather than Timestamp.valueOf, which uses the JVM zone.
        var javaTimestamp: Timestamp = null
        withSQLConf(vanillaSparkConfs(): _*) {
          javaTimestamp = spark.sql(query).collect().head.getTimestamp(0)
        }
        assert(javaTimestamp.getNanos == 123456000)
        Seq("6", "3", "6").foreach {
          unit =>
            withSQLConf("spark.gluten.paimon.read.timestamp-unit" -> unit) {
              val expected = Timestamp.from(javaTimestamp.toInstant)
              if (unit == "3") {
                expected.setNanos(expected.getNanos / 1000000 * 1000000)
              }
              val df = spark.sql(query)
              checkAnswer(df, Seq(Row(expected)))
              boltPaimonScan(df)
            }
        }
      }
    }
  }

  test("Bolt append scans support constant projections and empty results") {
    val table = "paimon_constant_projection"
    withTable(table) {
      spark.sql(s"CREATE TABLE $table (id INT) USING paimon " +
        "TBLPROPERTIES ('file.format' = 'parquet')")
      spark.sql(s"INSERT INTO $table VALUES (1), (2), (3)")
      val constantProjection = runQueryAndCompare(s"SELECT 1 FROM $table") {
        boltPaimonScanNode
      }
      checkAnswer(constantProjection, Seq(Row(1), Row(1), Row(1)))
      runQueryAndCompare(s"SELECT id FROM $table WHERE id > 10") {
        boltPaimonScanNode
      }
    }
  }

  test("Bolt Paimon storage partition join preserves empty task partitions") {
    withSQLConf(
      "spark.sql.adaptive.enabled" -> "false",
      "spark.sql.autoBroadcastJoinThreshold" -> "-1",
      "spark.sql.sources.v2.bucketing.enabled" -> "true",
      "spark.sql.sources.v2.bucketing.pushPartValues.enabled" -> "true",
      "spark.sql.sources.v2.bucketing.partiallyClusteredDistribution.enabled" -> "false"
    ) {
      withTable("paimon_join_left", "paimon_join_right") {
        Seq("paimon_join_left", "paimon_join_right").foreach {
          table =>
            sql(s"CREATE TABLE $table (id INT) USING paimon " +
              "TBLPROPERTIES ('file.format' = 'parquet', 'bucket' = '4', 'bucket-key' = 'id')")
        }
        sql("INSERT INTO paimon_join_left SELECT CAST(id AS INT) FROM range(64)")
        sql("INSERT INTO paimon_join_right VALUES (0)")
        val df = sql("SELECT l.id, r.id FROM paimon_join_left l " +
          "LEFT JOIN paimon_join_right r ON l.id = r.id")
        checkAnswer(df, (0 until 64).map(id => Row(id, if (id == 0) 0 else null)))
        val scans = getExecutedPlan(df).collect { case scan: BoltPaimonScanTransformer => scan }
        assert(scans.size == 2, df.queryExecution.executedPlan.toString)
        assert(
          scans.exists(_.getSplitInfos.exists(
            _.toProtobuf.asInstanceOf[ReadRel.LocalFiles].getItemsCount == 0)),
          "Storage partition join did not exercise an empty Paimon task partition"
        )
      }
    }
  }

  test("Bolt Paimon reports scan rows and actual storage reads") {
    val table = "paimon_read_metrics"
    withTable(table) {
      spark.sql(s"CREATE TABLE $table (id INT, value STRING) USING paimon " +
        "TBLPROPERTIES ('file.format' = 'parquet', 'bucket' = '-1')")
      spark.sql(s"INSERT INTO $table VALUES (1, 'one'), (2, 'two'), (3, 'three')")
      val df = spark.sql(s"SELECT * FROM $table")
      checkAnswer(df, Seq(Row(1, "one"), Row(2, "two"), Row(3, "three")))
      val metrics = boltPaimonScan(df).metrics
      assert(metrics("rawInputRows").value == 3)
      assert(metrics("rawInputBytes").value > 0)
      assert(metrics("storageReadBytes").value == metrics("rawInputBytes").value)
      assert(metrics("storageReadCount").value > 0)
    }
  }

  test("Bolt append scan follows schema evolution and compaction") {
    val table = "paimon_append_evolution"
    withTable(table) {
      spark.sql(s"""
                   |CREATE TABLE $table (id INT, value STRING) USING paimon
                   |TBLPROPERTIES (
                   |  'file.format' = 'parquet',
                   |  'bucket' = '1',
                   |  'bucket-key' = 'id',
                   |  'target-file-size' = '100b')
                   |""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'one'), (2, 'two')")
      runQueryAndCompare(s"SELECT id, value FROM $table ORDER BY id") {
        boltPaimonScan
      }

      spark.sql(s"ALTER TABLE $table ADD COLUMN note STRING")
      spark.sql(s"INSERT INTO $table (id, value, note) VALUES (3, 'three', 'added')")
      runQueryAndCompare(s"SELECT id, value, note FROM $table ORDER BY id") {
        boltPaimonScan
      }

      spark.sql(s"ALTER TABLE $table RENAME COLUMN note TO renamed_note")
      runQueryAndCompare(s"SELECT id, renamed_note FROM $table ORDER BY id") {
        boltPaimonScan
      }

      spark.sql(s"ALTER TABLE $table DROP COLUMN value")
      runQueryAndCompare(s"SELECT id, renamed_note FROM $table ORDER BY id") {
        boltPaimonScan
      }

      spark.sql(s"CALL sys.compact(table => '$table')")
      runQueryAndCompare(s"SELECT id, renamed_note FROM $table ORDER BY id") {
        boltPaimonScan
      }
    }
  }

  test("Bolt ORC scan supports ordinary filtering") {
    val table = "paimon_native_orc"
    withTable(table) {
      spark.sql(s"""
                   |CREATE TABLE $table (id INT, value STRING) USING paimon
                   |TBLPROPERTIES ('file.format' = 'orc')
                   |""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'one'), (2, 'two')")

      val df = runQueryAndCompare(s"SELECT * FROM $table WHERE id >= 2 ORDER BY id") {
        boltPaimonScan
      }
      checkAnswer(df, Seq(Row(2, "two")))
    }
  }

  test("Bolt ORC scan matches Spark for decimal and complex columns") {
    val table = "paimon_orc_native_types"
    withTable(table) {
      withSQLConf("spark.sql.session.timeZone" -> "UTC") {
        spark.sql(s"""
                     |CREATE TABLE $table (
                     |  id INT,
                     |  amount DECIMAL(38, 6),
                     |  numbers ARRAY<INT>,
                     |  labels MAP<STRING, INT>,
                     |  nested STRUCT<x: INT, y: STRING>)
                     |USING paimon
                     |TBLPROPERTIES ('file.format' = 'orc', 'bucket' = '-1')
                     |""".stripMargin)
        spark.sql(s"""
                     |INSERT INTO $table VALUES
                     |  (1, 12345678901234567890123456789012.123456,
                     |    array(1, CAST(NULL AS INT), 3),
                     |    map('first', 10, 'second', CAST(NULL AS INT)),
                     |    named_struct('x', 7, 'y', 'nested')),
                     |  (2, NULL, NULL, NULL, NULL)
                     |""".stripMargin)

        runQueryAndCompare(s"SELECT * FROM $table ORDER BY id") {
          boltPaimonScan
        }
      }
    }
  }

  test("Bolt ORC scan falls back for exact Timestamp results") {
    val table = "paimon_orc_timestamp_fallback"
    withTable(table) {
      withSQLConf("spark.sql.session.timeZone" -> "UTC") {
        spark.sql(s"CREATE TABLE $table (id INT, event_time TIMESTAMP) USING paimon " +
          "TBLPROPERTIES ('file.format' = 'orc', 'bucket' = '-1')")
        spark.sql(s"INSERT INTO $table VALUES " +
          "(1, TIMESTAMP '1969-12-31 23:59:59.123456'), " +
          "(2, TIMESTAMP '1970-01-01 00:00:00.654321')")

        val df = runJavaQueryAndCompare(s"SELECT id, event_time FROM $table ORDER BY id")
        checkAnswer(
          df,
          Seq(
            Row(1, Timestamp.from(Instant.parse("1969-12-31T23:59:59.123456Z"))),
            Row(2, Timestamp.from(Instant.parse("1970-01-01T00:00:00.654321Z")))))
      }
    }
  }

  test("Bolt ORC scan supports constant count and empty projections") {
    val table = "paimon_orc_projections"
    withTable(table) {
      spark.sql(s"CREATE TABLE $table (id INT) USING paimon " +
        "TBLPROPERTIES ('file.format' = 'orc', 'bucket' = '-1')")
      spark.sql(s"INSERT INTO $table VALUES (1), (2), (3)")

      val constantProjection = runQueryAndCompare(s"SELECT 1 FROM $table") {
        boltPaimonScanNode
      }
      checkAnswer(constantProjection, Seq(Row(1), Row(1), Row(1)))
      val count = runQueryAndCompare(s"SELECT COUNT(*) FROM $table WHERE id >= 2") {
        boltPaimonScanNode
      }
      checkAnswer(count, Seq(Row(2L)))
      val empty = runQueryAndCompare(s"SELECT id FROM $table WHERE id > 10") {
        boltPaimonScanNode
      }
      checkAnswer(empty, Seq.empty)
    }
  }

  test("Bolt ORC scan follows schema evolution") {
    val table = "paimon_orc_evolution"
    withTable(table) {
      spark.sql(s"CREATE TABLE $table (id INT, value STRING) USING paimon " +
        "TBLPROPERTIES ('file.format' = 'orc', 'bucket' = '-1')")
      spark.sql(s"INSERT INTO $table VALUES (1, 'one'), (2, 'two')")
      spark.sql(s"ALTER TABLE $table ADD COLUMN note STRING")
      spark.sql(s"INSERT INTO $table (id, value, note) VALUES (3, 'three', 'added')")

      val df = runQueryAndCompare(s"SELECT id, value, note FROM $table ORDER BY id") {
        boltPaimonScan
      }
      checkAnswer(
        df,
        Seq(Row(1, "one", null), Row(2, "two", null), Row(3, "three", "added")))
    }
  }

  test("Bolt Paimon falls back for metadata column reads") {
    val table = "paimon_fallback_metadata"
    withTable(table) {
      spark.sql(s"CREATE TABLE $table (id INT, value STRING) USING paimon")
      spark.sql(s"INSERT INTO $table VALUES (1, 'one'), (2, 'two')")

      val df = runJavaQueryAndCompare(
        s"SELECT id, __paimon_row_index FROM $table ORDER BY id")
      val rows = df.collect()
      assert(rows.map(_.getInt(0)).toSeq == Seq(1, 2))
      assert(rows.forall(!_.isNullAt(1)))
    }
  }

  test("Bolt Paimon falls back when deletion vectors are enabled") {
    val table = "paimon_fallback_deletion_vectors"
    withTable(table) {
      spark.sql(s"""
                   |CREATE TABLE $table (id INT, value STRING) USING paimon
                   |TBLPROPERTIES (
                   |  'file.format' = 'parquet',
                   |  'deletion-vectors.enabled' = 'true')
                   |""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'one'), (2, 'two')")

      val df = runJavaQueryAndCompare(s"SELECT * FROM $table ORDER BY id")
      checkAnswer(df, Seq(Row(1, "one"), Row(2, "two")))
    }
  }

  test("Bolt Paimon falls back for LOOKUP changelog tables") {
    val table = "paimon_fallback_lookup"
    withTable(table) {
      spark.sql(s"""
                   |CREATE TABLE $table (id INT, value STRING) USING paimon
                   |TBLPROPERTIES (
                   |  'primary-key' = 'id',
                   |  'bucket' = '1',
                   |  'changelog-producer' = 'lookup')
                   |""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'old'), (2, 'two')")
      spark.sql(s"INSERT INTO $table VALUES (1, 'new')")

      val df = runJavaQueryAndCompare(s"SELECT * FROM $table ORDER BY id")
      checkAnswer(df, Seq(Row(1, "new"), Row(2, "two")))
    }
  }

  test("Bolt Paimon falls back for deduplicate merge tables") {
    val table = "paimon_fallback_deduplicate"
    withTable(table) {
      spark.sql(s"""
                   |CREATE TABLE $table (id INT, value STRING) USING paimon
                   |TBLPROPERTIES (
                   |  'primary-key' = 'id',
                   |  'bucket' = '1',
                   |  'merge-engine' = 'deduplicate')
                   |""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 'old'), (2, 'two')")
      spark.sql(s"INSERT INTO $table VALUES (1, 'new')")

      val df = runJavaQueryAndCompare(s"SELECT * FROM $table ORDER BY id")
      checkAnswer(df, Seq(Row(1, "new"), Row(2, "two")))
    }
  }

  test("Bolt Paimon falls back for partial-update merge tables") {
    val table = "paimon_fallback_partial_update"
    withTable(table) {
      spark.sql(s"""
                   |CREATE TABLE $table (id INT, left_value INT, right_value INT) USING paimon
                   |TBLPROPERTIES (
                   |  'primary-key' = 'id',
                   |  'bucket' = '1',
                   |  'merge-engine' = 'partial-update')
                   |""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 10, NULL)")
      spark.sql(s"INSERT INTO $table VALUES (1, NULL, 20)")

      val df = runJavaQueryAndCompare(s"SELECT * FROM $table ORDER BY id")
      checkAnswer(df, Seq(Row(1, 10, 20)))
    }
  }

  test("Bolt Paimon falls back for aggregation merge tables") {
    val table = "paimon_fallback_aggregation"
    withTable(table) {
      spark.sql(s"""
                   |CREATE TABLE $table (id INT, total BIGINT) USING paimon
                   |TBLPROPERTIES (
                   |  'primary-key' = 'id',
                   |  'bucket' = '1',
                   |  'merge-engine' = 'aggregation',
                   |  'fields.total.aggregate-function' = 'sum')
                   |""".stripMargin)
      spark.sql(s"INSERT INTO $table VALUES (1, 10), (2, 7)")
      spark.sql(s"INSERT INTO $table VALUES (1, 20)")

      val df = runJavaQueryAndCompare(s"SELECT * FROM $table ORDER BY id")
      checkAnswer(df, Seq(Row(1, 30L), Row(2, 7L)))
    }
  }

  test("Bolt Paimon falls back for chain tables") {
    val table = "paimon_fallback_chain"
    withTable(table) {
      spark.sql(s"""
                   |CREATE TABLE $table (id INT, seq INT, value STRING, dt STRING) USING paimon
                   |PARTITIONED BY (dt)
                   |TBLPROPERTIES (
                   |  'primary-key' = 'id,dt',
                   |  'bucket' = '1',
                   |  'sequence.field' = 'seq',
                   |  'scan.fallback-snapshot-branch' = 'chain_snapshot',
                   |  'scan.fallback-delta-branch' = 'chain_delta',
                   |  'partition.timestamp-pattern' = '$$dt',
                   |  'partition.timestamp-formatter' = 'yyyy-MM-dd')
                   |""".stripMargin)
      spark.sql(s"CALL sys.create_branch(table => '$table', branch => 'chain_snapshot')")
      spark.sql(s"CALL sys.create_branch(table => '$table', branch => 'chain_delta')")
      spark.sql(s"ALTER TABLE $table SET TBLPROPERTIES ('chain-table.enabled' = 'true')")
      spark.sql(s"INSERT INTO $table VALUES (1, 1, 'one', '2026-08-31')")
      spark.sql(s"INSERT INTO $table VALUES (2, 1, 'two', '2026-09-01')")

      val df = runJavaQueryAndCompare(s"SELECT id, value, dt FROM $table ORDER BY id")
      checkAnswer(df, Seq(Row(1, "one", "2026-08-31"), Row(2, "two", "2026-09-01")))
    }
  }

  test("paimon transformer fallback: primary key table after full compact") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true',
                   | 'merge-engine' = 'partial-update'
                   |)
                   |""".stripMargin)

      import testImplicits._

      val randomData = (1 to 1000).map {
        _ =>
          val a = Random.nextInt(100) + 1
          val b = "b" + (Random.nextInt(90) + 10)

          (a, b)
      }

      randomData.toDF("a", "b").createOrReplaceTempView("source")
      spark.sql(s"""
                   |insert into $tbl_name select * from source
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name select * from source
                   |""".stripMargin)

      runJavaQueryAndCompare(s"""
                                |select * from $tbl_name;
                                |""".stripMargin)

      // Compaction does not move primary-key tables into the append-only native whitelist.
      spark.sql(s"CALL sys.compact(table => '$tbl_name')")
      runJavaQueryAndCompare(s"""
                                |select * from $tbl_name;
                                |""".stripMargin)
    }
  }

  test("paimon filter push down: value filter") {
    Seq(true, false).foreach {
      ignore =>
        val tbl_name = s"paimon_tb"

        withTable("paimon_tb") {
          spark.sql(s"""
                       |create table $tbl_name (id INT, seq INT, name STRING)
                       | using paimon
                       |TBLPROPERTIES (
                       | 'primary-key' = 'id',
                       | 'bucket' = '3',
                       | 'sequence.field' = 'seq',
                       | 'target-file-size' = '100b',
                       | 'write-only' = 'true',
                       | 'ignore-delete' = '$ignore'
                       |)
                       |""".stripMargin)

          spark.sql(s"""
                       |insert into $tbl_name values(1, 2, '1')
                       |""".stripMargin)
          spark.sql(s"""
                       |insert into $tbl_name values(1, 1, '2')
                       |""".stripMargin)

          runJavaQueryAndCompare(s"""
                                    |select * from $tbl_name where id = 1;
                                    |""".stripMargin)

          runJavaQueryAndCompare(s"""
                                    |select * from $tbl_name where name = '2';
                                    |""".stripMargin)
        }
    }
  }

  test("paimon transformer exists: primary key with sequence field") {
    val tbl_name = s"paimon_tb"
    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, seq INT, name STRING)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   | 'sequence.field' = 'seq',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true'
                   |)
                   |""".stripMargin)
      val randomData1 = (1 to 1000).map {
        _ =>
          val a = Random.nextInt(100) + 1
          // inject some null sequence number
          val b = if (a % 3 == 0) {
            None
          } else {
            Some(Random.nextInt(10))
          }
          val c = "c" + (Random.nextInt(90) + 10)
          (a, b, c)
      }
      val randomData2 = (1 to 1000).map {
        _ =>
          val a = Random.nextInt(100) + 1
          // inject some other null in sequence field
          val b = if (a % 3 == 0 || a % 4 == 0) {
            None
          } else {
            Some(Random.nextInt(10))
          }
          val c = "c" + (Random.nextInt(90) + 10)
          (a, b, c)
      }
      import testImplicits._
      randomData1.toDF("a", "b", "c").createOrReplaceTempView("source1")
      randomData2.toDF("a", "b", "c").createOrReplaceTempView("source2")
      spark.sql(s"""
                   |insert into $tbl_name select * from source1
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name select * from source2
                   |""".stripMargin)
      spark.sql(s"select * from $tbl_name")
      runJavaQueryAndCompare(s"""
                                |select * from $tbl_name;
                                |""".stripMargin)
    }
  }

  test("paimon transformer fallback: primary key table - custom single sequence key") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, val INT, seq INT)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true',
                   | 'sequence.field' = 'seq'
                   |)
                   |""".stripMargin)

      import testImplicits._

      val firstBatch = (1 to 1000).map {
        x =>
          val a = x
          val b = x + 1000
          val c = x
          (a, b, c)
      }

      firstBatch.toDF("a", "b", "c").createOrReplaceTempView("firstBatch")
      spark.sql(s"""
                   |insert into $tbl_name select * from firstBatch
                   |""".stripMargin)

      val secondBatch = (2000 to 3000).map {
        x =>
          val a = x
          val b = x + 1000
          val c = x
          (a, b, c)
      }

      secondBatch.toDF("a", "b", "c").createOrReplaceTempView("secondBatch")
      spark.sql(s"""
                   |insert into $tbl_name select * from secondBatch
                   |""".stripMargin)

      runJavaQueryAndCompare(s"select * from $tbl_name")
    }
  }

  test("paimon transformer fallback: primary key table - custom multiple sequence key") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, val INT, seq1 INT, seq2 INT)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true',
                   | 'sequence.field' = 'seq1,seq2'
                   |)
                   |""".stripMargin)

      import testImplicits._

      val firstBatch = (1 to 1000).map {
        x =>
          val a = x
          val b = x + 1000
          val c = x
          val d = 2 * x
          (a, b, c, d)
      }

      firstBatch.toDF("a", "b", "c", "d").createOrReplaceTempView("firstBatch")
      spark.sql(s"""
                   |insert into $tbl_name select * from firstBatch
                   |""".stripMargin)

      val secondBatch = (2000 to 3000).map {
        x =>
          val a = x
          val b = x + 1000
          val c = x
          val d = 3000 - x
          (a, b, c, d)
      }

      secondBatch.toDF("a", "b", "c", "d").createOrReplaceTempView("secondBatch")
      spark.sql(s"""
                   |insert into $tbl_name select * from secondBatch
                   |""".stripMargin)

      runJavaQueryAndCompare(s"select * from $tbl_name")
    }
  }

  ignore("paimon transformer fallback: primary key table with aggregate engine") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true',
                   | 'merge-engine' = 'aggregation',
                   | 'fields.name.aggregate-function' = 'first_value'
                   |)
                   |""".stripMargin)

      import testImplicits._

      val randomData = (1 to 1000).map {
        _ =>
          val a = Random.nextInt(100) + 1
          val b = "b" + (Random.nextInt(90) + 10)

          (a, b)
      }

      randomData.toDF("a", "b").createOrReplaceTempView("source")
      spark.sql(s"""
                   |insert into $tbl_name select * from source
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name select * from source
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false)(assertJavaBatchScan)
    }
  }

  test("paimon transformer fallback: primary key table with partial-update engine") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name INT, g INT)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true',
                   | 'merge-engine' = 'partial-update',
                   | 'fields.g.sequence-group' = 'name'
                   |)
                   |""".stripMargin)

      import testImplicits._

      val randomData = (1 to 1000).map {
        _ =>
          val a = Random.nextInt(100) + 1
          val b = Random.nextInt(90) + 10
          val c = Random.nextInt(90) + 100
          (a, b, c)
      }

      randomData.toDF("a", "b", "c").createOrReplaceTempView("source")
      spark.sql(s"""
                   |insert into $tbl_name select * from source
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name select * from source
                   |""".stripMargin)

      runJavaQueryAndCompare(s"""
                                |select name from $tbl_name;
                                |""".stripMargin)
    }
  }

  test("paimon transformer: primary key table - NULL + NOT NULL") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, val INT)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true'
                   |)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values (1, NULL)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values (1, 2)
                   |""".stripMargin)

      runJavaQueryAndCompare(s"select * from $tbl_name")
    }
  }

  test("paimon transformer: primary key table - NOT NULL + NULL") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, val INT)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true'
                   |)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values (1, 3)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values (1, NULL)
                   |""".stripMargin)

      runJavaQueryAndCompare(s"select * from $tbl_name")
    }
  }

  test("paimon transformer: primary key table - NOT NULL + NOT NULL") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, val INT)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true'
                   |)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values (1, NULL)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values (1, NULL)
                   |""".stripMargin)

      runJavaQueryAndCompare(s"select * from $tbl_name")
    }
  }

  test("paimon transformer: primary key table - custom row kind") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, val INT, kind String)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true',
                   | 'rowkind.field' = 'kind'
                   |)
                   |""".stripMargin)

      import testImplicits._

      val firstBatch = (1 to 1000).map {
        x =>
          val a = x
          val b = x + 1000
          val c = "+I"
          (a, b, c)
      }

      firstBatch.toDF("a", "b", "c").createOrReplaceTempView("firstBatch")
      spark.sql(s"""
                   |insert into $tbl_name select * from firstBatch
                   |""".stripMargin)

      val secondBatch = (2000 to 3000).map {
        x =>
          val a = x
          val b = x + 1000
          val c = "+I"
          (a, b, c)
      }

      secondBatch.toDF("a", "b", "c").createOrReplaceTempView("secondBatch")
      spark.sql(s"""
                   |insert into $tbl_name select * from secondBatch
                   |""".stripMargin)

      runJavaQueryAndCompare(s"select * from $tbl_name")
    }
  }

  test("paimon aggregate: sum") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT, sint SMALLINT, iint INTEGER,
                   |                         bint BIGINT, f FLOAT, d DOUBLE,
                   |                        sdec DECIMAL(10, 2), ldec DECIMAL(38, 9))
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'sum',
                   |'fields.sint.aggregate-function' = 'sum',
                   |'fields.iint.aggregate-function' = 'sum',
                   |'fields.bint.aggregate-function' = 'sum',
                   |'fields.f.aggregate-function' = 'sum',
                   |'fields.d.aggregate-function' = 'sum',
                   |'fields.sdec.aggregate-function' = 'sum',
                   |'fields.ldec.aggregate-function' = 'sum')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values(1,  3,  5,  7,  9, 3.5, 5.5, 7.5, 8.1),
                   |                            (2, 11, 13, 15, 17, 5.6, 7.6, 6.7, 9.1),
                   |                            (5, 19, 21, 23, 25, 9.3, 11.3, 7.8, 10.1)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values(1,  2,  4,  6,  8, 2.5, 3.5, 1.2, 11.1),
                   |                            (2, 10, 12, 14, 16, 6.7, 8.9, 2.3, 12.1),
                   |                            (7, 18, 20, 22, 24, 1.3, 7.4, 3.4, 13.1)
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false) {
        assertJavaBatchScan
      }
    }
  }

  test("paimon aggregate empty: sum") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT, sint SMALLINT, iint INTEGER,
                   |                         bint BIGINT, f FLOAT, d DOUBLE,
                   |                        sdec DECIMAL(10, 2), ldec DECIMAL(38, 9))
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'sum',
                   |'fields.sint.aggregate-function' = 'sum',
                   |'fields.iint.aggregate-function' = 'sum',
                   |'fields.bint.aggregate-function' = 'sum',
                   |'fields.f.aggregate-function' = 'sum',
                   |'fields.d.aggregate-function' = 'sum',
                   |'fields.sdec.aggregate-function' = 'sum',
                   |'fields.ldec.aggregate-function' = 'sum')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false) {
        assertJavaBatchScan
      }
    }
  }

  test("paimon aggregate empty: product") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT, sint SMALLINT, iint INTEGER,
                   |                        bint BIGINT, f FLOAT, d DOUBLE, ds DECIMAL(10,2),
                   |                        dl DECIMAL(38,12))
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'product',
                   |'fields.sint.aggregate-function' = 'product',
                   |'fields.iint.aggregate-function' = 'product',
                   |'fields.bint.aggregate-function' = 'product',
                   |'fields.f.aggregate-function' = 'product',
                   |'fields.ds.aggregate-function' = 'product',
                   |'fields.dl.aggregate-function' = 'product')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false) {
        assertJavaBatchScan
      }
    }
  }

  test("paimon aggregate: product") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT, sint SMALLINT, iint INTEGER,
                   |                        bint BIGINT, f FLOAT, d DOUBLE, ds DECIMAL(10,2),
                   |                        dl DECIMAL(38,12))
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'product',
                   |'fields.sint.aggregate-function' = 'product',
                   |'fields.iint.aggregate-function' = 'product',
                   |'fields.bint.aggregate-function' = 'product',
                   |'fields.f.aggregate-function' = 'product',
                   |'fields.ds.aggregate-function' = 'product',
                   |'fields.dl.aggregate-function' = 'product')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values(1,  3,  5,  7,  9, 3.5, 5.5, 2.0, 3.1),
                   |                            (2, 11, 13, 15, 17, 5.6, 7.6, 4.5, 4.2),
                   |                            (5, 19, 21, 23, 25, 9.3, 11.3, 3.5, 5.3)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values(1,  2,  4,  6,  8, 2.5, 3.5, 1.0, 6.4),
                   |                            (2, 10, 12, 14, 16, 6.7, 8.9, 3.4, 7.5),
                   |                            (7, 18, 20, 22, 24, 1.3, 7.4, 4.3, 8.6)
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false) {
        assertJavaBatchScan
      }
    }
  }

  // bug https://meego.larkoffice.com/dpus_queryengine_dataapplication/issue/detail/6762856624
  ignore("paimon aggregate: count") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, iint INTEGER, bint BIGINT)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.iint.aggregate-function' = 'count',
                   |'fields.bint.aggregate-function' = 'count')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values(1,  3,  5),
                   |                            (2, 11, 13),
                   |                            (5, 19, 21)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values(1,  2,  4),
                   |                            (2, 10, 12),
                   |                            (7, 18, 20)
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false) {
        assertJavaBatchScan
      }
    }
  }

  test("paimon aggregate: max") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT,
                   |                        sint SMALLINT, iint INTEGER, bint BIGINT,
                   |                        f FLOAT, d DOUBLE, c CHAR(2), v VARCHAR(10),
                   |                        s STRING, da DATE)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'max',
                   |'fields.sint.aggregate-function' = 'max',
                   |'fields.iint.aggregate-function' = 'max',
                   |'fields.bint.aggregate-function' = 'max',
                   |'fields.f.aggregate-function' = 'max',
                   |'fields.d.aggregate-function' = 'max',
                   |'fields.c.aggregate-function' = 'max',
                   |'fields.v.aggregate-function' = 'max',
                   |'fields.s.aggregate-function' = 'max',
                   |'fields.da.aggregate-function' = 'max')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  3,  5,  7,  9, 3.5, 5.5, "AB", "Hello", "Hello", DATE '2025-09-15'),
                   | (2, 11, 13, 15, 17, 5.6, 7.6, "CD", "World", "World", DATE '2024-01-01'),
                   | (5, 19, 21, 23, 25, 9.3, 11.3, "EF", "String", "String", DATE '2023-07-04')
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  2,  4,  6,  8, 2.5, 3.5, 'ab', "New", "New", DATE '2022-12-31'),
                   | (2, 10, 12, 14, 16, 6.7, 8.9, 'cd', "olleH", "olleH", DATE '2000-02-29'),
                   | (7, 18, 20, 22, 24, 1.3, 7.4, 'ef', "dlroW", "dlroW", DATE '1970-01-01')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false) {
        assertJavaBatchScan
      }
    }
  }

  test("paimon aggregate: min") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT,
                   |                        sint SMALLINT, iint INTEGER, bint BIGINT,
                   |                        f FLOAT, d DOUBLE, c CHAR(2), v VARCHAR(10),
                   |                        s STRING, da DATE)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'min',
                   |'fields.sint.aggregate-function' = 'min',
                   |'fields.iint.aggregate-function' = 'min',
                   |'fields.bint.aggregate-function' = 'min',
                   |'fields.f.aggregate-function' = 'min',
                   |'fields.d.aggregate-function' = 'min',
                   |'fields.c.aggregate-function' = 'min',
                   |'fields.v.aggregate-function' = 'min',
                   |'fields.s.aggregate-function' = 'min',
                   |'fields.da.aggregate-function' = 'min')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  3,  5,  7,  9, 3.5, 5.5, "AB", "Hello", "Hello", DATE '2025-09-15'),
                   | (2, 11, 13, 15, 17, 5.6, 7.6, "CD", "World", "World", DATE '2024-01-01'),
                   | (5, 19, 21, 23, 25, 9.3, 11.3, "EF", "String", "String", DATE '2023-07-04')
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  2,  4,  6,  8, 2.5, 3.5, 'ab', "New", "New", DATE '2022-12-31'),
                   | (2, 10, 12, 14, 16, 6.7, 8.9, 'cd', "olleH", "olleH", DATE '2000-02-29'),
                   | (7, 18, 20, 22, 24, 1.3, 7.4, 'ef', "dlroW", "dlroW", DATE '1970-01-01')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false) {
        assertJavaBatchScan
      }
    }
  }

  test("paimon aggregate: last_value") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT,
                   |                        sint SMALLINT, iint INTEGER, bint BIGINT,
                   |                        f FLOAT, d DOUBLE, c CHAR(2), v VARCHAR(10),
                   |                        s STRING, da DATE)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'last_value',
                   |'fields.sint.aggregate-function' = 'last_value',
                   |'fields.iint.aggregate-function' = 'last_value',
                   |'fields.bint.aggregate-function' = 'last_value',
                   |'fields.f.aggregate-function' = 'last_value',
                   |'fields.d.aggregate-function' = 'last_value',
                   |'fields.c.aggregate-function' = 'last_value',
                   |'fields.v.aggregate-function' = 'last_value',
                   |'fields.s.aggregate-function' = 'last_value',
                   |'fields.da.aggregate-function' = 'last_value')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  3,  5,  7,  9, 3.5, 5.5, "AB", "Hello", "Hello", DATE '2025-09-15'),
                   | (2, 11, 13, 15, 17, 5.6, 7.6, "CD", "World", "World", DATE '2024-01-01'),
                   | (5, 19, 21, 23, 25, 9.3, 11.3, "EF", "String", "String", DATE '2023-07-04')
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  2,  4,  6,  8, 2.5, 3.5, 'ab', "New", "New", DATE '2022-12-31'),
                   | (2, 10, 12, 14, 16, 6.7, 8.9, 'cd', "olleH", "olleH", DATE '2000-02-29'),
                   | (7, 18, 20, 22, 24, 1.3, 7.4, 'ef', "dlroW", "dlroW", DATE '1970-01-01')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false) {
        assertJavaBatchScan
      }
    }
  }

  test("paimon aggregate: last_non_null_value") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT,
                   |                        sint SMALLINT, iint INTEGER, bint BIGINT,
                   |                        f FLOAT, d DOUBLE, c CHAR(2), v VARCHAR(10),
                   |                        s STRING, da DATE)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'last_non_null_value',
                   |'fields.sint.aggregate-function' = 'last_non_null_value',
                   |'fields.iint.aggregate-function' = 'last_non_null_value',
                   |'fields.bint.aggregate-function' = 'last_non_null_value',
                   |'fields.f.aggregate-function' = 'last_non_null_value',
                   |'fields.d.aggregate-function' = 'last_non_null_value',
                   |'fields.c.aggregate-function' = 'last_non_null_value',
                   |'fields.v.aggregate-function' = 'last_non_null_value',
                   |'fields.s.aggregate-function' = 'last_non_null_value',
                   |'fields.da.aggregate-function' = 'last_non_null_value')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  3,  5,  7,  9, 3.5, 5.5, "AB", "Hello", "Hello", DATE '2025-09-15'),
                   | (2, 11, 13, 15, 17, 5.6, 7.6, "CD", "World", "World", DATE '2024-01-01'),
                   | (5, 19, 21, 23, 25, 9.3, 11.3, "EF", "String", "String", DATE '2023-07-04')
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  2,  4,  6,  8, 2.5, 3.5, 'ab', "New", "New", DATE '2022-12-31'),
                   | (2, 10, 12, 14, 16, 6.7, 8.9, 'cd', "olleH", "olleH", DATE '2000-02-29'),
                   | (7, 18, 20, 22, 24, 1.3, 7.4, 'ef', "dlroW", "dlroW", DATE '1970-01-01')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false) {
        assertJavaBatchScan
      }
    }
  }

  test("paimon aggregate: listagg") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, s STRING)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.s.aggregate-function' = 'listagg')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values(1, "Hello "),
                   |                            (2, NULL),
                   |                            (3, "New"),
                   |                            (4, ""),
                   |                            (5, "Good"),
                   |                            (6, "")
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values(1, "World"),
                   |                            (2, " are "),
                   |                            (3, NULL),
                   |                            (6, NULL),
                   |                            (7, NULL)
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false) {
        assertJavaBatchScan
      }
    }
  }

  test("paimon aggregate: bool_and") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, b BOOLEAN)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.b.aggregate-function' = 'bool_and')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values(1, TRUE),
                   |                            (2, TRUE),
                   |                            (3, TRUE),
                   |                            (4, TRUE),
                   |                            (5, FALSE),
                   |                            (6, FALSE),
                   |                            (7, FALSE),
                   |                            (8, FALSE),
                   |                            (9, NULL),
                   |                            (10,NULL),
                   |                            (11,NULL),
                   |                            (12,NULL)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values(1, TRUE),
                   |                            (2, FALSE),
                   |                            (3, NULL),
                   |                            (5, TRUE),
                   |                            (6, FALSE),
                   |                            (7, NULL),
                   |                            (9, TRUE),
                   |                            (10,FALSE),
                   |                            (11,NULL),
                   |                            (13, TRUE),
                   |                            (14,FALSE),
                   |                            (15,NULL)
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false) {
        assertJavaBatchScan
      }
    }
  }

  test("paimon aggregate: bool_or") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, b BOOLEAN)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.b.aggregate-function' = 'bool_or')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values(1, TRUE),
                   |                            (2, TRUE),
                   |                            (3, TRUE),
                   |                            (4, TRUE),
                   |                            (5, FALSE),
                   |                            (6, FALSE),
                   |                            (7, FALSE),
                   |                            (8, FALSE),
                   |                            (9, NULL),
                   |                            (10,NULL),
                   |                            (11,NULL),
                   |                            (12,NULL)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values(1, TRUE),
                   |                            (2, FALSE),
                   |                            (3, NULL),
                   |                            (5, TRUE),
                   |                            (6, FALSE),
                   |                            (7, NULL),
                   |                            (9, TRUE),
                   |                            (10,FALSE),
                   |                            (11,NULL),
                   |                            (13, TRUE),
                   |                            (14,FALSE),
                   |                            (15,NULL)
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false) {
        assertJavaBatchScan
      }
    }
  }

  test("paimon aggregate: first_value") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT,
                   |                        sint SMALLINT, iint INTEGER, bint BIGINT,
                   |                        f FLOAT, d DOUBLE, c CHAR(2), v VARCHAR(10),
                   |                        s STRING, da DATE)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'first_value',
                   |'fields.sint.aggregate-function' = 'first_value',
                   |'fields.iint.aggregate-function' = 'first_value',
                   |'fields.bint.aggregate-function' = 'first_value',
                   |'fields.f.aggregate-function' = 'first_value',
                   |'fields.d.aggregate-function' = 'first_value',
                   |'fields.c.aggregate-function' = 'first_value',
                   |'fields.v.aggregate-function' = 'first_value',
                   |'fields.s.aggregate-function' = 'first_value',
                   |'fields.da.aggregate-function' = 'first_value')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  3,  5,  7,  9, 3.5, 5.5, "AB", "Hello", "Hello", DATE '2025-09-15'),
                   | (2, 11, 13, 15, 17, 5.6, 7.6, "CD", "World", "World", DATE '2024-01-01'),
                   | (5, 19, 21, 23, 25, 9.3, 11.3, "EF", "String", "String", DATE '2023-07-04')
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  2,  4,  6,  8, 2.5, 3.5, 'ab', "New", "New", DATE '2022-12-31'),
                   | (2, 10, 12, 14, 16, 6.7, 8.9, 'cd', "olleH", "olleH", DATE '2000-02-29'),
                   | (7, 18, 20, 22, 24, 1.3, 7.4, 'ef', "dlroW", "dlroW", DATE '1970-01-01')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false) {
        assertJavaBatchScan
      }
    }
  }

  test("paimon aggregate: first_non_null_value") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, tint TINYINT,
                   |                        sint SMALLINT, iint INTEGER, bint BIGINT,
                   |                        f FLOAT, d DOUBLE, c CHAR(2), v VARCHAR(10),
                   |                        s STRING, da DATE)
                   |using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'primary-key' = 'id',
                   | 'bucket' = '3',
                   |'merge-engine' = 'aggregation',
                   |'fields.tint.aggregate-function' = 'first_non_null_value',
                   |'fields.sint.aggregate-function' = 'first_non_null_value',
                   |'fields.iint.aggregate-function' = 'first_non_null_value',
                   |'fields.bint.aggregate-function' = 'first_non_null_value',
                   |'fields.f.aggregate-function' = 'first_non_null_value',
                   |'fields.d.aggregate-function' = 'first_non_null_value',
                   |'fields.c.aggregate-function' = 'first_non_null_value',
                   |'fields.v.aggregate-function' = 'first_non_null_value',
                   |'fields.s.aggregate-function' = 'first_non_null_value',
                   |'fields.da.aggregate-function' = 'first_non_null_value')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  3,  5,  7,  9, 3.5, 5.5, "AB", "Hello", "Hello", DATE '2025-09-15'),
                   | (2, 11, 13, 15, 17, 5.6, 7.6, "CD", "World", "World", DATE '2024-01-01'),
                   | (5, 19, 21, 23, 25, 9.3, 11.3, "EF", "String", "String", DATE '2023-07-04')
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name values
                   | (1,  2,  4,  6,  8, 2.5, 3.5, 'ab', "New", "New", DATE '2022-12-31'),
                   | (2, 10, 12, 14, 16, 6.7, 8.9, 'cd', "olleH", "olleH", DATE '2000-02-29'),
                   | (7, 18, 20, 22, 24, 1.3, 7.4, 'ef', "dlroW", "dlroW", DATE '1970-01-01')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false) {
        assertJavaBatchScan
      }
    }
  }

  // Looks Java paimon has bug:
  // https://meego.larkoffice.com/dpus_queryengine_dataapplication/issue/detail/6781551769
  ignore("paimon partial-update: Single fields in group") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (k   INT,
                   |    a   INT,
                   |    b   INT,
                   |    g_1 INT,
                   |    c   INT,
                   |    d   INT,
                   |    g_2 INT)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'k',
                   | 'merge-engine' = 'partial-update',
                   | 'fields.g_1.sequence-group' = 'a,b',
                   | 'fields.g_2.sequence-group' = 'c,d'
                   |)
                   |""".stripMargin)

      spark.sql(s"""
                   |insert into $tbl_name
                   |values (1, 1, 1, 1, 1, 1, 1);
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name
                   |values (1, 2, 2, 2, 2, 2, CAST(NULL AS INT));
                   |""".stripMargin)

      runJavaQueryAndCompare(s"""
                                |select d from $tbl_name;
                                |""".stripMargin)
    }
  }

  test(s"deduplicate engine- select less") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING, hh STRING)
                 |-- TBLPROPERTIES ('primary-key' = 'id,dt,hh', 'merge-engine' = 'deduplicate',
                 |-- 'bucket' = '4')
                 |TBLPROPERTIES ('bucket-key' = 'id', 'bucket' = '4')
                 |PARTITIONED BY (dt, hh)
                 |""".stripMargin)

    spark.sql("""
                |INSERT INTO T VALUES
                |(1, 'a', '2023-10-01', '12'),
                |(2, 'b', '2023-10-01', '12'),
                |(3, 'c', '2023-10-02', '12'),
                |(4, 'd', '2023-10-02', '13'),
                |(5, 'e', '2023-10-02', '14'),
                |(6, 'f', '2023-10-02', '15')
    """.stripMargin)

    runQueryAndCompare("SELECT name FROM T") {
      boltPaimonScan
    }
  }

  test(s"test delete is drop partition") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING, hh STRING)
                 |-- TBLPROPERTIES ('primary-key' = 'id,dt,hh', 'merge-engine' = 'deduplicate',
                 |-- 'bucket' = '4')
                 |TBLPROPERTIES ('bucket-key' = 'id', 'bucket' = '4')
                 |PARTITIONED BY (dt, hh)
                 |""".stripMargin)

    spark.sql("""
                |INSERT INTO T VALUES
                |(1, 'a', '2023-10-01', '12'),
                |(2, 'b', '2023-10-01', '12'),
                |(3, 'c', '2023-10-02', '12'),
                |(4, 'd', '2023-10-02', '13'),
                |(5, 'e', '2023-10-02', '14'),
                |(6, 'f', '2023-10-02', '15')
    """.stripMargin)

    // delete isn't drop partition
    spark.sql("DELETE FROM T WHERE name = 'a' and hh = '12'")
    runQueryAndCompare("SELECT * FROM T ORDER BY id") {
      boltPaimonScan
    }
  }

  test("paimon first row supports transform") {
    val tbl_name = s"paimon_tb"

    withTable("paimon_tb") {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING)
                   | using paimon
                   |TBLPROPERTIES (
                   | 'primary-key' = 'id',
                   | 'bucket' = '1',
                   | 'target-file-size' = '100b',
                   | 'write-only' = 'true',
                   | 'merge-engine' = 'first-row'
                   |)
                   |""".stripMargin)

      import testImplicits._

      val randomData = (1 to 1000).map {
        _ =>
          val a = Random.nextInt(100) + 1
          val b = "b" + (Random.nextInt(90) + 10)

          (a, b)
      }

      randomData.toDF("a", "b").createOrReplaceTempView("source")
      spark.sql(s"""
                   |insert into $tbl_name select * from source
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name select * from source
                   |""".stripMargin)

      runJavaQueryAndCompare(s"""
                                |select * from $tbl_name;
                                |""".stripMargin)
    }

  }

  test(s"insert overwrite date type partition table") {
    spark.sql(s"""
                 |CREATE TABLE T (
                 |  id STRING,
                 |  dt date)
                 | PARTITIONED BY (dt)
                 |TBLPROPERTIES (
                 |  'primary-key' = 'id,dt',
                 |  'bucket' = '3'
                 |);
                 |""".stripMargin)

    spark.sql("INSERT OVERWRITE T partition (dt='2024-04-18') values(1)")

    runJavaQueryAndCompare(s"""
                              |SELECT * FROM T
                              |""".stripMargin)
  }

  // todo fix null partition
  ignore(s"insert null as partition") {
    spark.sql(s"""
                 |CREATE TABLE T (
                 |  id STRING,
                 |  dt STRING)
                 | PARTITIONED BY (dt)
                 |TBLPROPERTIES (
                 |  'bucket' = '3',
                 |  'partition.default-name'  = 'null'
                 |);
                 |""".stripMargin)

    spark.sql("INSERT OVERWRITE T values('1', null)")

    runQueryAndCompare(s"""
                          |SELECT * FROM T
                          |""".stripMargin) {
      boltPaimonScan
    }
  }

  // These cases that date/timestamp/bool is used as the partition field type are to be supported.
  test(s"insert overwrite table using different as the partition field type") {
    Seq(IntegerType, LongType, FloatType, DoubleType, DecimalType).foreach {
      dataType =>
        case class PartitionSQLAndValue(sql: Any, value: Any)

        val (ptField, sv1, sv2) = dataType match {
          case IntegerType =>
            ("INT", PartitionSQLAndValue(1, 1), PartitionSQLAndValue(2, 2))
          case LongType =>
            ("LONG", PartitionSQLAndValue(1L, 1L), PartitionSQLAndValue(2L, 2L))
          case FloatType =>
            ("FLOAT", PartitionSQLAndValue(12.3f, 12.3f), PartitionSQLAndValue(45.6f, 45.6f))
          case DoubleType =>
            ("DOUBLE", PartitionSQLAndValue(12.3d, 12.3), PartitionSQLAndValue(45.6d, 45.6))
          case DecimalType =>
            (
              "DECIMAL(5, 2)",
              PartitionSQLAndValue(11.222, 11.22),
              PartitionSQLAndValue(66.777, 66.78))
        }

        spark.sql(s"""
                     |CREATE TABLE T (a INT, b STRING, pt $ptField)
                     |PARTITIONED BY (pt)
                     |""".stripMargin)

        spark.sql(s"INSERT INTO T SELECT 1, 'a', ${sv1.sql} UNION ALL SELECT 2, 'b', ${sv2.sql}")
        checkAnswer(
          spark.sql("SELECT * FROM T ORDER BY a"),
          Row(1, "a", sv1.value) :: Row(2, "b", sv2.value) :: Nil)

        // overwrite the whole table
        spark.sql(
          s"INSERT OVERWRITE T SELECT 3, 'c', ${sv1.sql} UNION ALL SELECT 4, 'd', ${sv2.sql}")
        checkAnswer(
          spark.sql("SELECT * FROM T ORDER BY a"),
          Row(3, "c", sv1.value) :: Row(4, "d", sv2.value) :: Nil)

        // overwrite the a=1 partition
        spark.sql(s"INSERT OVERWRITE T PARTITION (pt = ${sv1.value}) VALUES (5, 'e'), (7, 'g')")
        checkAnswer(
          spark.sql("SELECT * FROM T ORDER BY a"),
          Row(4, "d", sv2.value) :: Row(5, "e", sv1.value) :: Row(7, "g", sv1.value) :: Nil)

        spark.sql("DROP TABLE T")
    }
  }

  private def validateMetadataColumnQuery(tbl_name: String, metadataColumn: String): Unit = {
    // Test different query formats
    Seq(
      s"""SELECT $metadataColumn FROM $tbl_name""",
      s"""SELECT $metadataColumn as metadataColumn FROM $tbl_name""",
      s"""SELECT *, $metadataColumn FROM $tbl_name""",
      s"""SELECT $metadataColumn, * FROM $tbl_name""",
      s"""SELECT $metadataColumn as metadataColumn, * FROM $tbl_name"""
    ).foreach {
      query => runJavaQueryAndCompare(query)
    }
  }

  test("paimon metadata columns: __paimon_row_index - empty table") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_row_index"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet'
                   |)""".stripMargin)

      validateMetadataColumnQuery(tbl_name, metadataColumn)
    }
  }

  test("paimon metadata columns: __paimon_row_index - tables with primary key definitions") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_row_index"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'primary-key' = 'id'
                   |)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (4, '4'), (3, '3'), (2, '2'), (1, '1')")

      checkAnswer(
        spark.sql(s"SELECT id, $metadataColumn FROM $tbl_name ORDER BY id"),
        Seq(Row(1, 0), Row(2, 1), Row(3, 2), Row(4, 3)))
    }
  }

  test("paimon metadata columns: __paimon_row_index - deduplicate with single insert operation") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_row_index"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'merge-engine' = 'deduplicate',
                   |'primary-key' = 'id'
                   |)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, '1'), (2, '2'), (3, '3')")

      val result = spark.sql(s"SELECT id, $metadataColumn FROM $tbl_name ORDER BY id").collect()
      assert(result.length == 3, "Expected 3 rows")
      // Verify indices are consistent and non-negative
      result.zipWithIndex.foreach {
        case (row, expectedIndex) =>
          assert(row.getLong(1) == expectedIndex, "Row index does not match")
      }
    }
  }

  test("paimon metadata columns: __paimon_row_index - MOR tables with multiple insert operations") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_row_index"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'merge-engine' = 'deduplicate',
                   |'primary-key' = 'id',
                   |'deletion-vectors.enabled' = 'true'
                   |)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, '1'), (2, '2')")
      spark.sql(s"INSERT INTO $tbl_name VALUES (3, '3'), (4, '4')")

      checkAnswer(
        spark.sql(s"SELECT id, $metadataColumn FROM $tbl_name ORDER BY id"),
        Seq(Row(1, 0), Row(2, 1), Row(3, 0), Row(4, 1)))
    }
  }

  test("paimon metadata columns: __paimon_row_index - MOR table, overwrite records") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_row_index"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'primary-key' = 'id',
                   |'merge-engine' = 'partial-update',
                   |'deletion-vectors.enabled' = 'true'
                   |)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, 'original1'), (2, 'original2')")
      // Overwrite some records
      spark.sql(s"INSERT INTO $tbl_name VALUES (1, 'updated1'), (3, 'new3')")

      // Verify final state - id=1 should have the updated value
      checkAnswer(
        spark.sql(s"SELECT id, name FROM $tbl_name ORDER BY id"),
        Row(1, "updated1") :: Row(2, "original2") :: Row(3, "new3") :: Nil)

      // Verify row indices for all records
      checkAnswer(
        spark.sql(s"SELECT id, $metadataColumn FROM $tbl_name ORDER BY id"),
        Row(1, 0) :: Row(2, 1) :: Row(3, 1) :: Nil)
    }
  }

  test("paimon metadata columns: __paimon_row_index - table with filter pushdown") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_row_index"
    // set spark conf parquet row group size to 1KiB
    spark.conf.set("spark.sql.parquet.rowGroupSize", "1024")

    withTable(tbl_name) {
      // Create table with primary key
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet'
                   |)""".stripMargin)

      val dfName = s"30k_data"
      val maxValue = 30000
      val df =
        spark.range(1, maxValue + 1).toDF("id").withColumn("name", concat(lit("name_"), col("id")))
      df.createOrReplaceTempView(dfName)

      spark.sql(s"INSERT INTO $tbl_name SELECT * FROM $dfName")

      // query the table in chunks of 1000, verify the row index is equal to id-1
      val chunks = maxValue / 1000
      for (i <- 0 until chunks) {
        val minId = i * 1000 + 1
        val maxId = (i + 1) * 1000
        checkAnswer(
          spark.sql(s"""
                       |SELECT id, $metadataColumn
                       |FROM $tbl_name
                       |WHERE id >= $minId AND id <= $maxId
                       |ORDER BY id
                       |""".stripMargin),
          (minId until maxId + 1).map(id => Row(id, id - 1)).toSeq
        )
      }
    }
    spark.conf.unset("spark.sql.parquet.rowGroupSize")
  }

  test("paimon metadata columns: __paimon_file_path - empty table") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_file_path"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet'
                   |)""".stripMargin)

      validateMetadataColumnQuery(tbl_name, metadataColumn)
    }
  }

  test("paimon metadata columns: __paimon_file_path - tables with primary key definitions") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_file_path"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'primary-key' = 'id'
                   |)""".stripMargin)

      // Insert test data
      spark.sql(s"INSERT INTO $tbl_name VALUES (1, '1'), (2, '2'), (3, '3'), (4, '4')")

      checkAnswer(
        spark.sql(
          s"SELECT COUNT(*) FROM $tbl_name WHERE $metadataColumn IS NULL OR $metadataColumn = ''"),
        Row(0))

      // Verify all file paths are absolute paths and non empty and nonnull
      val results = spark.sql(s"SELECT $metadataColumn FROM $tbl_name").collect()
      results.foreach {
        row =>
          {
            val path = row.getString(0)
            assert(path.nonEmpty, "File path should be non-empty string")
            val uri = java.net.URI.create(path)
            assert(uri.isAbsolute, "uri must not be a relative path.")
          }
      }
    }
  }

  test("paimon metadata columns: __paimon_file_path - tables with single insert operation") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_file_path"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'primary-key' = 'id'
                   |)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, '1'), (2, '2'), (3, '3')")
      checkAnswer(
        spark.sql(
          s"SELECT COUNT(*) FROM $tbl_name WHERE $metadataColumn IS NULL OR $metadataColumn = ''"),
        Row(0))

      val results = spark.sql(s"SELECT $metadataColumn FROM $tbl_name").collect()
      results.foreach {
        row =>
          {
            val path = row.getString(0)
            assert(path.nonEmpty, "File path should be non-empty string")
            val uri = java.net.URI.create(path)
            assert(uri.isAbsolute, "uri must not be a relative path.")
          }
      }
    }
  }

  test("paimon metadata columns: __paimon_file_path - tables with multiple insert operations") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_file_path"

    withTable(tbl_name) {
      // Create Merge-On-Read table
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'primary-key' = 'id',
                   |'deletion-vectors.enabled' = 'true'
                   |)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, '1'), (2, '2')")
      spark.sql(s"INSERT INTO $tbl_name VALUES (3, '3'), (4, '4')")

      checkAnswer(
        spark.sql(
          s"SELECT COUNT(*) FROM $tbl_name WHERE $metadataColumn IS NULL OR $metadataColumn = ''"),
        Row(0))

      // Deletion-vector tables may compact active rows from different commits into the same file.
      // Verify the logical rows and the metadata contract without depending on file layout.
      val results =
        runJavaQueryAndCompare(s"SELECT id, $metadataColumn FROM $tbl_name ORDER BY id").collect()
      assert(results.map(_.getInt(0)).toSeq == Seq(1, 2, 3, 4))
      results.foreach {
        row =>
          {
            val path = row.getString(1)
            assert(path.nonEmpty, "File path should be non-empty string")
            val uri = java.net.URI.create(path)
            assert(uri.isAbsolute, "uri must not be a relative path.")
          }
      }
    }
  }

  test("paimon metadata columns: __paimon_file_path - MOR table, overwrite records") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_file_path"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'primary-key' = 'id',
                   |'merge-engine' = 'partial-update',
                   |'deletion-vectors.enabled' = 'true'
                   |)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, 'original1'), (2, 'original2')")
      spark.sql(s"INSERT INTO $tbl_name VALUES (1, 'updated1'), (3, 'new3')")

      checkAnswer(
        spark.sql(
          s"SELECT COUNT(*) FROM $tbl_name WHERE $metadataColumn IS NULL OR $metadataColumn = ''"),
        Row(0))

      val results = runJavaQueryAndCompare(
        s"SELECT id, name, $metadataColumn FROM $tbl_name ORDER BY id").collect()
      assert(results.map(row => (row.getInt(0), row.getString(1))).toSeq ==
        Seq((1, "updated1"), (2, "original2"), (3, "new3")))
      results.foreach {
        row =>
          val path = row.getString(2)
          assert(path.nonEmpty, "File path should be non-empty string")
          assert(java.net.URI.create(path).isAbsolute, "uri must not be a relative path.")
      }
    }
  }

  // Test for __paimon_bucket metadata column

  test("paimon metadata columns: __paimon_bucket, simple bucketed table") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_bucket"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id BIGINT, part BIGINT) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'bucket' = '4',
                   |'bucket-key' = 'id'
                   |) PARTITIONED BY (part)""".stripMargin)
      spark.sql(s"INSERT INTO $tbl_name SELECT id, id % 2 AS part FROM range(100)")

      validateMetadataColumnQuery(tbl_name, metadataColumn)
      val schema = spark.sql(s"SELECT $metadataColumn FROM $tbl_name LIMIT 0").schema
      assert(schema.fields.length == 1)
      assert(schema.fields(0).dataType == IntegerType)
      assert(schema.fields(0).name == metadataColumn)
      checkAnswer(
        spark.sql(s"""SELECT $metadataColumn FROM $tbl_name
                     | GROUP BY $metadataColumn
                     | ORDER BY $metadataColumn""".stripMargin),
        Row(0) ::
          Row(1) ::
          Row(2) ::
          Row(3) :: Nil
      )
    }
  }

  test("paimon metadata columns: __paimon_bucket - cross-partition data consistency") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_bucket"
    val numBuckets = 4

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id BIGINT, part BIGINT) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'bucket' = '$numBuckets',
                   |'bucket-key' = 'id'
                   |) PARTITIONED BY (part)""".stripMargin)
      spark.sql(s"INSERT INTO $tbl_name SELECT id, id % 2 as part FROM range(100)")

      // For each partition, verify buckets are numbered from 0 to numBuckets-1
      val partitions = spark.sql(s"SELECT DISTINCT part FROM $tbl_name").collect()
      partitions.foreach {
        row =>
          val partition = row.getLong(0)
          val bucketIds = spark
            .sql(
              s"SELECT DISTINCT $metadataColumn FROM $tbl_name WHERE part = $partition"
            )
            .collect()
            .map(_.getInt(0))
            .sorted
          bucketIds.foreach {
            bucketId =>
              assert(
                bucketId >= 0 && bucketId < numBuckets,
                s"Invalid bucket ID: $bucketId in partition $partition")
          }
      }
    }
  }

  // Test for __paimon_partition metadata column

  test("paimon metadata columns: __paimon_partition - basic existence verification") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_partition"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id BIGINT, part BIGINT) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet'
                   |) PARTITIONED BY (part)""".stripMargin)
      spark.sql(s"INSERT INTO $tbl_name SELECT id, id % 2 AS part FROM range(10)")

      validateMetadataColumnQuery(tbl_name, metadataColumn)
    }
  }

  test("paimon metadata columns: __paimon_partition - data type validation") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_partition"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id BIGINT, part BIGINT) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet'
                   |) PARTITIONED BY (part)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, 0), (2, 0), (3, 1), (4, 1)")

      val schema = spark.sql(s"SELECT $metadataColumn FROM $tbl_name LIMIT 0").schema
      assert(schema.fields.length == 1)
      assert(
        schema.fields(0).dataType.isInstanceOf[StructType],
        s"Expected StructType for $metadataColumn, got ${schema.fields(0).dataType}")
      assert(schema.fields(0).name == metadataColumn)
      val structType = schema.fields(0).dataType.asInstanceOf[StructType]
      assert(structType.fields.length >= 1, "Partition struct should have at least one field")
      val hasPartitionField = structType.fields.exists(field => field.name == "part")
      assert(hasPartitionField, s"Partition struct does not contain field matching 'part'")
    }
  }

  test("paimon metadata columns: __paimon_partition - partition values correctness") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_partition"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id BIGINT, part BIGINT) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet'
                   |) PARTITIONED BY (part)""".stripMargin)
      spark.sql(s"INSERT INTO $tbl_name VALUES (1, 0), (2, 0), (3, 1), (4, 1)")

      // Verify partition values match actual partition values
      // __paimon_partition is a ROW type that contains the partition key-value pairs
      val results = spark.sql(s"SELECT id, part, $metadataColumn FROM $tbl_name").collect()

      results.foreach {
        row =>
          val id = row.getLong(0)
          val expectedPartitionValue = row.getLong(1)
          val partitionStruct = row.get(2).asInstanceOf[Row]

          // Check that the struct is not null
          assert(partitionStruct != null, "Partition struct should not be null")

          // Get struct fields and check for matching partition value. The field
          // name in the struct is the same as the partition column name
          val partitionFieldValue = partitionStruct.getAs[Long]("part")
          assert(partitionFieldValue == expectedPartitionValue)

      }
    }
  }

  test("paimon metadata columns: __paimon_partition - multi-level partitions") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_partition"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id BIGINT, part BIGINT, part2 BIGINT) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet'
                   |) PARTITIONED BY (part, part2)""".stripMargin)
      spark.sql(
        s"INSERT INTO $tbl_name SELECT id, id % 2 as part, id % 3 AS part2 FROM range(7)"
      )
      val results = spark.sql(s"SELECT id, part, part2, $metadataColumn FROM $tbl_name").collect()

      results.foreach {
        row =>
          val id = row.getLong(0)
          val expectedPartition1 = row.getLong(1)
          val expectedPartition2 = row.getLong(2)

          val partitionStruct = row.get(3).asInstanceOf[Row]

          assert(partitionStruct != null, "Partition struct should not be null")

          var foundPartition1 = false
          var foundPartition2 = false

          for (i <- 0 until partitionStruct.length) {
            val fieldValue = partitionStruct.get(i)
            if (fieldValue != null) {
              if (fieldValue == expectedPartition1) foundPartition1 = true
              if (fieldValue == expectedPartition2) foundPartition2 = true
            }
          }

          assert(
            foundPartition1,
            s"Partition struct doesn't contain expected value $expectedPartition1 for ID $id")
          assert(
            foundPartition2,
            s"Partition struct doesn't contain expected value $expectedPartition2 for ID $id")
      }
    }
  }

  test("paimon metadata columns: __paimon_partition - behavior under table modifications") {
    val tbl_name = s"paimon_tb"
    val metadataColumn = "__paimon_partition"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id BIGINT, part BIGINT) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet'
                   |) PARTITIONED BY (part)""".stripMargin)

      spark.sql(s"INSERT INTO $tbl_name VALUES (1, 0), (2, 0)")

      val initialResults = spark.sql(s"SELECT $metadataColumn FROM $tbl_name").collect()
      assert(initialResults.nonEmpty)

      initialResults.foreach {
        row =>
          val partitionStruct = row.get(0).asInstanceOf[Row]
          assert(partitionStruct != null, "Partition struct should not be null")
          assert(partitionStruct.length > 0, "Partition struct should have at least one field")
      }

      spark.sql(s"INSERT INTO $tbl_name VALUES (3, 1), (4, 1), (5, 0), (6, 1)")

      val updatedResults = spark.sql(s"SELECT $metadataColumn FROM $tbl_name").collect()
      assert(updatedResults.length >= initialResults.length)

      updatedResults.foreach {
        row =>
          val partitionStruct = row.get(0).asInstanceOf[Row]
          assert(partitionStruct != null, "Partition struct should not be null after update")
          assert(partitionStruct.length > 0, "Partition struct should have fields after update")
      }

      checkAnswer(
        spark.sql(s"SELECT COUNT(*) FROM $tbl_name WHERE $metadataColumn IS NULL"),
        Row(0))
    }
  }

  test("paimon metadata columns: combined usage of __paimon_bucket and __paimon_partition") {
    val tbl_name = s"paimon_tb"

    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id BIGINT, part BIGINT) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   |'bucket' = '4',
                   |'bucket-key' = 'id'
                   |) PARTITIONED BY (part)""".stripMargin)
      spark.sql(s"INSERT INTO $tbl_name VALUES (1, 0), (2, 0), (3, 1), (4, 1)")
      validateMetadataColumnQuery(tbl_name, "__paimon_bucket, __paimon_partition")

      val results =
        spark
          .sql(s"SELECT id, part, __paimon_bucket, __paimon_partition FROM $tbl_name")
          .collect()

      assert(results.length == 4, "Expected 4 rows in combined query")
      results.foreach {
        row =>
          val bucket = row.getInt(2)
          assert(bucket >= 0 && bucket < 4, s"Invalid bucket ID: $bucket")

          val partitionStruct = row.get(3).asInstanceOf[Row]
          assert(partitionStruct != null, "Partition metadata should not be null")
          assert(partitionStruct.length > 0, "Partition struct should have at least one field")

          // Verify the partition value matches the part column
          val id = row.getLong(0)
          val expectedPartitionValue = row.getLong(1) // Using actual part column value
          var foundMatchingValue = false

          for (i <- 0 until partitionStruct.length) {
            val fieldValue = partitionStruct.get(i)
            if (fieldValue != null && fieldValue == expectedPartitionValue) {
              foundMatchingValue = true
            }
          }

          assert(
            foundMatchingValue,
            s"Partition struct doesn't contain expected value $expectedPartitionValue for ID $id")
      }
    }
  }

  test("paimon metadata columns: __paimon_partition and __paimon_bucket no partition no bucket") {
    val tbl_name = s"tbl_no_partition_no_bucket"

    withTable(tbl_name) {
      spark.sql(s"""
                   |CREATE TABLE $tbl_name
                   |TBLPROPERTIES('file.format' = 'parquet')
                   |AS
                   |SELECT id, id % 4 as part
                   |FROM range(100)""".stripMargin)
      validateMetadataColumnQuery(tbl_name, "__paimon_partition, __paimon_bucket")

      // For non-partitioned table, __paimon_partition should still work
      // but with an empty struct value
      // val partitionResults =
      //   spark.sql(s"SELECT DISTINCT __paimon_partition FROM $tbl_name").collect()
      // assert(partitionResults.nonEmpty, "Should return results for __paimon_partition")
      // assert(partitionResults.length == 1, "Should return results for __paimon_partition")

      // For non-bucketed table, __paimon_bucket should still work but with default value of 0
      val bucketResults = spark.sql(s"SELECT DISTINCT __paimon_bucket FROM $tbl_name").collect()
      assert(bucketResults.nonEmpty, "Should return results for __paimon_bucket")
      assert(bucketResults.length == 1, "Should return results for __paimon_bucket")
      assert(bucketResults(0).get(0) == 0, "Should return results for __paimon_bucket")

      checkAnswer(spark.sql(s"SELECT COUNT(*) FROM $tbl_name"), Row(100))
    }
  }

  test("paimon metadata columns: __paimon_partition and __paimon_bucket no partition yes bucket") {
    val tbl_name = s"tbl_no_partition_yes_bucket"

    withTable(tbl_name) {
      spark.sql(s"""
                   |CREATE TABLE $tbl_name
                   |TBLPROPERTIES('file.format' = 'parquet', 'bucket' = '4', 'bucket-key' = 'id')
                   |AS
                   |SELECT id, id % 4 as part
                   |FROM range(100)""".stripMargin)
      validateMetadataColumnQuery(tbl_name, "__paimon_partition, __paimon_bucket")
      val bucketCounts =
        spark
          .sql(s"SELECT __paimon_bucket, COUNT(*) FROM $tbl_name GROUP BY __paimon_bucket")
          .collect()
      assert(bucketCounts.length == 4, "Should have 4 buckets as specified")

      bucketCounts.foreach {
        row =>
          val bucketId = row.getInt(0)
          assert(bucketId >= 0 && bucketId < 4, s"Invalid bucket ID: $bucketId")
      }
    }
  }

  test("paimon metadata columns: __paimon_partition and __paimon_bucket yes partition no bucket") {
    val tbl_name = s"tbl_yes_partition_no_bucket"

    withTable(tbl_name) {
      spark.sql(s"""
                   |CREATE TABLE $tbl_name
                   |PARTITIONED BY (part)
                   |TBLPROPERTIES('file.format' = 'parquet')
                   |AS
                   |SELECT id, id % 4 as part
                   |FROM range(100)""".stripMargin)
      validateMetadataColumnQuery(tbl_name, "__paimon_partition, __paimon_bucket")
      val partitionValues = spark.sql(s"SELECT DISTINCT part FROM $tbl_name").collect()
      assert(partitionValues.length == 4, "Should have 4 distinct partition values (0,1,2,3)")

      val results = spark.sql(s"SELECT part, __paimon_partition FROM $tbl_name LIMIT 10").collect()
      results.foreach {
        row =>
          val expectedPartition = row.getLong(0)
          val partitionStruct = row.get(1).asInstanceOf[Row]

          // Check partition struct contains the expected value
          var foundMatchingValue = false
          for (i <- 0 until partitionStruct.length) {
            val fieldValue = partitionStruct.get(i)
            if (fieldValue != null && fieldValue == expectedPartition) {
              foundMatchingValue = true
            }
          }

          assert(
            foundMatchingValue,
            s"Partition struct doesn't contain expected value $expectedPartition")
      }
    }
  }

  ignore("paimon metadata columns: metadata column filters") {
    val tbl_name = s"paimon_tb_meta_filters"

    withTable(tbl_name) {
      spark.sql(s"""
                   |CREATE TABLE $tbl_name
                   |PARTITIONED BY (part)
                   |TBLPROPERTIES('file.format' = 'parquet', 'bucket' = '4', 'bucket-key' = 'id')
                   |AS
                   |SELECT id, (id % 4) as part
                   |FROM range(100)""".stripMargin)
      validateMetadataColumnQuery(
        tbl_name,
        "__paimon_partition, __paimon_bucket, __paimon_row_index, __paimon_file_path")

      // verify that if we select a __paimon_row_index in the file we more than 0 records,
      // but less than 100
      val rowIndexDf = spark
        .sql(s"SELECT __paimon_row_index FROM $tbl_name WHERE __paimon_row_index = 7")
        .collect()
      assert(
        rowIndexDf.length > 0 && rowIndexDf.length < 100,
        "record count should be > 0 and < 100 for __paimon_row_index")

      // get all the file paths
      val filePaths = spark.sql(s"SELECT distinct __paimon_file_path FROM $tbl_name").collect()
      // take first file path, and run a query that filters on that path. Verify the results
      // are correct
      checkAnswer(
        spark.sql(s"""SELECT distinct __paimon_file_path FROM $tbl_name
                     |WHERE __paimon_file_path = '${filePaths(0).getString(0)}'
                     |""".stripMargin),
        Row(filePaths(0).getString(0)) :: Nil
      )

      // verify __paimon_bucket filters correctly
      checkAnswer(
        spark.sql(s"SELECT distinct __paimon_bucket FROM $tbl_name WHERE __paimon_bucket = 0"),
        Row(0) :: Nil
      )

      // verify __paimon_partition filters correctly
      checkAnswer(
        spark.sql(s"SELECT count(*) FROM $tbl_name WHERE __paimon_partition = 0"),
        Row(25) :: Nil
      )

    }
  }

  test("paimon metadata columns: __paimon_partition multiple partition keys") {
    val tbl_name = s"tbl_multi_partition"

    withTable(tbl_name) {
      spark.sql(s"""
                   |CREATE TABLE $tbl_name
                   |PARTITIONED BY (part, part2)
                   |TBLPROPERTIES('file.format' = 'parquet')
                   |AS
                   |SELECT id, id % 4 as part, id % 3 as part2
                   |FROM range(100)""".stripMargin)

      validateMetadataColumnQuery(tbl_name, "__paimon_partition")

      // Verify both partition keys are present in the __paimon_partition struct
      val results =
        spark.sql(s"SELECT part, part2, __paimon_partition FROM $tbl_name LIMIT 10").collect()
      results.foreach {
        row =>
          val expectedPartkey = row.getLong(0)
          val expectedPartkey2 = row.getLong(1)
          val partitionStruct = row.get(2).asInstanceOf[Row]

          // Check partition struct contains both expected values
          var foundPartkey = false
          var foundPartkey2 = false

          for (i <- 0 until partitionStruct.length) {
            val fieldValue = partitionStruct.get(i)
            if (fieldValue != null) {
              if (fieldValue == expectedPartkey) foundPartkey = true
              if (fieldValue == expectedPartkey2) foundPartkey2 = true
            }
          }

          assert(
            foundPartkey,
            s"Partition struct doesn't contain expected value $expectedPartkey for part")
          assert(
            foundPartkey2,
            s"Partition struct doesn't contain expected value $expectedPartkey2 for part2")
      }
    }
  }

  test("paimon metadata columns: _ROW_ID") {
    assume(
      !org.apache.spark.SPARK_VERSION_SHORT.startsWith("3.5"),
      "Spark 3.5 does not yet support _ROW_ID metadata column")
    val tbl_name = s"tbl_row_id"

    withTable(tbl_name) {
      spark.sql(s"""
                   |CREATE TABLE $tbl_name
                   |TBLPROPERTIES(
                   | 'file.format' = 'parquet',
                   | 'target-file-size' = '100b',
                   | 'row-tracking.enabled' = 'true',
                   | 'merge-engine' = 'partial-update'
                   |)
                   |AS
                   |SELECT id, id % 4 as data
                   |FROM range(100)""".stripMargin)
      validateMetadataColumnQuery(tbl_name, "_ROW_ID")
      spark.sql(s"INSERT INTO $tbl_name SELECT id, id % 4 FROM range(256, 512)")
      validateMetadataColumnQuery(tbl_name, "_ROW_ID")
    }
  }

  test("paimon metadata columns: _SEQUENCE_NUMBER") {
    assume(
      !org.apache.spark.SPARK_VERSION_SHORT.startsWith("3.5"),
      "Spark 3.5 does not yet support _SEQUENCE_NUMBER metadata column")
    val tbl_name = s"tbl_sequence_number"

    withTable(tbl_name) {
      spark.sql(s"""
                   |CREATE TABLE $tbl_name
                   |TBLPROPERTIES(
                   | 'file.format' = 'parquet',
                   | 'target-file-size' = '100b',
                   | 'row-tracking.enabled' = 'true',
                   | 'merge-engine' = 'partial-update'
                   |)
                   |AS
                   |SELECT id
                   |FROM range(100)""".stripMargin)

      validateMetadataColumnQuery(tbl_name, "_SEQUENCE_NUMBER")
      // insert into table, then read again
      spark.sql(s"INSERT INTO $tbl_name SELECT id FROM range(256, 512)")
      validateMetadataColumnQuery(tbl_name, "_SEQUENCE_NUMBER")
    }
  }
}

class DisableNativeSource extends WholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set(
        "spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.paimon.spark.SparkCatalog")
      .set("spark.sql.catalog.spark_catalog.warehouse", s"file://$rootPath/paimon-velox")
      .set("spark.gluten.paimon.native.source.enabled", "false")
  }

  test("paimon transformer exists: append table with fallback") {
    val tbl_name = s"paimon_tb"
    withTable(tbl_name) {
      spark.sql(s"""
                   |create table $tbl_name (id INT, name STRING) using paimon
                   |TBLPROPERTIES (
                   |'file.format' = 'parquet',
                   | 'bucket' = '3',
                   | 'bucket-key' = 'id')
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into $tbl_name values(1, '1')
                   |""".stripMargin)

      runQueryAndCompare(
        s"""
           |select * from $tbl_name;
           |""".stripMargin,
        noFallBack = false)(df => FallbackUtil.hasFallback(df.queryExecution.executedPlan))
    }
  }
}
