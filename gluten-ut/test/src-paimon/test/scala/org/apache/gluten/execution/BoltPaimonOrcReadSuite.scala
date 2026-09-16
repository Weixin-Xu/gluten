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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import java.sql.Timestamp
import java.time.Instant
import java.util.TimeZone

class BoltPaimonOrcReadSuite extends WholeStageTransformerSuite {
  private val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.paimon.native.source.enabled", "true")
      .set(
        "spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
      .set("spark.sql.catalog.spark_catalog", "org.apache.paimon.spark.SparkCatalog")
      .set("spark.sql.catalog.spark_catalog.warehouse", s"file://$rootPath/data-paimon-orc-ut")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    assume(
      BackendsApiManager.getBackendName == "bolt",
      "Skipping suite: backend is not Bolt")
  }

  private def assertBoltPaimonScan(df: DataFrame): Unit = {
    val plan = getExecutedPlan(df)
    val className = "org.apache.gluten.execution.BoltPaimonScanTransformer"
    assert(plan.exists(_.getClass.getName == className), plan.toString())
  }

  private def assertJavaPaimonScan(df: DataFrame): Unit = {
    val plan = getExecutedPlan(df)
    val className = "org.apache.gluten.execution.BoltPaimonScanTransformer"
    assert(!plan.exists(_.getClass.getName == className), plan.toString())
    assert(plan.exists(_.isInstanceOf[BatchScanExec]), plan.toString())
  }

  private def withTimeZones(jvmZoneId: String, sparkZoneId: String)(f: => Unit): Unit = {
    val originalJvmTimeZone = TimeZone.getDefault
    info(
      s"timestamp time zones before alignment: JVM=${originalJvmTimeZone.getID}, " +
        s"Spark=${spark.sessionState.conf.sessionLocalTimeZone}")
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(jvmZoneId))
      withSQLConf("spark.sql.session.timeZone" -> sparkZoneId) {
        info(
          s"timestamp time zones during regression: JVM=${TimeZone.getDefault.getID}, " +
            s"Spark=${spark.sessionState.conf.sessionLocalTimeZone}")
        f
      }
    } finally {
      TimeZone.setDefault(originalJvmTimeZone)
    }
  }

  test("Bolt reads Paimon ORC rows through the native scan") {
    withTable("paimon_orc_ut") {
      sql("CREATE TABLE paimon_orc_ut (id INT, value STRING) USING paimon " +
        "TBLPROPERTIES ('file.format'='orc', 'bucket'='-1')")
      sql("INSERT INTO paimon_orc_ut VALUES (1, 'a'), (2, 'b'), (3, NULL)")

      val filtered = sql("SELECT id FROM paimon_orc_ut WHERE id >= 2 ORDER BY id")
      assert(filtered.collect().toSeq == Seq(Row(2), Row(3)))
      assertBoltPaimonScan(filtered)

      val count = sql("SELECT COUNT(*) FROM paimon_orc_ut WHERE id >= 2")
      assert(count.collect().toSeq == Seq(Row(2L)))
      assertBoltPaimonScan(count)
    }
  }

  test("ORC timestamps preserve fractional values through Java fallback") {
    withTimeZones("America/Los_Angeles", "UTC") {
      withTable("paimon_orc_seconds_ut") {
        sql("CREATE TABLE paimon_orc_seconds_ut (id INT, value STRING, ts TIMESTAMP) " +
          "USING paimon TBLPROPERTIES ('file.format'='orc', 'bucket'='-1')")
        sql("INSERT INTO paimon_orc_seconds_ut VALUES " +
          "(1, 'positive', TIMESTAMP '1970-01-01 00:00:01.123456'), " +
          "(2, 'negative', TIMESTAMP '1969-12-31 23:59:58.876544')")

        val expectedTimestamps = Seq(
          Row(1, Timestamp.from(Instant.parse("1970-01-01T00:00:01.123456Z"))),
          Row(2, Timestamp.from(Instant.parse("1969-12-31T23:59:58.876544Z"))))
        var javaBaseline = Seq.empty[Row]
        withSQLConf("spark.gluten.paimon.native.source.enabled" -> "false") {
          val df = sql("SELECT id, ts FROM paimon_orc_seconds_ut ORDER BY id")
          javaBaseline = df.collect().toSeq
          info(s"Java baseline timestamp epochs: ${javaBaseline.map(_.getTimestamp(1).getTime)}")
          assert(javaBaseline == expectedTimestamps)
          assertJavaPaimonScan(df)
        }
        assert(javaBaseline == expectedTimestamps)

        val defaultUnit = sql("SELECT id, ts FROM paimon_orc_seconds_ut ORDER BY id")
        val defaultRows = defaultUnit.collect().toSeq
        info(s"default fallback timestamp epochs: ${defaultRows.map(_.getTimestamp(1).getTime)}")
        assert(defaultRows == javaBaseline)
        assertJavaPaimonScan(defaultUnit)

        withSQLConf("spark.gluten.paimon.read.timestamp-unit" -> "0") {
          val timestamps = sql("SELECT id, ts FROM paimon_orc_seconds_ut ORDER BY id")
          val timestampRows = timestamps.collect().toSeq
          info(
            s"seconds fallback timestamp epochs: ${timestampRows.map(_.getTimestamp(1).getTime)}")
          assert(timestampRows == javaBaseline)
          assertJavaPaimonScan(timestamps)

          val noTimestamp =
            sql("SELECT id, value FROM paimon_orc_seconds_ut WHERE id >= 1 ORDER BY id")
          assert(noTimestamp.collect().toSeq == Seq(Row(1, "positive"), Row(2, "negative")))
          assertJavaPaimonScan(noTimestamp)
        }
      }
    }
  }
}
