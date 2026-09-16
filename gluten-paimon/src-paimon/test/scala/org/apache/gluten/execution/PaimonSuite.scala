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

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame

abstract class PaimonSuite extends WholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  protected def fileFormatsToTest: Seq[String] = Seq("parquet", "orc")

  protected def checkPaimonScan(df: DataFrame): Unit =
    checkGlutenPlan[PaimonScanTransformer](df)

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
      .set("spark.sql.catalog.spark_catalog.warehouse", s"file://$rootPath/data-paimon")
  }

  test("paimon transformer exists") {
    fileFormatsToTest.foreach {
      format =>
        withTable(s"paimon_${format}_tbl") {
          sql(s"DROP TABLE IF EXISTS paimon_${format}_tbl")
          sql(
            s"CREATE TABLE paimon_${format}_tbl (id INT, name STRING) USING PAIMON " +
              s"TBLPROPERTIES ('file.format'='$format')")
          sql(s"INSERT INTO paimon_${format}_tbl VALUES (1, 'Bob'), (2, 'Blue'), (3, 'Mike')")
          runQueryAndCompare(s"""
                                |SELECT * FROM paimon_${format}_tbl;
                                |""".stripMargin) {
            checkPaimonScan
          }
        }
    }
  }

  test("paimon partitioned table") {
    fileFormatsToTest.foreach {
      format =>
        val table = s"paimon_${format}_partitioned_tbl"
        withTable(table) {
          sql(s"DROP TABLE IF EXISTS $table")
          sql(s"CREATE TABLE $table (id INT, p1 STRING, p2 STRING) USING paimon " +
            s"PARTITIONED BY (p1, p2) TBLPROPERTIES ('file.format'='$format')")
          sql(s"INSERT INTO $table VALUES (1, '1', '1'), (2, '1', '2')")
          runQueryAndCompare(s"SELECT p1 FROM $table WHERE p1 = '1' ORDER BY id") {
            checkPaimonScan
          }
          runQueryAndCompare(s"SELECT p2 FROM $table WHERE p1 = '1' ORDER BY id") {
            checkPaimonScan
          }
          runQueryAndCompare(s"SELECT p1 FROM $table WHERE p2 = '1'") {
            checkPaimonScan
          }
          runQueryAndCompare(s"SELECT p2 FROM $table WHERE p2 = '1'") {
            checkPaimonScan
          }
          runQueryAndCompare(s"SELECT id, p2 FROM $table WHERE p1 = '1' and p2 = '2'") {
            checkPaimonScan
          }
          runQueryAndCompare(s"SELECT id FROM $table ORDER BY id") {
            checkPaimonScan
          }
        }
    }
  }

  test("paimon transformer exists with bucket table") {
    fileFormatsToTest.foreach {
      format =>
        val table = s"paimon_${format}_bucket_tbl"
        withTable(table) {
          sql(s"""
                 |CREATE TABLE $table (id INT, name STRING)
                 |USING paimon
                 |TBLPROPERTIES (
                 | 'file.format' = '$format',
                 | 'bucket' = '1',
                 | 'bucket-key' = 'id'
                 |)
                 |""".stripMargin)
          sql(s"INSERT INTO $table VALUES (1, 'Bob'), (2, 'Blue'), (3, 'Mike')")
          runQueryAndCompare(s"SELECT * FROM $table") {
            checkPaimonScan
          }
        }
    }
  }
}
