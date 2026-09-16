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

import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.test.SharedSparkSession

class BoltPaimonSchemaEvolutionPlanningSuite extends SharedSparkSession {
  private val rootPath: String = getClass.getResource("/").getPath

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.gluten.paimon.native.source.enabled", "true")
      .set(
        "spark.sql.extensions",
        "org.apache.gluten.extension.BoltPaimonPlanningExtensions," +
          "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions"
      )
      .set("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
      .set("spark.sql.catalog.paimon.warehouse", s"file://$rootPath/data-paimon-schema-planning")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql("DROP DATABASE IF EXISTS paimon.schema_planning CASCADE")
    sql("CREATE DATABASE paimon.schema_planning")
    sql("USE paimon.schema_planning")
    sql(
      "CREATE TABLE evolved (id INT, payload STRUCT<a: INT>) USING paimon " +
        "TBLPROPERTIES ('file.format' = 'parquet')")
    sql("INSERT INTO evolved VALUES (1, STRUCT(10))")
    sql("ALTER TABLE evolved ADD COLUMN payload.b INT")
  }

  override def afterAll(): Unit = {
    try {
      sql("USE default")
      sql("DROP DATABASE IF EXISTS paimon.schema_planning CASCADE")
    } finally {
      super.afterAll()
    }
  }

  test("nested evolution keeps the original BatchScanExec and records its schema reason") {
    val query = "SELECT payload FROM evolved"
    val scan = sql(query).queryExecution.sparkPlan.collectFirst {
      case batch: BatchScanExec => batch
    }.getOrElse(fail("BatchScanExec missing before Bolt offload"))
    assert(BoltOffloadPaimonScan().offload(scan) eq scan)
    val reason = FallbackTags.get(scan).reason()
    assert(reason.contains("[Paimon Java fallback] nested type evolution"), reason)
    assert(reason.contains("field 'payload'"), reason)

    val plan = sql(query).queryExecution.executedPlan
    assert(plan.exists(_.isInstanceOf[BatchScanExec]), plan.toString())
    assert(!plan.exists(_.isInstanceOf[BoltPaimonScanTransformer]), plan.toString())
  }

  test("an unchanged projected field still uses Bolt after unrelated nested evolution") {
    val plan = sql("SELECT id FROM evolved").queryExecution.executedPlan
    assert(plan.exists(_.isInstanceOf[BoltPaimonScanTransformer]), plan.toString())
    assert(!plan.exists(_.isInstanceOf[BatchScanExec]), plan.toString())
  }

  test("nested fields used only by a filter are checked before offload") {
    val plan = sql("SELECT id FROM evolved WHERE payload.a > 0").queryExecution.executedPlan
    assert(plan.exists(_.isInstanceOf[BatchScanExec]), plan.toString())
    assert(!plan.exists(_.isInstanceOf[BoltPaimonScanTransformer]), plan.toString())
  }

  test("empty projection checks evolved fields even with pushed pruning filters") {
    val query = "SELECT id FROM evolved WHERE id > 0"
    val scan = sql(query).queryExecution.sparkPlan.collectFirst {
      case batch: BatchScanExec => batch
    }.getOrElse(fail("BatchScanExec missing before Bolt offload"))
    val paimonScan = scan.scan.asInstanceOf[org.apache.paimon.spark.PaimonScan]
    // Construct the empty projection explicitly: Spark versions can retain filter columns.
    val emptyProjection = scan.copy(
      output = Seq.empty,
      scan = paimonScan.copy(requiredSchema = org.apache.spark.sql.types.StructType(Seq.empty)))
    assert(paimonScan.pushedDataFilters.nonEmpty)
    assert(BoltOffloadPaimonScan().offload(emptyProjection) eq emptyProjection)
    assert(FallbackTags.get(emptyProjection).reason().contains("nested type evolution"))
  }
}
