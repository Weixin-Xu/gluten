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
import org.apache.spark.sql.{DataFrame, GlutenQueryTest, Row}

class BoltPaimonCommonSuite extends PaimonSuite {
  override protected def fileFormatsToTest: Seq[String] = Seq("parquet", "orc")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.paimon.native.source.enabled", "true")
      .set("spark.sql.catalog.spark_catalog.warehouse", s"file://$rootPath/data-paimon-bolt-common")
  }

  override protected def checkPaimonScan(df: DataFrame): Unit =
    checkGlutenPlan[BoltPaimonScanTransformer](df)

  override protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val analyzedDf = df
    assertEmptyMissingInput(analyzedDf)
    // Paimon 1.4.1 cannot serialize its scan through the additional df.rdd probe.
    // Keep the actual native query execution and comparison with vanilla Spark.
    GlutenQueryTest.checkAnswer(analyzedDf, expectedAnswer, checkToRDD = false)
  }
}
