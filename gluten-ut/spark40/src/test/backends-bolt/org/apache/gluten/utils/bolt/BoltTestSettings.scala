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
package org.apache.gluten.utils.bolt

import org.apache.gluten.utils.{BackendTestSettings, SQLQueryTestSettings}

import org.apache.spark.sql.GlutenBoltBackendSmokeSuite
import org.apache.spark.sql.catalyst.expressions.{GlutenArithmeticExpressionSuite, GlutenBitwiseExpressionsSuite, GlutenConditionalExpressionSuite, GlutenLiteralExpressionSuite, GlutenNullExpressionsSuite, GlutenPredicateSuite}

/** Spark 4 coverage is enabled per suite after checking Bolt execution and results. */
class BoltTestSettings extends BackendTestSettings {
  enableSuite[GlutenBoltBackendSmokeSuite]
  enableSuite[GlutenArithmeticExpressionSuite]
  enableSuite[GlutenBitwiseExpressionsSuite]
  enableSuite[GlutenConditionalExpressionSuite]
  enableSuite[GlutenLiteralExpressionSuite]
  enableSuite[GlutenNullExpressionsSuite]
  enableSuite[GlutenPredicateSuite]

  override def getSQLQueryTestSettings: SQLQueryTestSettings = BoltSQLQueryTestSettings
}

// Spark 3.5 backend-specific golden files are not reusable for Spark 4.
// Enable SQL query resources only after validating their version-specific results.
private object BoltSQLQueryTestSettings extends SQLQueryTestSettings {
  override def getResourceFilePath: String = getClass.getResource("/").getPath
  override def getSupportedSQLQueryTests: Set[String] = Set.empty
  override def getOverwriteSQLQueryTests: Set[String] = Set.empty
}
