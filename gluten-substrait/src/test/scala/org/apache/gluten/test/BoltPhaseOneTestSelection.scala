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
package org.apache.gluten.test

/** Phase one defers complete TPC workloads, never ordinary UTs using TPC data fixtures. */
object BoltPhaseOneTestSelection {
  private val deferredSuites = Set(
    "org.apache.spark.sql.GlutenTPCDSCollationQueryTestSuite",
    "org.apache.spark.sql.GlutenTPCDSModifiedPlanStabilitySuite",
    "org.apache.spark.sql.GlutenTPCDSModifiedPlanStabilityWithStatsSuite",
    "org.apache.spark.sql.GlutenTPCDSQueryANSISuite",
    "org.apache.spark.sql.GlutenTPCDSQuerySuite",
    "org.apache.spark.sql.GlutenTPCDSQueryTestSuite",
    "org.apache.spark.sql.GlutenTPCDSQueryWithStatsSuite",
    "org.apache.spark.sql.GlutenTPCDSV1_4_PlanStabilitySuite",
    "org.apache.spark.sql.GlutenTPCDSV1_4_PlanStabilityWithStatsSuite",
    "org.apache.spark.sql.GlutenTPCDSV2_7_PlanStabilitySuite",
    "org.apache.spark.sql.GlutenTPCDSV2_7_PlanStabilityWithStatsSuite",
    "org.apache.spark.sql.GlutenTPCHPlanStabilitySuite",
    "org.apache.spark.sql.GlutenTPCHQuerySuite",
    "org.apache.gluten.execution.BoltTPCHDeltaSuite",
    "org.apache.gluten.execution.BoltTPCHHudiSuite",
    "org.apache.gluten.execution.BoltTPCDSSuite"
  )

  def shouldDefer(ciEnabled: Boolean, isBoltBackend: => Boolean, suiteName: String): Boolean = {
    ciEnabled && deferredSuites.contains(suiteName) && isBoltBackend
  }
}
