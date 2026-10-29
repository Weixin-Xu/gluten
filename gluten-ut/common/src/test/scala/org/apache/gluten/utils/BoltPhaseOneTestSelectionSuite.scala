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
package org.apache.gluten.utils

import org.apache.gluten.test.{BoltPhaseOneTestSelection, BoltPhaseOneTestSelectionSupport}

import org.scalatest.{Args, BeforeAndAfterAll, Reporter}
import org.scalatest.events.{Event, InfoProvided, TestIgnored, TestSucceeded}
import org.scalatest.funsuite.AnyFunSuite

class BoltPhaseOneTestSelectionSuite extends AnyFunSuite {
  private val deferred = Seq(
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

  test("Bolt CI defers full TPC query, collation and plan stability suites") {
    deferred.foreach {
      suite => assert(BoltPhaseOneTestSelection.shouldDefer(true, true, suite), suite)
    }
  }

  test("local Bolt runs retain all TPC suites without discovering the backend") {
    deferred.foreach {
      suite =>
        assert(!BoltPhaseOneTestSelection.shouldDefer(
          false,
          throw new IllegalStateException("backend discovery"),
          suite))
    }
  }

  test("other backends retain all TPC suites even when the Bolt CI flag is set") {
    deferred.foreach {
      suite => assert(!BoltPhaseOneTestSelection.shouldDefer(true, false, suite), suite)
    }
  }

  test("ordinary UT and similar suite names remain selected") {
    val retained = Seq(
      "org.apache.spark.sql.GlutenJoinSuite",
      "org.apache.spark.sql.GlutenICUCollationsMapSuite",
      "org.apache.spark.sql.catalyst.expressions.GlutenCollationExpressionSuite",
      "org.apache.gluten.execution.BoltHashJoinSuite",
      "org.apache.gluten.execution.TestTPCHStoragePartitionedJoins",
      "org.apache.spark.sql.GlutenTPCHQuerySuiteExtra",
      "other.package.GlutenTPCHQuerySuite"
    )
    retained.foreach {
      suite => assert(!BoltPhaseOneTestSelection.shouldDefer(true, true, suite), suite)
    }
  }

  private class LifecycleSuite(
      ciEnabled: Boolean,
      boltBackend: Boolean,
      selectedSuite: String = "org.apache.gluten.execution.BoltTPCHDeltaSuite")
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BoltPhaseOneTestSelectionSupport {
    var lifecycle = Vector.empty[String]
    override protected def isBoltSuiteDeferred: Boolean =
      BoltPhaseOneTestSelection.shouldDefer(ciEnabled, boltBackend, selectedSuite)
    override protected def beforeAll(): Unit = { lifecycle :+= "beforeAll" }
    override protected def afterAll(): Unit = { lifecycle :+= "afterAll" }
    test("query")(lifecycle :+= "query")
  }

  test("deferred suites report ignored tests without initializing TPC fixtures") {
    val suite = new LifecycleSuite(true, true)
    val events = scala.collection.mutable.ArrayBuffer.empty[Event]
    val status = suite.run(
      None,
      Args(new Reporter {
        override def apply(event: Event): Unit = events += event
      }))
    assert(status.succeeds())
    assert(suite.lifecycle.isEmpty)
    assert(events.collect { case e: TestIgnored => e.testName }.toSeq == Seq("query"))
    assert(!events.exists(_.isInstanceOf[TestSucceeded]))
    assert(events.collect { case e: InfoProvided => e.message }.exists(
      _.contains("Deferred by Bolt UT-first CI scope")))
  }

  test("retained suites still initialize fixtures and execute their tests") {
    val retained = Seq(
      new LifecycleSuite(false, true),
      new LifecycleSuite(true, false),
      new LifecycleSuite(true, true, "org.apache.gluten.execution.BoltHashJoinSuite"))
    retained.foreach {
      suite =>
        val events = scala.collection.mutable.ArrayBuffer.empty[Event]
        val status = suite.run(
          None,
          Args(new Reporter {
            override def apply(event: Event): Unit = events += event
          }))
        assert(status.succeeds())
        assert(suite.lifecycle == Vector("beforeAll", "query", "afterAll"))
        assert(events.count(_.isInstanceOf[TestSucceeded]) == 1)
        assert(!events.exists(_.isInstanceOf[TestIgnored]))
    }
  }

}
