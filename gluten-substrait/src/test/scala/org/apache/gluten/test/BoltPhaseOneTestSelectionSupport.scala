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

import org.apache.gluten.backendsapi.SubstraitBackend
import org.apache.gluten.component.Component

import org.scalatest.{Args, Status, SucceededStatus, Suite, SuiteMixin}
import org.scalatest.events.{InfoProvided, NameInfo, TestIgnored}

/** Skip deferred suites before Spark's beforeAll initializes their external TPC fixtures. */
trait BoltPhaseOneTestSelectionSupport extends SuiteMixin { this: Suite =>
  protected def isBoltSuiteDeferred: Boolean = {
    BoltPhaseOneTestSelection.shouldDefer(
      sys.env.get("GLUTEN_BOLT_CI").contains("true"),
      Component.sorted().collectFirst { case b: SubstraitBackend => b.name() }.contains("bolt"),
      getClass.getName)
  }

  abstract override def run(testName: Option[String], args: Args): Status = {
    if (!isBoltSuiteDeferred) {
      super.run(testName, args)
    } else {
      val names = testName.toSeq ++ (if (testName.isEmpty) testNames.toSeq else Seq.empty)
      args.reporter(InfoProvided(
        ordinal = args.tracker.nextOrdinal(),
        message = s"Deferred by Bolt UT-first CI scope: $suiteId (${names.size} tests); " +
          "TPC queries and plan stability are outside phase one",
        nameInfo = Some(NameInfo(suiteName, suiteId, Some(getClass.getName), None))
      ))
      names.foreach {
        name =>
          args.reporter(TestIgnored(
            ordinal = args.tracker.nextOrdinal(),
            suiteName = suiteName,
            suiteId = suiteId,
            suiteClassName = Some(getClass.getName),
            testName = name,
            testText = name))
      }
      SucceededStatus
    }
  }
}
