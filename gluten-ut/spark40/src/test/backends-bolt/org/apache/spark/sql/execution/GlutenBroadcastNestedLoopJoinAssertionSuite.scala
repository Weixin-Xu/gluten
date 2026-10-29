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
package org.apache.spark.sql.execution

import org.apache.gluten.execution.BroadcastNestedLoopJoinExecTransformer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.optimizer.BuildRight
import org.apache.spark.sql.catalyst.plans.Inner

class GlutenBroadcastNestedLoopJoinAssertionSuite extends SparkFunSuite {
  test("Bolt BNL assertions accept the common type without loading Velox") {
    val tag = GlutenWholeStageCodegenSuite.broadcastNestedLoopJoinClassTag(
      isBoltBackend = true,
      throw new ClassNotFoundException("Velox is absent from the Bolt profile"))
    assert(tag.runtimeClass.isAssignableFrom(classOf[BroadcastNestedLoopJoinExecTransformer]))
    assert(tag.runtimeClass.isAssignableFrom(classOf[OtherBackendBnlFixture]))
  }

  test("non-Bolt BNL assertions retain the concrete type and reject other transformers") {
    val tag = GlutenWholeStageCodegenSuite.broadcastNestedLoopJoinClassTag(
      isBoltBackend = false,
      classOf[VeloxBnlFixture])
    assert(tag.runtimeClass == classOf[VeloxBnlFixture])
    assert(tag.runtimeClass.isAssignableFrom(classOf[VeloxBnlSubclassFixture]))
    assert(!tag.runtimeClass.isAssignableFrom(classOf[BroadcastNestedLoopJoinExecTransformer]))
    assert(!tag.runtimeClass.isAssignableFrom(classOf[OtherBackendBnlFixture]))
  }

  test("non-Bolt BNL assertions do not replace a missing concrete type with the common type") {
    intercept[ClassNotFoundException] {
      GlutenWholeStageCodegenSuite.broadcastNestedLoopJoinClassTag(
        isBoltBackend = false,
        throw new ClassNotFoundException("missing concrete transformer"))
    }
  }
}

abstract private class VeloxBnlFixture
  extends BroadcastNestedLoopJoinExecTransformer(null, null, BuildRight, Inner, None)

abstract private class VeloxBnlSubclassFixture extends VeloxBnlFixture

abstract private class OtherBackendBnlFixture
  extends BroadcastNestedLoopJoinExecTransformer(null, null, BuildRight, Inner, None)
