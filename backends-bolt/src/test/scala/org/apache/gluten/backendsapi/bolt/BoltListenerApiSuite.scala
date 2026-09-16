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
package org.apache.gluten.backendsapi.bolt

import org.scalatest.funsuite.AnyFunSuite

class BoltListenerApiSuite extends AnyFunSuite {
  private val platformDir = "linux/amd64"
  private val gflags = s"$platformDir/${System.mapLibraryName("gflags_nothreads")}"
  private val glog = s"$platformDir/${System.mapLibraryName("glog")}"

  test("load packaged flag registries in dependency order") {
    assert(
      BoltListenerApi.flagRegistryLibraries(platformDir, Set(gflags, glog).contains) ==
        Seq(gflags, glog))
  }

  test("reject missing packaged flag registry resources") {
    val error = intercept[IllegalStateException] {
      BoltListenerApi.flagRegistryLibraries(platformDir, Set.empty[String].contains)
    }
    assert(error.getMessage.contains(gflags))
    assert(error.getMessage.contains(glog))
  }

  test("reject a packaged flag registry closure missing glog") {
    val error = intercept[IllegalStateException] {
      BoltListenerApi.flagRegistryLibraries(platformDir, Set(gflags).contains)
    }
    assert(error.getMessage.contains(glog))
  }

  test("reject a packaged flag registry closure missing gflags") {
    val error = intercept[IllegalStateException] {
      BoltListenerApi.flagRegistryLibraries(platformDir, Set(glog).contains)
    }
    assert(error.getMessage.contains(gflags))
  }
}
