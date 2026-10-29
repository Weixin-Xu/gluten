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

import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Locale

class BoltTestBlacklistSuite extends AnyFunSuite {
  private val entry = """{"spark":"3.5","scala":"2.12","suite":"example.Suite",
      "test":"gluten literal # * and \nnewline","reason":"Confirmed engine failure",
      "evidence":"Bolt PR1045 / baseline report"}"""

  private def entries(json: String = "{\"entries\":[" + entry + "]}") = {
    val file = Files.createTempFile("bolt-blacklist", ".json")
    try {
      Files.write(file, json.getBytes(StandardCharsets.UTF_8))
      BoltTestBlacklist.load(file)
    } finally {
      Files.delete(file)
    }
  }

  test("only the exact suite and test at the matching Spark and Scala versions is skipped") {
    val loaded = entries()
    val name = "gluten literal # * and \nnewline"
    def skip(spark: String, scala: String, suite: String, test: String): Boolean =
      BoltTestBlacklist.shouldSkip(true, Some("bolt"), loaded, spark, scala, suite, test)
    assert(skip("3.5", "2.12", "example.Suite", name))
    assert(!skip("3.4", "2.12", "example.Suite", name))
    assert(!skip("3.5", "2.13", "example.Suite", name))
    assert(!skip("3.5", "2.12", "example.OtherSuite", name))
    assert(!skip("3.5", "2.12", "example.Suite", "gluten literal # anything"))
    assert(!skip("3.5", "2.12", "example.Suite", name.toUpperCase(Locale.ROOT)))
  }

  test("disabled opt-in does not discover a backend or read a blacklist") {
    assert(!BoltTestBlacklist.shouldSkip(
      false,
      throw new IllegalStateException("backend discovery"),
      throw new IllegalStateException("blacklist read"),
      "3.5",
      "2.12",
      "example.Suite",
      "test"))
  }

  test("non-Bolt and absent backends do not read a blacklist") {
    for (backend <- Seq(Some("velox"), Some("ch"), None)) {
      assert(!BoltTestBlacklist.shouldSkip(
        true,
        backend,
        throw new IllegalStateException("blacklist read"),
        "3.5",
        "2.12",
        "example.Suite",
        "test"))
    }
  }

  test("a blacklist entry must include a reason and failure evidence") {
    for (field <- Seq("reason", "evidence")) {
      val incomplete = entry.replaceAll("\"" + field + "\":\"[^\"]*\"", "\"" + field + "\":\"\"")
      intercept[IllegalArgumentException](entries("{\"entries\":[" + incomplete + "]}"))
    }
  }

  test("duplicate exact entries and malformed manifests fail visibly") {
    intercept[IllegalArgumentException](entries("{\"entries\":[" + entry + "," + entry + "]}"))
    intercept[IllegalArgumentException](entries("{}"))
    intercept[IllegalArgumentException](entries("{\"entries\":[{}]}"))
  }
  test("registration ignores only a selected UT and executes the other bodies") {
    val loaded = entries()
    var executed = Vector.empty[String]
    val selected = "gluten literal # * and \nnewline"
    val suite = new AnyFunSuite with BoltTestBlacklistSupport {
      override protected def isBoltTestBlacklisted(name: String): Boolean =
        BoltTestBlacklist.shouldSkip(
          true,
          Some("bolt"),
          loaded,
          "3.5",
          "2.12",
          "example.Suite",
          name)
      test(selected)(executed :+= "selected")
      test("healthy")(executed :+= "healthy")
    }
    val events = scala.collection.mutable.ArrayBuffer.empty[org.scalatest.events.Event]
    suite.run(
      None,
      org.scalatest.Args(new org.scalatest.Reporter {
        override def apply(event: org.scalatest.events.Event): Unit = events += event
      }))
    assert(executed == Vector("healthy"))
    assert(events.count(_.isInstanceOf[org.scalatest.events.TestIgnored]) == 1)
    assert(events.count(_.isInstanceOf[org.scalatest.events.TestSucceeded]) == 1)
    assert(!events.exists(_.isInstanceOf[org.scalatest.events.TestFailed]))
  }

}
