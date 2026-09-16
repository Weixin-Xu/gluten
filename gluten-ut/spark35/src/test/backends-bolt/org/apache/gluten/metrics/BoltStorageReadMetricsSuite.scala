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
package org.apache.gluten.metrics

import org.apache.spark.SparkFunSuite

import com.fasterxml.jackson.databind.ObjectMapper

class BoltStorageReadMetricsSuite extends SparkFunSuite {
  test("storage reads use native I/O counts instead of metric snapshot counts") {
    val mapper = new ObjectMapper()
    val cases = Seq(
      ("\"numStorageRead\": {\"sum\": 17, \"count\": 2},", 17L),
      ("\"numStorageRead\": {\"sum\": 0, \"count\": 2},", 0L),
      ("", 2L))
    cases.foreach {
      case (readCount, expected) =>
        val json = mapper.readTree(s"""{
          |  "customStats": {
          |    $readCount
          |    "storageReadBytes": {"sum": 8192, "count": 2}
          |  }
          |}""".stripMargin)
        val metrics = MetricsUtil.operatorMetricFromJson(json)
        assert(metrics.storageReadBytes == 8192)
        assert(metrics.storageReads == expected)
    }
  }
}
