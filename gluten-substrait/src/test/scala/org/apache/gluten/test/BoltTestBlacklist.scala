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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import java.nio.file.Path

import scala.collection.JavaConverters._

/** Exact, version-scoped exclusions for confirmed Bolt UT failures. */
object BoltTestBlacklist {
  final case class Entry(
      spark: String,
      scala: String,
      suite: String,
      test: String,
      reason: String,
      evidence: String)

  def load(path: Path): Seq[Entry] = {
    val root = new ObjectMapper().readTree(path.toFile)
    val rows = root.get("entries")
    require(rows != null && rows.isArray, "Bolt UT blacklist must contain an entries array")
    def field(row: JsonNode, name: String): String = {
      val value = row.get(name)
      require(
        value != null && value.isTextual && value.asText().nonEmpty,
        s"Bolt UT blacklist entry requires $name")
      value.asText()
    }
    val entries = rows.elements().asScala.map {
      row =>
        Entry(
          field(row, "spark"),
          field(row, "scala"),
          field(row, "suite"),
          field(row, "test"),
          field(row, "reason"),
          field(row, "evidence"))
    }.toVector
    val keys = entries.map(e => (e.spark, e.scala, e.suite, e.test))
    require(keys.distinct.size == keys.size, "Duplicate exact Bolt UT blacklist entry")
    entries
  }

  // The by-name arguments keep backend discovery and file reads out of unrelated test runs.
  def shouldSkip(
      enabled: Boolean,
      backend: => Option[String],
      entries: => Seq[Entry],
      spark: String,
      scala: String,
      suite: String,
      test: String): Boolean = {
    enabled && backend.contains("bolt") && entries.exists {
      e => e.spark == spark && e.scala == scala && e.suite == suite && e.test == test
    }
  }
}
