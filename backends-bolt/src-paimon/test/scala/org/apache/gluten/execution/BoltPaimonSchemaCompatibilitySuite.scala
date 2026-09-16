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

import org.apache.paimon.schema.TableSchema
import org.apache.paimon.types._
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class BoltPaimonSchemaCompatibilitySuite extends AnyFunSuite {
  private def row(fields: DataField*): RowType = new RowType(fields.asJava)

  private def schema(id: Long, fields: DataField*): TableSchema =
    new TableSchema(
      id,
      fields.asJava,
      100,
      Seq.empty[String].asJava,
      Seq.empty[String].asJava,
      Map.empty[String, String].asJava,
      "")

  private val oldPayload = row(new DataField(1, "a", new IntType()))
  private val newPayload = row(
    new DataField(1, "a", new IntType()),
    new DataField(2, "b", new IntType()))

  test("selected struct, array and map evolution rejects historical files") {
    val typePairs = Seq[(DataType, DataType)](
      oldPayload -> newPayload,
      new ArrayType(oldPayload) -> new ArrayType(newPayload),
      new MapType(new IntType(), oldPayload) -> new MapType(new IntType(), newPayload))
    typePairs.foreach {
      case (oldType, newType) =>
        val oldSchema = schema(0, new DataField(0, "payload", oldType))
        val querySchema = schema(1, new DataField(0, "payload", newType))
        val reason = BoltPaimonSchemaCompatibility.validate(
          querySchema,
          Seq("payload"),
          Iterator(0L),
          _ => oldSchema)
        assert(reason.exists(_.contains("nested type evolution")))
        assert(reason.exists(_.contains("field 'payload' (id 0)")))
        assert(reason.exists(_.contains("file schema 0 to query schema 1")))
    }
  }

  test("nested fields renamed or replaced with new IDs require Java reading") {
    val oldSchema = schema(0, new DataField(0, "payload", oldPayload))
    Seq(
      row(new DataField(1, "renamed", new IntType())),
      row(new DataField(2, "a", new IntType())),
      row(new DataField(1, "a", new BigIntType()))).foreach {
      payload =>
        val querySchema = schema(1, new DataField(0, "payload", payload))
        assert(
          BoltPaimonSchemaCompatibility
            .validate(querySchema, Seq("payload"), Iterator(0L), _ => oldSchema)
            .nonEmpty)
    }
  }

  test("top-level names and positions do not replace field IDs") {
    val oldSchema = schema(
      0,
      new DataField(0, "old_name", oldPayload),
      new DataField(3, "id", new IntType()))
    val querySchema = schema(
      1,
      new DataField(3, "id", new BigIntType()),
      new DataField(0, "renamed", oldPayload))
    assert(
      BoltPaimonSchemaCompatibility
        .validate(querySchema, Seq("renamed", "id"), Iterator(0L), _ => oldSchema)
        .isEmpty)
  }

  test("new complex fields and unprojected nested evolution remain admissible") {
    val oldSchema = schema(
      0,
      new DataField(0, "payload", oldPayload),
      new DataField(3, "id", new IntType()))
    val querySchema = schema(
      1,
      new DataField(0, "payload", newPayload),
      new DataField(3, "id", new IntType()),
      new DataField(4, "new_payload", newPayload))
    assert(
      BoltPaimonSchemaCompatibility
        .validate(querySchema, Seq("id", "new_payload"), Iterator(0L), _ => oldSchema)
        .isEmpty)
  }

  test("empty native projection checks the full schema") {
    val oldSchema = schema(0, new DataField(0, "payload", oldPayload))
    val querySchema = schema(1, new DataField(0, "payload", newPayload))
    assert(
      BoltPaimonSchemaCompatibility
        .validate(querySchema, Seq.empty, Iterator(0L), _ => oldSchema)
        .nonEmpty)
  }

  test("historical schemas are loaded once and the query schema needs no loading") {
    val oldSchema = schema(0, new DataField(0, "payload", oldPayload))
    val querySchema = schema(1, new DataField(0, "payload", oldPayload))
    var loaded = Seq.empty[Long]
    val reason = BoltPaimonSchemaCompatibility.validate(
      querySchema,
      Seq("payload"),
      Iterator(0L, 1L, 0L),
      id => {
        loaded :+= id
        oldSchema
      })
    assert(reason.isEmpty)
    assert(loaded == Seq(0L))
  }

  test("schema lookup failures and unknown read fields fail closed") {
    val querySchema = schema(1, new DataField(0, "payload", oldPayload))
    val loadFailure = BoltPaimonSchemaCompatibility.validate(
      querySchema,
      Seq("payload"),
      Iterator(0L),
      _ => throw new IllegalStateException("missing schema"))
    assert(loadFailure.exists(_.contains("file schema 0 inspection failed")))
    assert(loadFailure.exists(_.contains("missing schema")))

    val unknownField = BoltPaimonSchemaCompatibility.validate(
      querySchema,
      Seq("unknown"),
      Iterator(1L),
      _ => querySchema)
    assert(unknownField.exists(_.contains("read field 'unknown' is absent from query schema 1")))
  }
}
