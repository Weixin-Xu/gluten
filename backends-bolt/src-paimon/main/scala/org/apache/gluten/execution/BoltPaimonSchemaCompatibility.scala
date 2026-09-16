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
import org.apache.paimon.types.{ArrayType, DataType, MapType, RowType}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

/** Admission check for the native reader's atomic-only schema evolution support. */
private[execution] object BoltPaimonSchemaCompatibility {
  def validate(
      querySchema: TableSchema,
      readFieldNames: Seq[String],
      fileSchemaIds: Iterator[Long],
      loadSchema: Long => TableSchema): Option[String] = {
    val queryFields = querySchema.fields().asScala
    val byName = queryFields.map(field => field.name() -> field).toMap
    // Native SetReadSchema(empty) reads the full table schema, including for count-only scans.
    val names = if (readFieldNames.isEmpty) queryFields.map(_.name()).toSeq else readFieldNames
    val readFields = names.distinct.map {
      name =>
        byName.getOrElse(
          name,
          return Some(s"read field '$name' is absent from query schema ${querySchema.id()}"))
    }
    val complexFields = readFields.filter(field => isComplex(field.`type`()))
    if (complexFields.isEmpty) {
      return None
    }

    val schemas = mutable.Map(querySchema.id() -> querySchema)
    val checked = mutable.Set.empty[Long]
    fileSchemaIds.foreach {
      schemaId =>
        if (checked.add(schemaId)) {
          try {
            val fileSchema = schemas.getOrElseUpdate(schemaId, loadSchema(schemaId))
            val fileFields = fileSchema.fields().asScala.map(field => field.id() -> field).toMap
            complexFields.foreach {
              readField =>
                // Missing top-level IDs are handled by the native reader's null-filled columns.
                fileFields.get(readField.id()).foreach {
                  fileField =>
                    // Top-level names/order/nullability do not affect the native ID mapping.
                    if (!sameType(fileField.`type`().copy(true), readField.`type`().copy(true))) {
                      return Some(
                        s"nested type evolution is unsupported for field '${readField.name()}' " +
                          s"(id ${readField.id()}), file schema $schemaId to query schema " +
                          s"${querySchema.id()}")
                    }
                }
            }
          } catch {
            case NonFatal(error) =>
              return Some(
                s"file schema $schemaId inspection failed: ${error.getClass.getSimpleName}: " +
                  Option(error.getMessage).getOrElse("no detail"))
          }
        }
    }
    None
  }

  private def isComplex(dataType: DataType): Boolean = dataType match {
    case _: RowType | _: ArrayType | _: MapType => true
    case _ => false
  }

  // Ignore comments/default-value metadata, but retain nested field IDs, names and order. Native
  // complex columns are not recursively remapped, so a nested drop/re-add with a new ID is unsafe
  // even if its Arrow type happens to remain identical.
  private def sameType(left: DataType, right: DataType): Boolean = {
    if (left.isNullable != right.isNullable) {
      return false
    }
    (left, right) match {
      case (l: RowType, r: RowType) =>
        val leftFields = l.getFields.asScala
        val rightFields = r.getFields.asScala
        leftFields.size == rightFields.size && leftFields.zip(rightFields).forall {
          case (lf, rf) =>
            lf.id() == rf.id() && lf.name() == rf.name() && sameType(lf.`type`(), rf.`type`())
        }
      case (l: ArrayType, r: ArrayType) => sameType(l.getElementType, r.getElementType)
      case (l: MapType, r: MapType) =>
        sameType(l.getKeyType, r.getKeyType) && sameType(l.getValueType, r.getValueType)
      case _ => left == right
    }
  }
}
