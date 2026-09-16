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

import org.apache.gluten.config.GlutenPaimonConfig
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat.{OrcReadFormat, ParquetReadFormat}

import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.paimon.CoreOptions
import org.apache.paimon.CoreOptions.ChangelogProducer
import org.apache.paimon.predicate.{CompoundPredicate, LeafPredicate, Predicate}
import org.apache.paimon.spark.{PaimonInputPartition, PaimonScan}
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.types.DataTypeRoot

import java.lang.reflect.Method
import java.net.URI

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

object BoltPaimonScanValidator {
  private[execution] def readFileFormat(options: CoreOptions): Option[ReadFileFormat] = {
    val format = options.fileFormatString().toLowerCase(java.util.Locale.ROOT)
    val homogeneous = options.fileFormatPerLevel().values().asScala
      .forall(_.equalsIgnoreCase(format))
    if (!homogeneous) {
      None
    } else {
      format match {
        case "parquet" => Some(ParquetReadFormat)
        case "orc" => Some(OrcReadFormat)
        case _ => None
      }
    }
  }

  def validate(scan: BatchScanExec): ValidationResult =
    validate(scan, _.getMethod("beforeFiles"), PaimonSplitSerde.serialize)

  private[execution] def validate(
      scan: BatchScanExec,
      beforeFilesMethodLookup: Class[_] => Method): ValidationResult =
    validate(scan, beforeFilesMethodLookup, PaimonSplitSerde.serialize)

  private[execution] def validate(
      scan: BatchScanExec,
      beforeFilesMethodLookup: Class[_] => Method,
      splitSerializer: DataSplit => Array[Byte]): ValidationResult = {
    if (!SQLConf.get.getConf(GlutenPaimonConfig.NATIVE_SOURCE_ENABLED)) {
      return fail("disabled")
    }
    if (scan.scan.getClass != classOf[PaimonScan]) {
      return fail("not an exact PaimonScan")
    }

    val paimonScan = scan.scan.asInstanceOf[PaimonScan]
    if (!paimonScan.table.isInstanceOf[FileStoreTable]) {
      return fail("not a FileStoreTable")
    }

    val table = paimonScan.table.asInstanceOf[FileStoreTable]
    val coreOptions = table.coreOptions()
    val tablePath = coreOptions.toConfiguration.get(CoreOptions.PATH)
    if (!isSupportedTableUri(tablePath)) {
      return fail("table path does not use an absolute URI")
    }

    val format = readFileFormat(coreOptions) match {
      case Some(value) => value
      case None => return fail("file format is unsupported or heterogeneous")
    }
    if (
      format == OrcReadFormat &&
      SQLConf.get.getConf(GlutenPaimonConfig.READ_TIMESTAMP_UNIT) == 0
    ) {
      return fail(
        "ORC native scan does not support timestamp unit 0 (seconds); " +
          "use 3, 6, or 9 for native execution")
    }
    val partitionKeys = table.schema().partitionKeys().asScala.toSet
    val partitionTypesSupported = table.schema().fields().asScala.forall {
      field =>
        !partitionKeys.contains(field.name()) || (field.`type`().getTypeRoot match {
          // Paimon needs binary-row/path conversion even for unprojected partition fields.
          case DataTypeRoot.BOOLEAN | DataTypeRoot.TINYINT | DataTypeRoot.SMALLINT |
              DataTypeRoot.INTEGER | DataTypeRoot.BIGINT | DataTypeRoot.CHAR |
              DataTypeRoot.VARCHAR | DataTypeRoot.DATE => true
          case _ => false
        })
    }
    if (!partitionTypesSupported) {
      return fail("partition type is unsupported")
    }

    val dataSplits = plannedDataSplits(paimonScan) match {
      case Left(_) => return fail("partition/split type is unsupported")
      case Right(splits) => splits
    }
    dataSplits.foreach {
      split =>
        inspectBeforeFiles(split, beforeFilesMethodLookup) match {
          case Left(reason) => return fail(reason)
          case Right(true) => return fail("beforeFiles is unsupported")
          case Right(false) =>
        }
    }
    val unsupportedVersion =
      try {
        dataSplits.iterator
          .map(splitSerializer)
          .find(bytes => !PaimonSplitSerde.isSupportedVersion(bytes))
      } catch {
        case NonFatal(_) => return fail("split serde inspection failed")
      }
    if (unsupportedVersion.nonEmpty) {
      return fail("split serde version is unsupported")
    }
    if (coreOptions.deletionVectorsEnabled()) {
      return fail("deletion vectors are unsupported")
    }
    val requestedMetadataColumn = paimonScan.requiredSchema.fieldNames.exists {
      fieldName => PaimonMetadataColumn.SUPPORTED_METADATA_COLUMNS.contains(fieldName)
    }
    if (requestedMetadataColumn) {
      return fail("metadata columns are unsupported")
    }

    val readTypesSupported = paimonScan.readSchema().fields.forall {
      field => supportsType(field.dataType)
    }
    val outputTypesSupported = scan.output.forall(attribute => supportsType(attribute.dataType))
    if (!readTypesSupported || !outputTypesSupported) {
      return fail("Spark type is unsupported")
    }
    if (coreOptions.changelogProducer() == ChangelogProducer.LOOKUP) {
      return fail("LOOKUP is unsupported")
    }
    if (!table.primaryKeys().isEmpty) {
      return fail("primary-key merge engines are not yet whitelisted")
    }
    val schemaCompatibility =
      try {
        val readFields = paimonScan.readSchema().fieldNames.toSeq
        val filterFields = paimonScan.pushedDataFilters.flatMap(predicateFieldNames) ++
          scan.runtimeFilters.flatMap(_.references.map(_.name))
        // Paimon pushed filters may only prune splits; they do not guarantee a nonempty native
        // projection. Native SetReadSchema(empty) reads all fields, so check all of them in that case.
        val fieldsToCheck =
          if (readFields.isEmpty) Seq.empty else (readFields ++ filterFields).distinct
        BoltPaimonSchemaCompatibility.validate(
          table.schema(),
          fieldsToCheck,
          dataSplits.iterator.flatMap(_.dataFiles().asScala.iterator.map(_.schemaId())),
          schemaId => table.schemaManager().schema(schemaId)
        )
      } catch {
        case NonFatal(error) =>
          Some(
            s"read schema inspection failed: ${error.getClass.getSimpleName}: " +
              Option(error.getMessage).getOrElse("no detail"))
      }
    schemaCompatibility.foreach(reason => return fail(reason))
    ValidationResult.succeeded
  }

  private def predicateFieldNames(predicate: Predicate): Seq[String] = predicate match {
    case leaf: LeafPredicate => leaf.fieldNames().asScala.toSeq
    case compound: CompoundPredicate =>
      compound.children().asScala.toSeq.flatMap(predicateFieldNames)
    case other =>
      throw new IllegalArgumentException(s"unsupported predicate ${other.getClass.getName}")
  }

  private def fail(reason: String): ValidationResult =
    ValidationResult.failed(s"[Paimon Java fallback] $reason")

  private[execution] def isSupportedTableUri(tablePath: String): Boolean =
    Try(new URI(tablePath)).toOption.exists {
      // Native Paimon delegates to Bolt's filesystem registry, including object stores.
      // The available schemes depend on the native build and registered implementations.
      uri => uri.isAbsolute
    }

  private[execution] def supportsType(dataType: DataType): Boolean = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType |
        FloatType | DoubleType | StringType | BinaryType | DateType | TimestampType =>
      true
    case value: DecimalType => value.precision <= 38
    case ArrayType(elementType, _) => supportsType(elementType)
    case MapType(keyType, valueType, _) =>
      supportsType(keyType) && supportsType(valueType)
    case StructType(fields) => fields.forall(field => supportsType(field.dataType))
    case _ => false
  }

  // Paimon 1.4 removed beforeFiles from DataSplit. Only a confirmed missing method means this split
  // representation cannot carry before-files; every other reflection failure rejects fail closed.
  private def inspectBeforeFiles(
      split: DataSplit,
      methodLookup: Class[_] => Method): Either[String, Boolean] = {
    val method =
      try {
        methodLookup(split.getClass)
      } catch {
        case _: NoSuchMethodException => return Right(false)
        case NonFatal(_) => return Left("beforeFiles inspection failed")
      }

    try {
      Right(!method.invoke(split).asInstanceOf[java.util.List[_]].isEmpty)
    } catch {
      case NonFatal(_) => Left("beforeFiles inspection failed")
    }
  }

  private def plannedDataSplits(scan: PaimonScan): Either[String, Seq[DataSplit]] = {
    val splits = Seq.newBuilder[DataSplit]
    scan.toBatch.planInputPartitions().foreach {
      case partition: PaimonInputPartition =>
        partition.splits.foreach {
          case split: DataSplit => splits += split
          case other => return Left(s"unsupported split ${other.getClass.getName}")
        }
      case other => return Left(s"unsupported partition ${other.getClass.getName}")
    }
    Right(splits.result())
  }
}
