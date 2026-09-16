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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.proto.PaimonTableEnhancement
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.`type`.{TypeBuilder, TypeNode}
import org.apache.gluten.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import org.apache.gluten.substrait.rel.{PaimonSerializedSplitLocalFilesBuilder, SplitInfo}
import org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.softaffinity.SoftAffinity
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.paimon.CoreOptions
import org.apache.paimon.spark.{PaimonInputPartition, PaimonScan}
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.DataSplit

import java.net.URI

import scala.collection.JavaConverters._

case class BoltPaimonScanTransformer(
    override val output: Seq[AttributeReference],
    @transient override val scan: Scan,
    override val runtimeFilters: Seq[Expression],
    @transient override val table: Table,
    override val keyGroupedPartitioning: Option[Seq[Expression]] = None,
    override val commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None,
    override val pushDownFilters: Option[Seq[Expression]] = None)
  extends BatchScanExecTransformerBase(
    output = output,
    scan = scan,
    runtimeFilters = runtimeFilters,
    table = table,
    keyGroupedPartitioning = keyGroupedPartitioning,
    commonPartitionValues = commonPartitionValues
  ) {

  private lazy val paimonScan = scan.asInstanceOf[PaimonScan]
  private lazy val fileStoreTable = paimonScan.table.asInstanceOf[FileStoreTable]
  private lazy val coreOptions = fileStoreTable.coreOptions()
  private lazy val tablePath =
    new URI(coreOptions.toConfiguration.get(CoreOptions.PATH)).normalize().toString

  private def physicalReadFields: Seq[StructField] = {
    // Catalyst may spell an output attribute using the query's case. Resolve it
    // back to the physical read schema before the shared scan code normalizes names.
    val readFields = scan.readSchema().fields
    val resolver = SQLConf.get.resolver
    output.map {
      attr =>
        readFields.find(_.name == attr.name)
          .orElse(readFields.find(field => resolver(field.name, attr.name)))
          .getOrElse {
            throw new GlutenNotSupportException(s"Paimon read field not found: ${attr.name}")
          }
    }
  }

  override protected def readRelNames: java.util.List[String] =
    physicalReadFields.map(_.name).asJava

  override protected def readRelTypeNodes: java.util.List[TypeNode] = {
    def physicalTypeNode(dataType: DataType, nullable: Boolean): TypeNode = dataType match {
      case struct: StructType =>
        TypeBuilder.makeStruct(
          nullable,
          struct.fields.map(field => physicalTypeNode(field.dataType, field.nullable)).toSeq.asJava,
          struct.fieldNames.toSeq.asJava)
      case ArrayType(elementType, containsNull) =>
        TypeBuilder.makeList(nullable, physicalTypeNode(elementType, containsNull))
      case MapType(keyType, valueType, valueContainsNull) =>
        TypeBuilder.makeMap(
          nullable,
          physicalTypeNode(keyType, nullable = false),
          physicalTypeNode(valueType, valueContainsNull))
      case _ => ConverterUtils.getTypeNode(dataType, nullable)
    }

    output.zip(physicalReadFields).map {
      case (attr, field) => physicalTypeNode(field.dataType, attr.nullable)
    }.asJava
  }

  override def getPartitionSchema: StructType =
    StructType(scan.readSchema().filter(
      field => paimonScan.table.partitionKeys().contains(field.name)))

  override def getDataSchema: StructType =
    StructType(scan.readSchema().filterNot(
      field => paimonScan.table.partitionKeys().contains(field.name)))

  override lazy val fileFormat: ReadFileFormat =
    BoltPaimonScanValidator.readFileFormat(coreOptions).getOrElse(ReadFileFormat.UnknownFormat)

  override def doValidateInternal(): ValidationResult = {
    if (fileFormat == ReadFileFormat.UnknownFormat) {
      return ValidationResult.failed("Native Paimon scan requires a supported homogeneous format.")
    }
    if (!fileStoreTable.primaryKeys().isEmpty || coreOptions.deletionVectorsEnabled()) {
      return ValidationResult.failed("Native Paimon scan does not support PK/DV tables.")
    }
    super.doValidateInternal()
  }

  override protected[this] def supportsBatchScan(scan: Scan): Boolean =
    scan.getClass == classOf[PaimonScan]

  override def doExecuteColumnar(): RDD[ColumnarBatch] = throw new UnsupportedOperationException()

  override def getSplitInfosFromPartitions(
      partitions: Seq[(Partition, ReadFileFormat)]): Seq[SplitInfo] = {
    partitions.map {
      case (partition: SparkDataSourceRDDPartition, _) => partitionToSplitInfo(partition)
      case _ => throw new GlutenNotSupportException("Unsupported Paimon task partition.")
    }
  }

  override def withNewPushdownFilters(filters: Seq[Expression]): BoltPaimonScanTransformer =
    this.copy(pushDownFilters = Some(filters))

  override def withOutput(newOutput: Seq[AttributeReference]): BoltPaimonScanTransformer =
    this.copy(output = newOutput)

  private def partitionToSplitInfo(
      partition: SparkDataSourceRDDPartition): SplitInfo = {
    val dataSplits = partition.inputPartitions.flatMap {
      case inputPartition: PaimonInputPartition => inputPartition.splits
      case other =>
        throw new GlutenNotSupportException(
          s"Unsupported Paimon input partition: ${other.getClass.getName}")
    }.map {
      case split: DataSplit => split
      case other =>
        throw new GlutenNotSupportException(
          s"Unsupported Paimon split: ${other.getClass.getName}")
    }

    val serialized = dataSplits.map(PaimonSplitSerde.serialize)
    val affinityPaths = dataSplits.map {
      split =>
        val rawFiles = split.convertToRawFiles()
        if (rawFiles.isPresent) {
          rawFiles.get().asScala.headOption.map(_.path()).getOrElse(tablePath)
        } else {
          tablePath
        }
    }

    PaimonSerializedSplitLocalFilesBuilder.make(
      partition.index,
      affinityPaths.asJava,
      SoftAffinity
        .getFilePartitionLocations(affinityPaths.toArray, partition.preferredLocations())
        .toList
        .asJava,
      serialized.asJava
    )
  }

  override def getAdvancedExtension: Option[AdvancedExtensionNode] = {
    val enhancement = PaimonTableEnhancement
      .newBuilder()
      .putAllTableProperties(fileStoreTable.options())
      .setTablePath(tablePath)
      .setTableSchemaJson(fileStoreTable.schema().toString)
      .build()
    Some(
      ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(enhancement)))
  }

  override def doCanonicalize(): BoltPaimonScanTransformer =
    this.copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      runtimeFilters = QueryPlan.normalizePredicates(
        runtimeFilters.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral)),
        output),
      pushDownFilters = pushDownFilters.map(QueryPlan.normalizePredicates(_, output))
    )
}

object BoltPaimonScanTransformer {
  def apply(batchScan: BatchScanExec): BoltPaimonScanTransformer = {
    new BoltPaimonScanTransformer(
      batchScan.output,
      batchScan.scan,
      batchScan.runtimeFilters,
      table = batchScan.table,
      keyGroupedPartitioning = batchScan.keyGroupedPartitioning,
      commonPartitionValues = SparkShimLoader.getSparkShims.getCommonPartitionValues(batchScan)
    )
  }
}
