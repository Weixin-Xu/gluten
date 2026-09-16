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
package org.apache.gluten

package extension {

  import org.apache.gluten.component.BoltPaimonComponent
  import org.apache.gluten.extension.injector.Injector

  import org.apache.spark.sql.SparkSessionExtensions

  class BoltPaimonPlanningExtensions extends (SparkSessionExtensions => Unit) {
    override def apply(extensions: SparkSessionExtensions): Unit = {
      val injector = new Injector(extensions)
      new BoltPaimonComponent().injectRules(injector)
      injector.inject()
    }
  }
}

package execution {

  import org.apache.gluten.config.GlutenConfig
  import org.apache.gluten.extension.columnar.FallbackTags
  import org.apache.gluten.proto.PaimonTableEnhancement
  import org.apache.gluten.substrait.SubstraitContext

  import org.apache.spark.SparkConf
  import org.apache.spark.sql.catalyst.expressions.Literal
  import org.apache.spark.sql.execution.SparkPlan
  import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
  import org.apache.spark.sql.test.SharedSparkSession
  import org.apache.spark.sql.types._

  import com.google.protobuf.StringValue
  import io.substrait.proto.ReadRel
  import org.apache.paimon.schema.TableSchema
  import org.apache.paimon.spark.PaimonScan
  import org.apache.paimon.table.FileStoreTable
  import org.apache.paimon.table.source.DataSplit

  import java.net.URI
  import java.nio.{ByteBuffer, ByteOrder}

  import scala.collection.JavaConverters._

  class BoltPaimonPlanningSuite extends SharedSparkSession {
    private val rootPath: String = getClass.getResource("/").getPath

    override protected def sparkConf: SparkConf = {
      super.sparkConf
        .set("spark.sql.shuffle.partitions", "1")
        .set("spark.gluten.sql.enable.native.validation", "false")
        .set("spark.gluten.paimon.native.source.enabled", "true")
        .set(
          "spark.sql.extensions",
          "org.apache.gluten.extension.BoltPaimonPlanningExtensions," +
            "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions"
        )
        .set("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
        .set("spark.sql.catalog.paimon.warehouse", s"file://$rootPath/data-paimon-planning")
    }

    override def beforeAll(): Unit = {
      super.beforeAll()
      sql("DROP DATABASE IF EXISTS paimon.planning CASCADE")
      sql("CREATE DATABASE paimon.planning")
      sql("USE paimon.planning")
      sql(
        "CREATE TABLE append_table (id INT, value STRING) USING paimon " +
          "TBLPROPERTIES ('file.format' = 'parquet')")
      sql(
        "CREATE TABLE orc_table (id INT, value STRING) USING paimon " +
          "TBLPROPERTIES ('file.format' = 'orc')")
      sql(
        "CREATE TABLE orc_timestamp_table (id INT, ts TIMESTAMP) USING paimon " +
          "TBLPROPERTIES ('file.format' = 'orc')")
      sql(
        "CREATE TABLE primary_key_table (id INT, value STRING) USING paimon " +
          "TBLPROPERTIES ('file.format' = 'parquet', 'primary-key' = 'id')")
      sql(
        "CREATE TABLE decimal_partition_table (id INT, pt DECIMAL(5, 2)) USING paimon " +
          "PARTITIONED BY (pt) TBLPROPERTIES ('file.format' = 'parquet')")
      sql("INSERT INTO append_table VALUES (1, 'one')")
      sql("INSERT INTO decimal_partition_table VALUES (1, 12.34)")
    }

    override def afterAll(): Unit = {
      try {
        sql("USE default")
        sql("DROP DATABASE IF EXISTS paimon.planning CASCADE")
      } finally {
        super.afterAll()
      }
    }

    private def originalPlan(query: String): SparkPlan = sql(query).queryExecution.sparkPlan

    private def executedPlan(query: String): SparkPlan = sql(query).queryExecution.executedPlan

    private def originalBatchScan(query: String): BatchScanExec =
      originalPlan(query).collectFirst {
        case scan: BatchScanExec => scan
      }.getOrElse(fail(s"BatchScanExec missing from original plan for: $query"))

    test("Paimon reader settings propagate to the Bolt native session") {
      val paimonSettings = Map(
        "spark.gluten.paimon.read.batch-size" -> "1024",
        "spark.gluten.paimon.read.multi-thread-row-to-batch" -> "true",
        "spark.gluten.paimon.read.row-to-batch-thread-num" -> "2",
        "spark.gluten.paimon.read.prefetch-enabled" -> "true",
        "spark.gluten.paimon.read.prefetch-batch-count" -> "4",
        "spark.gluten.paimon.read.prefetch-max-parallel" -> "2",
        "spark.gluten.paimon.read.predicate-filter-enabled" -> "false",
        "spark.gluten.paimon.io.natural-read-size" -> "4096",
        "spark.gluten.paimon.io.coalesce-reads" -> "false",
        "spark.gluten.paimon.read.timestamp-unit" -> "6"
      )

      val nativeSettings = GlutenConfig.getNativeSessionConf("bolt", paimonSettings)
      paimonSettings.foreach {
        case (key, value) => assert(nativeSettings.get(key).contains(value), key)
      }
    }

    test("rejected Bolt offload keeps the scan and records the fallback reason") {
      withSQLConf("spark.gluten.paimon.native.source.enabled" -> "false") {
        val scan = originalBatchScan("SELECT * FROM append_table")
        assert(BoltOffloadPaimonScan().offload(scan) eq scan)
        assert(FallbackTags.get(scan).reason().contains("[Paimon Java fallback] disabled"))
      }
    }

    test("disabled native source keeps BatchScanExec") {
      withSQLConf("spark.gluten.paimon.native.source.enabled" -> "false") {
        val plan = executedPlan("SELECT * FROM append_table")
        assert(!plan.exists(_.isInstanceOf[PaimonScanTransformer]), plan.toString())
        assert(plan.exists(_.isInstanceOf[BatchScanExec]), plan.toString())
      }
    }

    test("supported Parquet append scan uses serialized Bolt Paimon input") {
      val scan = originalBatchScan("SELECT * FROM append_table")
      val validation = BoltPaimonScanValidator.validate(scan)
      assert(validation.ok())

      val plan = executedPlan("SELECT * FROM append_table")
      assert(!plan.exists(_.isInstanceOf[BatchScanExec]), plan.toString())
      val transformer = plan.collectFirst {
        case scan: BoltPaimonScanTransformer => scan
      }.getOrElse(fail(s"BoltPaimonScanTransformer missing from:\n$plan"))

      val localFiles = transformer.getSplitInfos.head.toProtobuf
        .asInstanceOf[ReadRel.LocalFiles]
      assert(localFiles.getItemsCount > 0)
      localFiles.getItemsList.asScala.foreach {
        item =>
          assert(item.hasPaimon)
          assert(!item.getPaimon.getSerializedSplit.isEmpty)
      }

      val advancedExtension = transformer.getAdvancedExtension
        .getOrElse(fail("Paimon table enhancement missing"))
        .toProtobuf
      assert(advancedExtension.hasEnhancement)
      val enhancement = advancedExtension.getEnhancement.unpack(classOf[PaimonTableEnhancement])
      assert(enhancement.getTablePath.nonEmpty)
      assert(new URI(enhancement.getTablePath).normalize().toString == enhancement.getTablePath)
      assert(enhancement.getTablePropertiesMap.asScala("file.format") == "parquet")
      val boundSchema = scan.scan.asInstanceOf[PaimonScan].table
        .asInstanceOf[FileStoreTable].schema()
      assert(TableSchema.fromJson(enhancement.getTableSchemaJson) == boundSchema)
    }

    test("Paimon enhancement retains the full schema when the scan projects one column") {
      val scan = originalBatchScan("SELECT value FROM append_table")
      val transformer = BoltPaimonScanTransformer(scan)
      val enhancement = transformer.getAdvancedExtension.get.toProtobuf.getEnhancement
        .unpack(classOf[PaimonTableEnhancement])
      val schema = TableSchema.fromJson(enhancement.getTableSchemaJson)
      val boundSchema = scan.scan.asInstanceOf[PaimonScan].table
        .asInstanceOf[FileStoreTable].schema()
      assert(schema == boundSchema)
      assert(schema.fieldNames().asScala.toSeq == Seq("id", "value"))
      assert(scan.scan.readSchema().fieldNames.toSeq == Seq("value"))
    }

    test("Paimon read relation keeps physical names before native conversion") {
      withTable("physical_names_table") {
        withSQLConf("spark.sql.caseSensitive" -> "false") {
          sql("CREATE TABLE physical_names_table (ID INT, Payload STRUCT<MixedField: INT>, " +
            "Items ARRAY<STRUCT<ItemField: INT>>, " +
            "ValuesMap MAP<STRING, STRUCT<ValueField: INT>>) USING paimon")
          val scan = originalBatchScan("SELECT * FROM physical_names_table")
          val transformer = BoltPaimonScanTransformer(scan)
            .withOutput(scan.output.map {
              attr => attr.withName(attr.name.toLowerCase(java.util.Locale.ROOT))
            })
          val schema = transformer.transform(new SubstraitContext).root.toProtobuf
            .getRead.getBaseSchema
          assert(schema.getNamesList.asScala.toSeq == Seq("ID", "Payload", "Items", "ValuesMap"))
          val types = schema.getStruct.getTypesList
          assert(types.get(1).getStruct.getNamesList.asScala.toSeq == Seq("MixedField"))
          assert(
            types.get(2).getList.getType.getStruct.getNamesList.asScala.toSeq == Seq("ItemField"))
          assert(
            types.get(3).getMap.getValue.getStruct.getNamesList.asScala.toSeq == Seq("ValueField"))
        }
      }
    }

    test("Paimon enhancement uses the planned table schema after a catalog schema change") {
      withTable("schema_binding_table") {
        sql("CREATE TABLE schema_binding_table (id INT) USING paimon")
        val scan = originalBatchScan("SELECT id FROM schema_binding_table")
        val boundSchema = scan.scan.asInstanceOf[PaimonScan].table
          .asInstanceOf[FileStoreTable].schema()
        sql("ALTER TABLE schema_binding_table ADD COLUMNS (value STRING)")
        val currentScan = originalBatchScan("SELECT * FROM schema_binding_table")
        val currentSchema = currentScan.scan.asInstanceOf[PaimonScan].table
          .asInstanceOf[FileStoreTable].schema()
        assert(currentSchema.id() > boundSchema.id())

        val enhancement = BoltPaimonScanTransformer(scan).getAdvancedExtension.get.toProtobuf
          .getEnhancement.unpack(classOf[PaimonTableEnhancement])
        assert(TableSchema.fromJson(enhancement.getTableSchemaJson) == boundSchema)
      }
    }

    test("Bolt Paimon transformer copies retain their concrete type") {
      val transformer = executedPlan("SELECT * FROM append_table").collectFirst {
        case scan: BoltPaimonScanTransformer => scan
      }.getOrElse(fail("BoltPaimonScanTransformer missing"))

      val withOutput = transformer.withOutput(transformer.output)
      val withFilters = transformer.withNewPushdownFilters(Seq(Literal.TrueLiteral))
      val canonicalized = transformer.canonicalized
      assert(withOutput.isInstanceOf[BoltPaimonScanTransformer])
      assert(withFilters.isInstanceOf[BoltPaimonScanTransformer])
      assert(withFilters.pushDownFilters.contains(Seq(Literal.TrueLiteral)))
      assert(canonicalized.isInstanceOf[BoltPaimonScanTransformer])
    }

    test("common Paimon transformer retains the default scan extension") {
      val transformer = PaimonScanTransformer(originalBatchScan("SELECT * FROM append_table"))
      val localFiles = transformer.getSplitInfos.head.toProtobuf
        .asInstanceOf[ReadRel.LocalFiles]
      assert(localFiles.getItemsCount > 0)
      localFiles.getItemsList.asScala.foreach {
        item =>
          assert(item.hasParquet)
          assert(!item.hasPaimon)
          assert(item.getUriFile.nonEmpty)
      }

      val advancedExtension = transformer.getAdvancedExtension
        .getOrElse(fail("Default scan extension missing"))
        .toProtobuf

      assert(advancedExtension.getOptimizationCount == 1)
      assert(
        advancedExtension.getOptimization(0).unpack(classOf[StringValue]).getValue ==
          "isMergeTree=0\n")
      assert(!advancedExtension.hasEnhancement)
    }

    test("split serde inspection failures reject the scan") {
      val scan = originalBatchScan("SELECT * FROM append_table")
      val failingSerializers = Seq[(String, DataSplit => Array[Byte])](
        "serialization" -> (_ => throw new IllegalStateException("serialization failed")),
        "header inspection" -> (_ => Array.emptyByteArray))

      failingSerializers.foreach {
        case (failure, serializer) =>
          val validation = BoltPaimonScanValidator.validate(
            scan,
            (splitClass: Class[_]) => splitClass.getMethod("beforeFiles"),
            serializer)
          assert(!validation.ok(), failure)
          assert(
            validation.reason().endsWith("[Paimon Java fallback] split serde inspection failed"),
            s"$failure: ${validation.reason()}")
      }
    }

    test("unsupported split serde version has a distinct fallback reason") {
      val scan = originalBatchScan("SELECT * FROM append_table")
      val validation = BoltPaimonScanValidator.validate(
        scan,
        (splitClass: Class[_]) => splitClass.getMethod("beforeFiles"),
        (split: DataSplit) => {
          val bytes = PaimonSplitSerde.serialize(split)
          ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putInt(java.lang.Long.BYTES, 9)
          bytes
        }
      )

      assert(!validation.ok())
      assert(
        validation.reason().endsWith("[Paimon Java fallback] split serde version is unsupported"),
        validation.reason())
    }

    test("supported ORC append scan uses the Bolt ORC input format") {
      val validation =
        BoltPaimonScanValidator.validate(originalBatchScan("SELECT * FROM orc_table"))
      assert(validation.ok())
      val plan = executedPlan("SELECT * FROM orc_table")
      val transformer = plan.collectFirst {
        case scan: BoltPaimonScanTransformer => scan
      }.getOrElse(fail(s"BoltPaimonScanTransformer missing from:\n$plan"))
      assert(
        transformer.fileFormat ==
          org.apache.gluten.substrait.rel.LocalFilesNode.ReadFileFormat.OrcReadFormat)
    }

    test("ORC Timestamp scan uses the generic Java fallback") {
      val plan = executedPlan("SELECT id, ts FROM orc_timestamp_table")
      assert(!plan.exists(_.isInstanceOf[BoltPaimonScanTransformer]), plan.toString())
      assert(plan.exists(_.isInstanceOf[BatchScanExec]), plan.toString())
    }

    test("ORC seconds timestamp unit falls back before native reader creation") {
      val orcQuery = "SELECT id, value FROM orc_table"
      withSQLConf("spark.gluten.paimon.read.timestamp-unit" -> "0") {
        val orcScan = originalBatchScan(orcQuery)
        val orcValidation = BoltPaimonScanValidator.validate(orcScan)
        assert(!orcValidation.ok())
        assert(
          orcValidation
            .reason()
            .endsWith(
              "[Paimon Java fallback] ORC native scan does not support timestamp unit 0 " +
                "(seconds); use 3, 6, or 9 for native execution"),
          orcValidation.reason()
        )

        val orcPlan = executedPlan(orcQuery)
        assert(!orcPlan.exists(_.isInstanceOf[BoltPaimonScanTransformer]), orcPlan.toString())
        assert(orcPlan.exists(_.isInstanceOf[BatchScanExec]), orcPlan.toString())

        val parquetPlan = executedPlan("SELECT id, value FROM append_table")
        assert(
          parquetPlan.exists(_.isInstanceOf[BoltPaimonScanTransformer]),
          parquetPlan.toString())
      }

      val defaultOrcPlan = executedPlan(orcQuery)
      assert(
        defaultOrcPlan.exists(_.isInstanceOf[BoltPaimonScanTransformer]),
        defaultOrcPlan.toString())
    }

    test("mixed and unknown Paimon file formats are rejected") {
      val mixed = new org.apache.paimon.CoreOptions(
        Map("file.format" -> "orc", "file.format.per.level" -> "0:parquet").asJava)
      val unknown = new org.apache.paimon.CoreOptions(Map("file.format" -> "avro").asJava)

      assert(BoltPaimonScanValidator.readFileFormat(mixed).isEmpty)
      assert(BoltPaimonScanValidator.readFileFormat(unknown).isEmpty)
    }

    test("metadata column scan falls back before transformer creation") {
      val validation = BoltPaimonScanValidator.validate(
        originalBatchScan("SELECT __paimon_row_index FROM append_table"))
      assert(!validation.ok())
      assert(
        validation.reason().endsWith("[Paimon Java fallback] metadata columns are unsupported"),
        validation.reason())
      val plan = executedPlan("SELECT __paimon_row_index FROM append_table")
      assert(!plan.exists(_.isInstanceOf[PaimonScanTransformer]), plan.toString())
      assert(plan.exists(_.isInstanceOf[BatchScanExec]), plan.toString())
    }

    test("primary-key scan falls back before transformer creation") {
      val validation = BoltPaimonScanValidator.validate(
        originalBatchScan("SELECT * FROM primary_key_table"))
      assert(!validation.ok())
      assert(
        validation
          .reason()
          .endsWith("[Paimon Java fallback] primary-key merge engines are not yet whitelisted"),
        validation.reason())
      val plan = executedPlan("SELECT * FROM primary_key_table")
      assert(!plan.exists(_.isInstanceOf[PaimonScanTransformer]), plan.toString())
      assert(plan.exists(_.isInstanceOf[BatchScanExec]), plan.toString())
    }

    test("decimal partition scan falls back before transformer creation") {
      val scan = originalBatchScan("SELECT * FROM decimal_partition_table")
      val validation = BoltPaimonScanValidator.validate(scan)
      assert(!validation.ok())
      assert(
        validation.reason().endsWith("[Paimon Java fallback] partition type is unsupported"),
        validation.reason())
      val plan = executedPlan("SELECT * FROM decimal_partition_table")
      assert(!plan.exists(_.isInstanceOf[BoltPaimonScanTransformer]), plan.toString())
      assert(plan.exists(_.isInstanceOf[BatchScanExec]), plan.toString())
    }

    test("floating partition types fall back even when not projected") {
      Seq("FLOAT", "DOUBLE").foreach {
        partitionType =>
          withTable("floating_partition_table") {
            sql(s"CREATE TABLE floating_partition_table (id INT, pt $partitionType) " +
              "USING paimon PARTITIONED BY (pt)")
            sql("INSERT INTO floating_partition_table VALUES (1, 12.34)")
            Seq("*", "id").foreach {
              projection =>
                val query = s"SELECT $projection FROM floating_partition_table"
                val result = BoltPaimonScanValidator.validate(originalBatchScan(query))
                assert(!result.ok())
                assert(result.reason().contains("partition type is unsupported"))
                val plan = executedPlan(query)
                assert(plan.exists(_.isInstanceOf[BatchScanExec]), plan.toString())
                assert(!plan.exists(_.isInstanceOf[BoltPaimonScanTransformer]), plan.toString())
            }
          }
      }
    }

    test("beforeFiles lookup failures reject the scan fail closed") {
      val result = BoltPaimonScanValidator.validate(
        originalBatchScan("SELECT * FROM append_table"),
        (_: Class[_]) => throw new SecurityException("reflection access denied"))
      assert(!result.ok())
      assert(
        result.reason().endsWith("[Paimon Java fallback] beforeFiles inspection failed"),
        result.reason())
    }

    test("beforeFiles invocation failures reject the scan fail closed") {
      val result = BoltPaimonScanValidator.validate(
        originalBatchScan("SELECT * FROM append_table"),
        (_: Class[_]) => classOf[String].getMethod("length"))
      assert(!result.ok())
      assert(
        result.reason().endsWith("[Paimon Java fallback] beforeFiles inspection failed"),
        result.reason())
    }

    test("table path allows object storage through the native filesystem registry") {
      Seq(
        "file:///tmp/paimon",
        "hdfs://namenode/table",
        "s3://bucket/table",
        "s3a://bucket/table",
        "oss://bucket/table",
        "tos://bucket/table").foreach {
        path => assert(BoltPaimonScanValidator.isSupportedTableUri(path), path)
      }
      Seq("relative/table", "file path with spaces", "://missing-scheme")
        .foreach {
          path => assert(!BoltPaimonScanValidator.isSupportedTableUri(path), path)
        }
    }

    test("native Spark types are checked recursively") {
      val supported = Seq(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        DecimalType(38, 18),
        StringType,
        BinaryType,
        DateType,
        TimestampType,
        ArrayType(IntegerType, containsNull = true),
        MapType(StringType, ArrayType(LongType, containsNull = false), valueContainsNull = true),
        StructType(
          Seq(
            StructField("id", LongType, nullable = false),
            StructField("payload", ArrayType(BinaryType, containsNull = true))))
      )
      supported.foreach {
        dataType => assert(BoltPaimonScanValidator.supportsType(dataType), dataType.toString)
      }

      val unsupported = Seq(
        NullType,
        CalendarIntervalType,
        ArrayType(NullType, containsNull = true),
        MapType(StringType, NullType, valueContainsNull = true),
        StructType(Seq(StructField("unsupported", NullType)))
      )
      unsupported.foreach {
        dataType => assert(!BoltPaimonScanValidator.supportsType(dataType), dataType.toString)
      }
    }
  }
}
