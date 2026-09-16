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
package org.apache.gluten.config

import org.apache.spark.sql.internal.SQLConf

object GlutenPaimonConfig {
  val NATIVE_SOURCE_ENABLED = SQLConf
    .buildConf("spark.gluten.paimon.native.source.enabled")
    .booleanConf
    .createWithDefault(false)
  val READ_BATCH_SIZE = SQLConf
    .buildConf("spark.gluten.paimon.read.batch-size")
    .intConf
    .checkValue(_ > 0, "must be positive")
    .createWithDefault(16384)
  val MULTI_THREAD_ROW_TO_BATCH = SQLConf
    .buildConf("spark.gluten.paimon.read.multi-thread-row-to-batch")
    .booleanConf
    .createWithDefault(false)
  val ROW_TO_BATCH_THREAD_NUM = SQLConf
    .buildConf("spark.gluten.paimon.read.row-to-batch-thread-num")
    .intConf
    .checkValue(_ > 0, "must be positive")
    .createWithDefault(1)
  val PREFETCH_ENABLED = SQLConf
    .buildConf("spark.gluten.paimon.read.prefetch-enabled")
    .booleanConf
    .createWithDefault(false)
  val PREFETCH_BATCH_COUNT = SQLConf
    .buildConf("spark.gluten.paimon.read.prefetch-batch-count")
    .intConf
    .checkValue(_ > 0, "must be positive")
    .createWithDefault(600)
  val PREFETCH_MAX_PARALLEL = SQLConf
    .buildConf("spark.gluten.paimon.read.prefetch-max-parallel")
    .intConf
    .checkValue(_ > 0, "must be positive")
    .createWithDefault(3)
  val PREDICATE_FILTER_ENABLED = SQLConf
    .buildConf("spark.gluten.paimon.read.predicate-filter-enabled")
    .booleanConf
    .createWithDefault(true)
  val NATURAL_READ_SIZE = SQLConf
    .buildConf("spark.gluten.paimon.io.natural-read-size")
    .longConf
    .checkValue(_ > 0, "must be positive")
    .createWithDefault(32L << 20)
  val COALESCE_READS = SQLConf
    .buildConf("spark.gluten.paimon.io.coalesce-reads")
    .booleanConf
    .createWithDefault(true)
  val READ_TIMESTAMP_UNIT = SQLConf
    .buildConf("spark.gluten.paimon.read.timestamp-unit")
    .intConf
    .checkValues(Set(0, 3, 6, 9))
    .createWithDefault(6)
}
