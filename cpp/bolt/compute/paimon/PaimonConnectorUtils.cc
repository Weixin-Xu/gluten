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

#include "compute/paimon/PaimonConnectorUtils.h"

#include <array>
#include <string>
#include <utility>

#include "bolt/connectors/paimon/PaimonConfig.h"
#include "bolt/connectors/paimon/PaimonConnectorSplit.h"
#include "config/BoltConfig.h"

namespace gluten {
namespace {

using bytedance::bolt::connector::paimon::PaimonConfig;

constexpr std::array<std::pair<const char*, const char*>, 10> kConfigMappings{{
    {"spark.gluten.paimon.read.batch-size", PaimonConfig::kReadBatchSize},
    {"spark.gluten.paimon.read.multi-thread-row-to-batch", PaimonConfig::kMultiThreadRowToBatch},
    {"spark.gluten.paimon.read.row-to-batch-thread-num", PaimonConfig::kRowToBatchThreadNum},
    {"spark.gluten.paimon.read.prefetch-enabled", PaimonConfig::kPrefetchEnabled},
    {"spark.gluten.paimon.read.prefetch-batch-count", PaimonConfig::kPrefetchBatchCount},
    {"spark.gluten.paimon.read.prefetch-max-parallel", PaimonConfig::kPrefetchMaxParallel},
    {"spark.gluten.paimon.read.predicate-filter-enabled", PaimonConfig::kPredicateFilterEnabled},
    {"spark.gluten.paimon.io.natural-read-size", PaimonConfig::kNaturalReadSize},
    {"spark.gluten.paimon.io.coalesce-reads", PaimonConfig::kCoalesceReads},
    {"spark.gluten.paimon.read.timestamp-unit", PaimonConfig::kReadTimestampUnit},
}};

} // namespace

namespace paimon {

std::vector<std::shared_ptr<bytedance::bolt::connector::ConnectorSplit>> makeConnectorSplits(
    const PaimonSplitInfo& info) {
  BOLT_CHECK(info.mode == PaimonSplitMode::kSerialized, "Native Paimon connector splits require serialized mode");
  // An aligned task partition may have no splits (for example, an outer SPJ).
  std::vector<std::shared_ptr<bytedance::bolt::connector::ConnectorSplit>> connectorSplits;
  connectorSplits.reserve(info.serializedSplits.size());
  for (const auto& splitBytes : info.serializedSplits) {
    BOLT_CHECK(!splitBytes.empty(), "Serialized Paimon split payload must not be empty");
    connectorSplits.emplace_back(std::make_shared<bytedance::bolt::connector::paimon::PaimonConnectorSplit>(
        kPaimonConnectorId, splitBytes.data(), splitBytes.size()));
  }
  return connectorSplits;
}

std::optional<std::vector<std::shared_ptr<bytedance::bolt::connector::ConnectorSplit>>>
tryMakeSerializedConnectorSplits(const std::shared_ptr<SplitInfo>& info) {
  const auto paimonInfo = std::dynamic_pointer_cast<PaimonSplitInfo>(info);
  if (!paimonInfo || paimonInfo->mode != PaimonSplitMode::kSerialized) {
    return std::nullopt;
  }
  return makeConnectorSplits(*paimonInfo);
}

std::shared_ptr<bytedance::bolt::config::ConfigBase> makeConnectorConfig(
    const bytedance::bolt::config::ConfigBase& queryConfig) {
  // Spark timestamps have microsecond precision; Bolt's millisecond default loses data.
  std::unordered_map<std::string, std::string> configs{{PaimonConfig::kReadTimestampUnit, "6"}};
  for (const auto& [sparkKey, paimonKey] : kConfigMappings) {
    if (const auto value = queryConfig.get<std::string>(paimonKey)) {
      configs[paimonKey] = value.value();
    } else if (const auto value = queryConfig.get<std::string>(sparkKey)) {
      configs[paimonKey] = value.value();
    }
  }
  return std::make_shared<bytedance::bolt::config::ConfigBase>(std::move(configs));
}

} // namespace paimon
} // namespace gluten
