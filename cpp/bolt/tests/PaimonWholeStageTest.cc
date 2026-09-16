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

#include <gtest/gtest.h>

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

bytedance::bolt::config::ConfigBase makeConfig(std::unordered_map<std::string, std::string> values) {
  return bytedance::bolt::config::ConfigBase(std::move(values));
}

template <typename Func>
void expectFailureWithMessage(Func&& func, const std::string& expectedMessage) {
  try {
    func();
    FAIL() << "Expected an exception containing: " << expectedMessage;
  } catch (const std::exception& error) {
    EXPECT_NE(std::string(error.what()).find(expectedMessage), std::string::npos) << error.what();
  }
}

} // namespace

TEST(PaimonConnectorUtilsTest, SelectsSerializedInfoWithoutAnyPathIndexedState) {
  auto info = std::make_shared<paimon::PaimonSplitInfo>(SplitInfo{});
  info->mode = paimon::PaimonSplitMode::kSerialized;
  const std::string binaryPayload{"a\0b\377", 4};
  info->serializedSplits = {binaryPayload, "second"};

  ASSERT_TRUE(info->paths.empty());
  ASSERT_TRUE(info->starts.empty());
  ASSERT_TRUE(info->lengths.empty());
  ASSERT_TRUE(info->properties.empty());
  ASSERT_TRUE(info->partitionColumns.empty());
  ASSERT_TRUE(info->metadataColumns.empty());

  const auto selected = paimon::tryMakeSerializedConnectorSplits(info);

  ASSERT_TRUE(selected.has_value());
  const auto& splits = selected.value();
  ASSERT_EQ(splits.size(), 2);
  const auto first = std::dynamic_pointer_cast<bytedance::bolt::connector::paimon::PaimonConnectorSplit>(splits[0]);
  const auto second = std::dynamic_pointer_cast<bytedance::bolt::connector::paimon::PaimonConnectorSplit>(splits[1]);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(first->connectorId, kPaimonConnectorId);
  EXPECT_EQ(second->connectorId, kPaimonConnectorId);
  EXPECT_EQ(first->splitBytes_, binaryPayload);
  EXPECT_EQ(second->splitBytes_, "second");
}

TEST(PaimonConnectorUtilsTest, DoesNotSelectPlainUnsetOrLegacySplitInfo) {
  EXPECT_FALSE(paimon::tryMakeSerializedConnectorSplits(std::make_shared<SplitInfo>()).has_value());

  auto info = std::make_shared<paimon::PaimonSplitInfo>(SplitInfo{});
  EXPECT_FALSE(paimon::tryMakeSerializedConnectorSplits(info).has_value());
  info->mode = paimon::PaimonSplitMode::kLegacy;
  EXPECT_FALSE(paimon::tryMakeSerializedConnectorSplits(info).has_value());
}

TEST(PaimonConnectorUtilsTest, RejectsDirectBuilderUseForNonSerializedModes) {
  paimon::PaimonSplitInfo info{SplitInfo{}};
  for (const auto mode : {paimon::PaimonSplitMode::kUnset, paimon::PaimonSplitMode::kLegacy}) {
    info.mode = mode;
    expectFailureWithMessage(
        [&] { paimon::makeConnectorSplits(info); }, "Native Paimon connector splits require serialized mode");
  }
}

TEST(PaimonConnectorUtilsTest, PreservesEmptySerializedTaskPartition) {
  auto info = std::make_shared<paimon::PaimonSplitInfo>(SplitInfo{});
  info->mode = paimon::PaimonSplitMode::kSerialized;

  const auto splits = paimon::tryMakeSerializedConnectorSplits(info);
  ASSERT_TRUE(splits.has_value());
  EXPECT_TRUE(splits->empty());
}

TEST(PaimonConnectorUtilsTest, RejectsEmptySerializedSplitPayload) {
  auto info = std::make_shared<paimon::PaimonSplitInfo>(SplitInfo{});
  info->mode = paimon::PaimonSplitMode::kSerialized;
  info->serializedSplits.emplace_back();

  expectFailureWithMessage(
      [&] { paimon::tryMakeSerializedConnectorSplits(info); }, "Serialized Paimon split payload must not be empty");
}

TEST(PaimonConnectorUtilsTest, MapsEveryPresentSparkPaimonConfigToItsBoltKey) {
  std::unordered_map<std::string, std::string> sourceValues;
  std::unordered_map<std::string, std::string> expectedValues;
  for (size_t i = 0; i < kConfigMappings.size(); ++i) {
    const auto value = "value-" + std::to_string(i);
    sourceValues.emplace(kConfigMappings[i].first, value);
    expectedValues.emplace(kConfigMappings[i].second, value);
  }

  const auto config = paimon::makeConnectorConfig(makeConfig(std::move(sourceValues)));

  EXPECT_EQ(config->rawConfigs(), expectedValues);
}

TEST(PaimonConnectorUtilsTest, CopiesOnlyPresentPaimonConfigKeys) {
  auto config = paimon::makeConnectorConfig(makeConfig({
      {"spark.gluten.paimon.read.prefetch-enabled", "true"},
      {"unrelated.key", "unrelated-value"},
  }));

  EXPECT_EQ(
      config->rawConfigs(),
      (std::unordered_map<std::string, std::string>{
          {PaimonConfig::kReadTimestampUnit, "6"}, {PaimonConfig::kPrefetchEnabled, "true"}}));
}

TEST(PaimonConnectorUtilsTest, PreservesSparkTimestampPrecisionByDefault) {
  const auto config = paimon::makeConnectorConfig(makeConfig({}));

  EXPECT_EQ(PaimonConfig(config).readTimestampUnit(), 6);
}

TEST(PaimonConnectorUtilsTest, PreservesValuesAlreadySetUnderBoltPaimonKeys) {
  std::unordered_map<std::string, std::string> values;
  for (size_t i = 0; i < kConfigMappings.size(); ++i) {
    values.emplace(kConfigMappings[i].first, "spark-" + std::to_string(i));
    values[kConfigMappings[i].second] = "bolt-" + std::to_string(i);
  }

  const auto config = paimon::makeConnectorConfig(makeConfig(std::move(values)));

  for (size_t i = 0; i < kConfigMappings.size(); ++i) {
    EXPECT_EQ(config->get<std::string>(kConfigMappings[i].second).value(), "bolt-" + std::to_string(i));
  }
}

} // namespace gluten
