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

#include "compute/paimon/PaimonPlanUtils.h"
#include "PaimonTableEnhancement.pb.h"
#include "compute/BoltPlanConverter.h"

#include <gtest/gtest.h>

namespace gluten::paimon {
namespace {

using FileOrFiles = substrait::ReadRel_LocalFiles_FileOrFiles;
using LocalFiles = substrait::ReadRel_LocalFiles;

::substrait::Plan makeScanPlan(const PaimonTableEnhancement* enhancement = nullptr) {
  ::substrait::Plan plan;
  auto* read = plan.add_relations()->mutable_root()->mutable_input()->mutable_read();
  read->mutable_base_schema()->mutable_struct_();
  read->mutable_named_table()->add_names("paimon_table");
  if (enhancement != nullptr) {
    read->mutable_advanced_extension()->mutable_enhancement()->PackFrom(*enhancement);
  }
  return plan;
}

class ConverterHarness {
 public:
  ConverterHarness()
      : pool_(bytedance::bolt::memory::MemoryManager::getInstance()->addLeafPool("paimon-plan-conversion")),
        config_(std::unordered_map<std::string, std::string>{}),
        converter_({}, pool_.get(), &config_) {}

  std::shared_ptr<SplitInfo> convert(
      std::vector<LocalFiles> localFiles,
      const PaimonTableEnhancement* enhancement = nullptr) {
    auto planNode = converter_.toBoltPlan(makeScanPlan(enhancement), std::move(localFiles));
    return converter_.splitInfos().at(planNode->id());
  }

 private:
  std::shared_ptr<bytedance::bolt::memory::MemoryPool> pool_;
  bytedance::bolt::config::ConfigBase config_;
  BoltPlanConverter converter_;
};

class PaimonPlanConversionTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    bytedance::bolt::memory::MemoryManager::testingSetInstance(bytedance::bolt::memory::MemoryManager::Options{});
  }
};

FileOrFiles makeSerializedPaimonFile(const std::string& bytes) {
  FileOrFiles file;
  file.mutable_paimon()->set_serialized_split(bytes);
  return file;
}

FileOrFiles makeLegacyPaimonFile(const std::string& path) {
  FileOrFiles file;
  file.set_uri_file(path);
  file.mutable_paimon()->mutable_parquet();
  return file;
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

TEST(PaimonPlanUtilsTest, PreservesBinarySerializedSplit) {
  const std::string bytes{"\0\1\377split", 8};

  auto info = PaimonPlanUtils::parsePaimonSplitInfo(makeSerializedPaimonFile(bytes), std::make_shared<SplitInfo>());

  EXPECT_EQ(info->mode, PaimonSplitMode::kSerialized);
  ASSERT_EQ(info->serializedSplits.size(), 1);
  EXPECT_EQ(info->serializedSplits.front(), bytes);
  EXPECT_NO_THROW(PaimonPlanUtils::validate(*info, 1));
}

TEST(PaimonPlanUtilsTest, PreservesLegacyPaimonParsing) {
  const std::string path = "file:///tmp/data.parquet";
  auto info = PaimonPlanUtils::parsePaimonSplitInfo(makeLegacyPaimonFile(path), std::make_shared<SplitInfo>());

  EXPECT_EQ(info->mode, PaimonSplitMode::kLegacy);
  EXPECT_EQ(info->metaByPath_.at(path).format, dwio::common::FileFormat::PARQUET);
  EXPECT_TRUE(info->serializedSplits.empty());
  EXPECT_NO_THROW(PaimonPlanUtils::validate(*info, 2));
}

TEST(PaimonPlanUtilsTest, RejectsSerializedThenLegacyMode) {
  auto info =
      PaimonPlanUtils::parsePaimonSplitInfo(makeSerializedPaimonFile("serialized"), std::make_shared<SplitInfo>());

  expectFailureWithMessage(
      [&] { PaimonPlanUtils::parsePaimonSplitInfo(makeLegacyPaimonFile("file:///tmp/data.parquet"), info); },
      "Paimon scan cannot mix serialized and legacy splits");
}

TEST(PaimonPlanUtilsTest, RejectsLegacyThenSerializedMode) {
  auto info = PaimonPlanUtils::parsePaimonSplitInfo(
      makeLegacyPaimonFile("file:///tmp/data.parquet"), std::make_shared<SplitInfo>());

  expectFailureWithMessage(
      [&] { PaimonPlanUtils::parsePaimonSplitInfo(makeSerializedPaimonFile("serialized"), info); },
      "Paimon scan cannot mix serialized and legacy splits");
}

TEST(PaimonPlanUtilsTest, DefensivelyRejectsConstructedEmptySerializedPayload) {
  // The parser classifies empty protobuf bytes as legacy. This guards split info
  // constructed or modified outside the parser.
  PaimonSplitInfo info{SplitInfo{}};
  info.mode = PaimonSplitMode::kSerialized;
  info.serializedSplits.emplace_back();

  expectFailureWithMessage(
      [&] { PaimonPlanUtils::validate(info, 1); }, "Serialized Paimon split payload must not be empty");
}

TEST(PaimonPlanUtilsTest, RejectsSerializedSplitCountMismatch) {
  auto info =
      PaimonPlanUtils::parsePaimonSplitInfo(makeSerializedPaimonFile("serialized"), std::make_shared<SplitInfo>());

  expectFailureWithMessage(
      [&] { PaimonPlanUtils::validate(*info, 2); }, "Serialized Paimon split count must match file count");
}

TEST(PaimonPlanUtilsTest, AllowsUnsetModeForEmptyFileList) {
  const PaimonSplitInfo info{SplitInfo{}};

  EXPECT_NO_THROW(PaimonPlanUtils::validate(info, 0));
}

TEST_F(PaimonPlanConversionTest, AccumulatesSerializedSplitsInFileOrder) {
  const std::string first{"\0first", 6};
  const std::string second{"second\377", 7};
  PaimonTableEnhancement enhancement;
  enhancement.set_table_path("file:///tmp/warehouse/paimon-table");
  (*enhancement.mutable_table_properties())["bucket"] = "2";
  std::vector<LocalFiles> localFiles(1);
  *localFiles.front().add_items() = makeSerializedPaimonFile(first);
  *localFiles.front().add_items() = makeSerializedPaimonFile(second);

  ConverterHarness harness;
  auto info = std::dynamic_pointer_cast<PaimonSplitInfo>(harness.convert(std::move(localFiles), &enhancement));

  ASSERT_NE(info, nullptr);
  EXPECT_EQ(info->mode, PaimonSplitMode::kSerialized);
  EXPECT_EQ(info->serializedSplits, (std::vector<std::string>{first, second}));
}

TEST_F(PaimonPlanConversionTest, RejectsNonPaimonFileAfterSerializedSplit) {
  std::vector<LocalFiles> localFiles(1);
  *localFiles.front().add_items() = makeSerializedPaimonFile("serialized");
  localFiles.front().add_items()->set_uri_file("file:///tmp/non-paimon");

  ConverterHarness harness;
  expectFailureWithMessage(
      [&] { harness.convert(std::move(localFiles)); }, "Serialized Paimon split count must match file count");
}

TEST_F(PaimonPlanConversionTest, PreservesEmptyFileList) {
  ConverterHarness harness;
  auto info = harness.convert(std::vector<LocalFiles>(1));

  ASSERT_NE(info, nullptr);
  EXPECT_EQ(info->leafType, SplitInfo::LeafType::TABLE_SCAN);
  EXPECT_TRUE(info->paths.empty());
  EXPECT_EQ(std::dynamic_pointer_cast<PaimonSplitInfo>(info), nullptr);
}

TEST_F(PaimonPlanConversionTest, TreatsEmptySerializedBytesAsLegacy) {
  // Proto3 bytes have no presence bit, so an empty value selects legacy mode.
  std::vector<LocalFiles> localFiles(1);
  auto legacyFile = makeLegacyPaimonFile("file:///tmp/data.parquet");
  legacyFile.mutable_paimon()->set_serialized_split(std::string{});
  *localFiles.front().add_items() = std::move(legacyFile);

  ConverterHarness harness;
  auto info = std::dynamic_pointer_cast<PaimonSplitInfo>(harness.convert(std::move(localFiles)));

  ASSERT_NE(info, nullptr);
  EXPECT_EQ(info->mode, PaimonSplitMode::kLegacy);
  EXPECT_TRUE(info->serializedSplits.empty());
}

} // namespace
} // namespace gluten::paimon
