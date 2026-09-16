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

#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

#include <gtest/gtest.h>
#include <paimon/memory/memory_pool.h>
#include <paimon/table/source/data_split.h>
#include <paimon/table/source/split.h>

namespace gluten {
namespace {

std::filesystem::path fixturePath() {
  return std::filesystem::path(__FILE__).parent_path().parent_path().parent_path().parent_path() /
      "backends-bolt/src-paimon/test/resources/paimon-split-v8.bin";
}

std::string readFixture(const std::filesystem::path& fixture) {
  std::ifstream input(fixture, std::ios::binary);
  if (!input.is_open()) {
    return {};
  }
  return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

TEST(PaimonSplitCompatibilityTest, JavaV8FixtureDeserializesWithFiles) {
  const auto fixture = fixturePath();
  const auto bytes = readFixture(fixture);
  ASSERT_FALSE(bytes.empty()) << "Unable to read Paimon split fixture: " << fixture;

  auto paimonPool = ::paimon::GetDefaultPool();
  auto result = ::paimon::Split::Deserialize(bytes.data(), bytes.size(), paimonPool);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  auto dataSplit = std::dynamic_pointer_cast<::paimon::DataSplit>(result.value());
  ASSERT_NE(dataSplit, nullptr);
  EXPECT_FALSE(dataSplit->GetFileList().empty());
}

TEST(PaimonSplitCompatibilityTest, RejectsTruncatedJavaV8Fixture) {
  auto bytes = readFixture(fixturePath());
  ASSERT_GT(bytes.size(), 1);
  bytes.resize(bytes.size() / 2);

  auto result = ::paimon::Split::Deserialize(bytes.data(), bytes.size(), ::paimon::GetDefaultPool());
  EXPECT_FALSE(result.ok());
}

} // namespace
} // namespace gluten
