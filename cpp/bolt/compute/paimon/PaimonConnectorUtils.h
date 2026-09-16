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

#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "bolt/common/config/Config.h"
#include "bolt/connectors/Connector.h"
#include "compute/paimon/PaimonPlanUtils.h"

namespace gluten {

namespace paimon {

// Gluten-to-connector handoff of the query-bound TableSchema JSON. A connector
// must explicitly consume this property to bind its read context to that schema;
// forwarding it as a generic Paimon option does not provide that guarantee.
inline constexpr char kReadTableSchema[] = "gluten.paimon.read.table-schema";

std::vector<std::shared_ptr<bytedance::bolt::connector::ConnectorSplit>> makeConnectorSplits(
    const PaimonSplitInfo& info);

std::optional<std::vector<std::shared_ptr<bytedance::bolt::connector::ConnectorSplit>>>
tryMakeSerializedConnectorSplits(const std::shared_ptr<SplitInfo>& info);

std::shared_ptr<bytedance::bolt::config::ConfigBase> makeConnectorConfig(
    const bytedance::bolt::config::ConfigBase& queryConfig);

} // namespace paimon
} // namespace gluten
