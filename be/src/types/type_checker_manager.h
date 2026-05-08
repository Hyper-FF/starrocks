// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include <memory>
#include <string>
#include <unordered_map>

#include "types/checker/type_checker.h"

namespace starrocks {

class TypeCheckerManager {
private:
    std::unordered_map<std::string, std::unique_ptr<TypeChecker>> _checkers;
    std::unique_ptr<TypeChecker> _default_checker;
    TypeCheckerManager();

    /**
     * Attempt to load type checkers from XML configuration.
     * All type checkers must be defined in XML.
     *
     * @param xml_file_path Path to the XML configuration file
     * @return true if XML was successfully loaded, false otherwise
     */
    bool try_load_from_xml(const std::string& xml_file_path);

    /**
     * Register the built-in hardcoded type checkers. Used as a fallback when
     * the XML configuration is unavailable so that JDBC scans keep working
     * even on deployments where conf/type_checker_config.xml is missing or
     * cannot be parsed.
     */
    void register_hardcoded_defaults();

public:
    TypeCheckerManager(const TypeCheckerManager&) = delete;
    TypeCheckerManager& operator=(const TypeCheckerManager&) = delete;

    static TypeCheckerManager& getInstance();

    void init();

    void registerChecker(const std::string& java_class, std::unique_ptr<TypeChecker> checker);
    StatusOr<LogicalType> checkType(const std::string& java_class, const SlotDescriptor* slot_desc);
};

} // namespace starrocks