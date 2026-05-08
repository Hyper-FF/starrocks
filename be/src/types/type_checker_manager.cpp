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

#include "types/type_checker_manager.h"

#include <cstdlib>

#include "checker/type_checker.h"
#include "checker/type_checker_xml_loader.h"
#include "common/logging.h"

namespace starrocks {

TypeCheckerManager::TypeCheckerManager() {
    init();
}

bool TypeCheckerManager::try_load_from_xml(const std::string& xml_file_path) {
    auto mappings_or = TypeCheckerXMLLoader::load_from_xml(xml_file_path);
    if (!mappings_or.ok()) {
        LOG(WARNING) << "Failed to load type checker configuration from XML: " << mappings_or.status().message();
        return false;
    }

    const auto& mappings = mappings_or.value();
    size_t loaded_count = 0;
    for (const auto& mapping : mappings) {
        auto checker = TypeCheckerXMLLoader::create_checker_from_mapping(mapping);
        if (checker == nullptr) {
            LOG(WARNING) << "Failed to create checker for: " << mapping.java_class;
            continue;
        }
        if (mapping.java_class == "*") {
            _default_checker = std::move(checker);
            continue;
        }
        registerChecker(mapping.java_class, std::move(checker));
        loaded_count++;
    }

    if (loaded_count == 0) {
        LOG(WARNING) << "No valid type checkers were loaded from XML configuration";
        return false;
    }

    return true;
}

TypeCheckerManager& TypeCheckerManager::getInstance() {
    static TypeCheckerManager instance;
    return instance;
}

void TypeCheckerManager::init() {
    // Load type checkers from XML configuration file
    // Location: conf/type_checker_config.xml (relative to BE home)
    std::string xml_path;

    const char* be_home = std::getenv("STARROCKS_HOME");
    if (be_home != nullptr) {
        xml_path = std::string(be_home) + "/conf/type_checker_config.xml";
    } else {
        // Use relative path as fallback
        xml_path = "conf/type_checker_config.xml";
    }

    // Try XML first; fall back to hardcoded defaults so that JDBC scans keep
    // working when the XML file is missing or fails to parse. Without this
    // fallback, every JDBC type check fails with "No type checker found for
    // Java class: ..." (commonly java.math.BigDecimal for Oracle NUMBER
    // columns, since BigDecimal is typically the first column type checked).
    if (try_load_from_xml(xml_path)) {
        LOG(INFO) << "TypeCheckerManager initialized from XML configuration: " << xml_path;
    } else {
        LOG(WARNING) << "Failed to load type checker configuration from XML: " << xml_path
                     << ". Falling back to hardcoded type checkers.";
        register_hardcoded_defaults();
    }
}

void TypeCheckerManager::register_hardcoded_defaults() {
    using TypeRule = ConfigurableTypeChecker::TypeRule;

    auto make_checker = [](std::string display_name, std::vector<TypeRule> rules) {
        return std::make_unique<ConfigurableTypeChecker>(std::move(display_name), std::move(rules));
    };

    registerChecker("java.lang.Byte",
                    make_checker("Byte", {{TYPE_BOOLEAN, TYPE_BOOLEAN},
                                          {TYPE_TINYINT, TYPE_TINYINT},
                                          {TYPE_SMALLINT, TYPE_TINYINT},
                                          {TYPE_INT, TYPE_TINYINT},
                                          {TYPE_BIGINT, TYPE_TINYINT}}));

    registerChecker("java.lang.Short", make_checker("Short", {{TYPE_TINYINT, TYPE_SMALLINT},
                                                              {TYPE_SMALLINT, TYPE_SMALLINT},
                                                              {TYPE_INT, TYPE_SMALLINT},
                                                              {TYPE_BIGINT, TYPE_SMALLINT}}));

    registerChecker("java.lang.Integer", make_checker("Integer", {{TYPE_TINYINT, TYPE_INT},
                                                                  {TYPE_SMALLINT, TYPE_INT},
                                                                  {TYPE_INT, TYPE_INT},
                                                                  {TYPE_BIGINT, TYPE_INT}}));

    registerChecker("java.lang.Long", make_checker("Long", {{TYPE_BIGINT, TYPE_BIGINT}}));

    registerChecker("java.math.BigInteger",
                    make_checker("BigInteger", {{TYPE_LARGEINT, TYPE_VARCHAR}, {TYPE_VARCHAR, TYPE_VARCHAR}}));

    registerChecker("java.lang.Boolean", make_checker("Boolean", {{TYPE_BOOLEAN, TYPE_BOOLEAN},
                                                                  {TYPE_SMALLINT, TYPE_BOOLEAN},
                                                                  {TYPE_INT, TYPE_BOOLEAN},
                                                                  {TYPE_BIGINT, TYPE_BOOLEAN}}));

    registerChecker("java.lang.Float", make_checker("Float", {{TYPE_FLOAT, TYPE_FLOAT}}));

    registerChecker("java.lang.Double",
                    make_checker("Double", {{TYPE_DOUBLE, TYPE_DOUBLE}, {TYPE_FLOAT, TYPE_DOUBLE}}));

    registerChecker("java.lang.String", make_checker("String", {{TYPE_CHAR, TYPE_VARCHAR},
                                                                {TYPE_VARCHAR, TYPE_VARCHAR},
                                                                {TYPE_JSON, TYPE_VARCHAR}}));

    registerChecker("java.sql.Timestamp",
                    make_checker("Timestamp", {{TYPE_DATETIME, TYPE_VARCHAR}, {TYPE_VARCHAR, TYPE_VARCHAR}}));

    registerChecker("java.sql.Date", make_checker("Date", {{TYPE_DATE, TYPE_VARCHAR}}));

    registerChecker("java.sql.Time", make_checker("Time", {{TYPE_TIME, TYPE_TIME}}));

    registerChecker("java.time.LocalDateTime",
                    make_checker("LocalDateTime", {{TYPE_DATETIME, TYPE_VARCHAR}}));

    registerChecker("java.time.LocalDate", make_checker("LocalDate", {{TYPE_DATE, TYPE_VARCHAR}}));

    registerChecker("java.math.BigDecimal", make_checker("BigDecimal", {{TYPE_DECIMAL32, TYPE_VARCHAR},
                                                                        {TYPE_DECIMAL64, TYPE_VARCHAR},
                                                                        {TYPE_DECIMAL128, TYPE_VARCHAR},
                                                                        {TYPE_DECIMAL256, TYPE_VARCHAR},
                                                                        {TYPE_VARCHAR, TYPE_VARCHAR},
                                                                        {TYPE_DOUBLE, TYPE_VARCHAR}}));

    registerChecker("oracle.sql.TIMESTAMP", make_checker("OracleTimestamp", {{TYPE_VARCHAR, TYPE_VARCHAR},
                                                                             {TYPE_DATETIME, TYPE_VARCHAR}}));
    registerChecker("oracle.sql.TIMESTAMPLTZ", make_checker("OracleTimestampLTZ", {{TYPE_VARCHAR, TYPE_VARCHAR},
                                                                                   {TYPE_DATETIME, TYPE_VARCHAR}}));
    registerChecker("oracle.sql.TIMESTAMPTZ", make_checker("OracleTimestampTZ", {{TYPE_VARCHAR, TYPE_VARCHAR},
                                                                                 {TYPE_DATETIME, TYPE_VARCHAR}}));

    registerChecker("microsoft.sql.DateTimeOffset",
                    make_checker("DateTimeOffset", {{TYPE_VARCHAR, TYPE_VARCHAR}}));

    registerChecker("org.postgresql.util.PGobject",
                    make_checker("PostgreSQLPGobject", {{TYPE_JSON, TYPE_VARCHAR}, {TYPE_VARCHAR, TYPE_VARCHAR}}));

    registerChecker("byte[]", make_checker("ByteArray", {{TYPE_BINARY, TYPE_VARBINARY},
                                                         {TYPE_VARBINARY, TYPE_VARBINARY}}));
    registerChecker("oracle.jdbc.OracleBlob",
                    make_checker("OracleBlob", {{TYPE_BINARY, TYPE_VARBINARY},
                                                {TYPE_VARBINARY, TYPE_VARBINARY}}));
    registerChecker("[B", make_checker("ByteArrayB", {{TYPE_BINARY, TYPE_VARBINARY},
                                                      {TYPE_VARBINARY, TYPE_VARBINARY}}));
    registerChecker("java.util.UUID", make_checker("UUID", {{TYPE_BINARY, TYPE_VARBINARY},
                                                            {TYPE_VARBINARY, TYPE_VARBINARY}}));

    registerChecker("com.clickhouse.data.value.UnsignedByte",
                    make_checker("UnsignedByte", {{TYPE_SMALLINT, TYPE_SMALLINT},
                                                  {TYPE_INT, TYPE_SMALLINT},
                                                  {TYPE_BIGINT, TYPE_SMALLINT}}));
    registerChecker("com.clickhouse.data.value.UnsignedShort",
                    make_checker("UnsignedShort", {{TYPE_INT, TYPE_INT}, {TYPE_BIGINT, TYPE_INT}}));
    registerChecker("com.clickhouse.data.value.UnsignedInteger",
                    make_checker("UnsignedInteger", {{TYPE_BIGINT, TYPE_BIGINT}}));
    registerChecker("com.clickhouse.data.value.UnsignedLong",
                    make_checker("UnsignedLong", {{TYPE_LARGEINT, TYPE_VARCHAR}}));

    _default_checker = make_checker("Default", {{TYPE_VARCHAR, TYPE_VARCHAR},
                                                {TYPE_BINARY, TYPE_VARCHAR},
                                                {TYPE_VARBINARY, TYPE_VARCHAR}});
}

void TypeCheckerManager::registerChecker(const std::string& java_class, std::unique_ptr<TypeChecker> checker) {
    _checkers.emplace(java_class, std::move(checker));
}

StatusOr<LogicalType> TypeCheckerManager::checkType(const std::string& java_class, const SlotDescriptor* slot_desc) {
    auto it = _checkers.find(java_class);
    if (it != _checkers.end()) {
        return it->second->check(java_class, slot_desc);
    }
    if (_default_checker != nullptr) {
        return _default_checker->check(java_class, slot_desc);
    }
    return Status::NotFound("No type checker found for Java class: " + java_class);
}

} // namespace starrocks