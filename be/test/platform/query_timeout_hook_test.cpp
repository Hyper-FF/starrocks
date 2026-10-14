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

#include "platform/query_timeout_hook.h"

#include <gtest/gtest.h>
#include <jemalloc/jemalloc.h>
#include <stdlib.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "common/config_diagnostic_fwd.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {

namespace {

// Number of mappings flagged MADV_DONTDUMP ("dd" in VmFlags) in this process.
int count_dontdump_mappings() {
    std::ifstream smaps("/proc/self/smaps");
    std::string line;
    int count = 0;
    while (std::getline(smaps, line)) {
        if (line.rfind("VmFlags:", 0) == 0 && line.find(" dd") != std::string::npos) {
            ++count;
        }
    }
    return count;
}

} // namespace

// The timeout hook dumps a core of a process that keeps running. Marking jemalloc's retained pages DONTDUMP there
// splits its mappings at every retained/active boundary for the rest of the process's life.
TEST(QueryTimeoutHookTest, does_not_mark_retained_pages_dontdump) {
    if (!std::filesystem::exists("/proc/self/smaps")) {
        GTEST_SKIP() << "needs /proc/self/smaps";
    }
    auto dir = std::filesystem::temp_directory_path() / "query_timeout_hook_test";
    std::filesystem::create_directories(dir);

    auto old_threshold = config::pipeline_gcore_timeout_threshold_sec;
    auto old_optimization = config::enable_core_file_size_optimization;
    auto old_output_dir = config::pipeline_gcore_output_dir;
    const char* old_path_env = getenv("PATH");
    std::string old_path = old_path_env != nullptr ? old_path_env : "";

    config::pipeline_gcore_timeout_threshold_sec = 1;
    config::enable_core_file_size_optimization = true;
    config::pipeline_gcore_output_dir = dir.string();
    // Keep gcore out of reach so the test does not dump its own process.
    setenv("PATH", dir.c_str(), 1);

    // Free every other block so that retained extents are interleaved with extents in use.
    constexpr size_t kBlocks = 4096;
    constexpr size_t kBlockSize = 64 * 1024;
    std::vector<void*> blocks(kBlocks);
    for (auto& block : blocks) {
        block = je_malloc(kBlockSize);
        memset(block, 1, kBlockSize);
    }
    for (size_t i = 0; i < kBlocks; i += 2) {
        je_free(blocks[i]);
        blocks[i] = nullptr;
    }

    int before = count_dontdump_mappings();
    TUniqueId query_id;
    query_id.__set_hi(1);
    query_id.__set_lo(2);
    hook_on_query_timeout(query_id, 2);
    int after = count_dontdump_mappings();

    for (void* block : blocks) {
        je_free(block);
    }
    setenv("PATH", old_path.c_str(), 1);
    config::pipeline_gcore_timeout_threshold_sec = old_threshold;
    config::enable_core_file_size_optimization = old_optimization;
    config::pipeline_gcore_output_dir = old_output_dir;
    std::filesystem::remove_all(dir);

    EXPECT_LE(after, before);
}

} // namespace starrocks
