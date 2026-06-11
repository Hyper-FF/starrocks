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

#include "exec/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

class SchemaTaskRunsScanner : public SchemaScanner {
public:
    SchemaTaskRunsScanner();
    ~SchemaTaskRunsScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    Status fill_chunk(ChunkPtr* chunk);
    // Fetch the next batch of task runs from the FE by advancing the pagination offset.
    Status _fetch_next_batch();

    // Number of task runs requested from the FE per RPC. The scanner keeps fetching batches until
    // the FE returns an empty page (or the query LIMIT is satisfied) instead of pulling everything
    // in a single RPC.
    static constexpr int64_t kFetchBatchSize = 1000;

    int _task_run_index{0};
    // Offset of the next batch to fetch, advanced by the requested batch size each RPC.
    int64_t _task_run_offset{0};
    bool _no_more{false};
    TGetTasksParams _task_params;
    TGetTaskRunInfoResult _task_run_result;
    static SchemaScanner::ColumnDesc _s_tbls_columns[];
};

} // namespace starrocks
