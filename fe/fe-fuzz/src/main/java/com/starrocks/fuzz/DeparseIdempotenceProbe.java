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

package com.starrocks.fuzz;

import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;

/**
 * Minimal reducer for round-trip violations found by {@link RoundTripFidelityChecker}: repeatedly
 * applies {@code toSQL(parse(s))} and prints each round, so unbounded growth and reparse breakage
 * are visible directly.
 *
 * <p>Usage: {@code DeparseIdempotenceProbe [rounds] ["<sql>" ...]} — with no SQL arguments it runs
 * the regression set of currently-known violations.
 */
public class DeparseIdempotenceProbe {

    private static final String[] KNOWN_VIOLATIONS = {
            // P0-1: +2 parentheses per round, unbounded
            "select v1 from t0 where v2 in (select v3 from t1)",
            // P0-2: deparses to `INSERT INTO t (cols) (VALUES(...))`, which does not parse
            "insert into t (id, val) values (1, 111)",
            // P0-3: map literal prints the Java type object
            "select map1 in (map{}) from sc2",
            // controls that must stay stable
            "select * from t0 where v1 in (1,2,3)",
            "select (v1) from t0",
            "select v1 from t0 where exists (select 1 from t1)",
    };

    public static void main(String[] args) {
        int rounds = 5;
        String[] cases = KNOWN_VIOLATIONS;
        if (args.length > 0) {
            rounds = Integer.parseInt(args[0]);
            if (args.length > 1) {
                cases = new String[args.length - 1];
                System.arraycopy(args, 1, cases, 0, args.length - 1);
            }
        }

        int violations = 0;
        for (String sql : cases) {
            System.out.println("### " + sql);
            String cur = sql;
            String prev = null;
            boolean stable = true;
            for (int i = 1; i <= rounds; i++) {
                try {
                    StatementBase ast = SqlParser.parse(cur, SqlModeHelper.MODE_DEFAULT).get(0);
                    cur = AstToSQLBuilder.toSQL(ast);
                } catch (Throwable t) {
                    System.out.printf("  round %d FAILED: %s: %s%n", i, t.getClass().getSimpleName(),
                            String.valueOf(t.getMessage()).replace('\n', ' '));
                    stable = false;
                    break;
                }
                System.out.printf("  round %d (len=%3d): %s%n", i, cur.length(), cur.replace('\n', ' '));
                if (prev != null && !prev.equals(cur)) {
                    stable = false;
                }
                prev = cur;
            }
            System.out.println(stable ? "  => STABLE" : "  => VIOLATION");
            if (!stable) {
                violations++;
            }
            System.out.println();
        }
        System.out.println("violations: " + violations + "/" + cases.length);
    }
}
