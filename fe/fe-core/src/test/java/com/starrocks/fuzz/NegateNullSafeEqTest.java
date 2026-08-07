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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * {@code WHERE NOT (a <=> b AND ...)} throws IllegalStateException out of the analyzer.
 *
 * <p>Found by the AST fuzzer, from a production seed that already contained {@code <=>}: M11 negated
 * its WHERE clause and the analyzer failed with {@code IllegalStateException: Not implemented}.
 *
 * <p>The mechanism is a guard applied at one level and skipped one level down. Every query's WHERE
 * goes through {@code SelectAnalyzer#analyzeWhere} -> {@code ExprUtils.pushNegationToOperands}, which
 * checks {@code ExprNegateFunction.isSupportNegate(root.getChild(0))} before negating.
 * {@code isSupportNegate} lists the six comparison operators it can invert and correctly leaves out
 * {@code EQ_FOR_NULL}, so {@code NOT (a <=> b)} is safe. But when the child is a CompoundPredicate the
 * guard passes, and {@code negateCompoundPredicate} then recurses with a bare {@code negate()} on each
 * operand -- no second check -- so a {@code <=>} nested one level inside the NOT reaches
 * {@code negateBinaryPredicate}, whose switch has no case for it and falls to
 * {@code default: throw new IllegalStateException("Not implemented")}.
 *
 * <p>An IllegalStateException is not a user error: it is an internal error escaping on valid SQL,
 * and {@code <=>} is ordinary null-safe equality that production queries use.
 */
public class NegateNullSafeEqTest {

    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert srAssert = new StarRocksAssert(ctx);
        srAssert.withDatabase("negate_db").useDatabase("negate_db");
        srAssert.withTable("create table t (k1 int, k2 int, k3 boolean) "
                + "duplicate key(k1) distributed by hash(k1) buckets 1 "
                + "properties('replication_num'='1')");
    }

    private static Throwable analyze(String sql) {
        try {
            StatementBase stmt = SqlParser.parse(sql, ctx.getSessionVariable()).get(0);
            ctx.setThreadLocalInfo();
            Analyzer.analyze(stmt, ctx);
            return null;
        } catch (Throwable t) {
            return t;
        }
    }

    /** The guard works when the null-safe comparison is the NOT's direct child. */
    @Test
    public void negatingANullSafeEqualityAloneIsSafe() {
        Throwable t = analyze("select k1 from t where not (k1 <=> k2)");
        Assertions.assertNull(t, () -> "the top-level guard regressed: " + t);
    }

    /** One level deeper, where the guard used not to be re-applied. */
    @Test
    public void negatingANullSafeEqualityInsideACompoundIsSafeToo() {
        for (String sql : new String[] {
                "select k1 from t where not (k1 <=> k2 and k1 > 0)",
                "select k1 from t where not (k1 > 0 and k1 <=> k2)",
                "select k1 from t where not (k1 <=> k2 or k1 > 0)",
                "select k1 from t where not (k3 <=> true and k1 > 0)",
        }) {
            Throwable t = analyze(sql);
            // Clean analysis, not merely a different exception. The first version of this test
            // asserted a failure was expected and only its TYPE was wrong -- written while the bug
            // was live, when every one of these threw. With the gate in place they are ordinary
            // valid SQL and anything thrown here is a regression.
            Assertions.assertNull(t, () -> "valid SQL failed to analyze: " + sql + " -> " + t
                    + (t instanceof IllegalStateException
                    ? "  (negateCompoundPredicate recursed past isSupportNegate again)" : ""));
        }
    }
}
