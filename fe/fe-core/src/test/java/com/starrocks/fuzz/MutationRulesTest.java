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
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.SqlParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

/**
 * The rule set is configuration, and configuration parsing is easy to get subtly wrong: a rule that
 * silently matches nothing leaves the artifacts in the report, and one that matches everything hides
 * real findings. These pin the semantics down.
 */
public class MutationRulesTest {

    /** The WHERE predicate of the parsed statement, unanalyzed — the shape the mutator sees. */
    private static Expr predicateOf(String sql) {
        StatementBase stmt = SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        return ((SelectRelation) ((QueryStatement) stmt).getQueryRelation()).getPredicate();
    }

    private static MutationRules rulesFrom(String xml) throws Exception {
        return MutationRules.forTesting(
                MutationRules.parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))));
    }

    @Test
    public void testParentTypeRuleBlocksEveryChild() throws Exception {
        MutationRules rules = rulesFrom("<fuzz-rules>"
                + "<rule name='r' action='skip'><match parent-type='ExistsPredicate'/></rule>"
                + "</fuzz-rules>");
        Expr exists = predicateOf("select * from t where exists (select 1 from s)");
        Assertions.assertTrue(rules.isBlocked(exists, 0));

        // A different parent is untouched.
        Expr compare = predicateOf("select * from t where a > 1");
        Assertions.assertFalse(rules.isBlocked(compare, 0));
        Assertions.assertFalse(rules.isBlocked(compare, 1));
    }

    @Test
    public void testChildTypeRuleBlocksOnlyThatChild() throws Exception {
        MutationRules rules = rulesFrom("<fuzz-rules>"
                + "<rule name='r' action='skip'><match child-type='Subquery'/></rule>"
                + "</fuzz-rules>");
        Expr in = predicateOf("select * from t where a in (select b from s)");
        // child 0 is the column, child 1 the subquery
        Assertions.assertFalse(rules.isBlocked(in, 0));
        Assertions.assertTrue(rules.isBlocked(in, 1));
    }

    @Test
    public void testAttributesWithinOneMatchAreConjunctive() throws Exception {
        String xml = "<fuzz-rules>"
                + "<rule name='r' action='skip'><match parent-function='time_slice' index='2'/></rule>"
                + "</fuzz-rules>";
        MutationRules rules = rulesFrom(xml);
        Expr call = predicateOf("select * from t where time_slice(d, interval 5 year, ceil) > d");
        Expr timeSlice = call.getChild(0);
        Assertions.assertEquals("FunctionCallExpr", timeSlice.getClass().getSimpleName());
        // only index 2 of that function is blocked -- the function name alone is not enough
        Assertions.assertFalse(rules.isBlocked(timeSlice, 0));
        Assertions.assertTrue(rules.isBlocked(timeSlice, 2));
    }

    @Test
    public void testMatchesWithinOneRuleAreDisjunctive() throws Exception {
        MutationRules rules = rulesFrom("<fuzz-rules>"
                + "<rule name='r' action='skip'>"
                + "  <match parent-type='ExistsPredicate'/>"
                + "  <match child-type='Subquery'/>"
                + "</rule></fuzz-rules>");
        Assertions.assertTrue(rules.isBlocked(predicateOf("select * from t where exists (select 1 from s)"), 0));
        Assertions.assertTrue(rules.isBlocked(predicateOf("select * from t where a in (select b from s)"), 1));
    }

    @Test
    public void testAllowOverridesSkip() throws Exception {
        MutationRules rules = rulesFrom("<fuzz-rules>"
                + "<rule name='broad' action='skip'><match child-type='Subquery'/></rule>"
                + "<rule name='carve-out' action='allow'><match parent-type='InPredicate'/></rule>"
                + "</fuzz-rules>");
        // the allow rule wins even though the skip rule also fires
        Assertions.assertFalse(rules.isBlocked(predicateOf("select * from t where a in (select b from s)"), 1));
    }

    @Test
    public void testEmptyRuleSetBlocksNothing() {
        MutationRules rules = MutationRules.forTesting(java.util.List.of());
        Assertions.assertFalse(rules.isBlocked(predicateOf("select * from t where exists (select 1 from s)"), 0));
    }

    @Test
    public void testMatchWithNoAttributesIsRejected() {
        // Such a match would fire on every position and silently disable the whole mutator.
        Assertions.assertThrows(IllegalStateException.class, () -> rulesFrom(
                "<fuzz-rules><rule name='r' action='skip'><match/></rule></fuzz-rules>"));
    }

    @Test
    public void testRuleWithNoMatchIsRejected() {
        Assertions.assertThrows(IllegalStateException.class, () -> rulesFrom(
                "<fuzz-rules><rule name='r' action='skip'/></fuzz-rules>"));
    }

    @Test
    public void testBundledRuleSetLoadsAndCoversTheKnownArtifacts() {
        MutationRules rules = MutationRules.get();
        // The shipped file must actually block the positions that produced artifacts in earlier runs.
        Assertions.assertTrue(rules.isBlocked(predicateOf("select * from t where exists (select 1 from s)"), 0),
                rules.describe());
        Assertions.assertTrue(rules.isBlocked(predicateOf("select * from t where a in (select b from s)"), 1),
                rules.describe());

        // time_slice parses into four children and only the first is an expression; the run that
        // blocked index 2 alone still produced 361 artifacts from indices 1 and 3.
        Expr timeSlice = predicateOf(
                "select * from t where time_slice(d, interval 5 year, ceil) > d").getChild(0);
        Assertions.assertEquals(4, timeSlice.getChildren().size(), "time_slice arity changed");
        Assertions.assertFalse(rules.isBlocked(timeSlice, 0), "the datetime argument must stay mutable");
        for (int i = 1; i <= 3; i++) {
            Assertions.assertTrue(rules.isBlocked(timeSlice, i), "time_slice child " + i + " must be blocked");
        }
    }

    @Test
    public void testAnalyticExprChildrenAreAllBlocked() {
        // Its children mirror the function arguments plus PARTITION BY / ORDER BY, while the function
        // itself is held separately, so setChild desynchronises the two halves.
        MutationRules rules = MutationRules.get();
        StatementBase stmt = SqlParser.parseSingleStatement(
                "select min_by(map('k',[v1]), v1) over (partition by v1 order by v2) from t",
                SqlModeHelper.MODE_DEFAULT);
        Expr analytic = ((SelectRelation) ((QueryStatement) stmt).getQueryRelation())
                .getSelectList().getItems().get(0).getExpr();
        Assertions.assertEquals("AnalyticExpr", analytic.getClass().getSimpleName());
        Assertions.assertTrue(analytic.getChildren().size() >= 3, "unexpected AnalyticExpr arity");
        for (int i = 0; i < analytic.getChildren().size(); i++) {
            Assertions.assertTrue(rules.isBlocked(analytic, i), "AnalyticExpr child " + i + " must be blocked");
        }
    }
}
