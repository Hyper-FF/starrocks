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

import com.starrocks.catalog.TableName;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.ExistsPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.parser.SqlParser;

import java.util.List;
import java.util.Random;

/**
 * M10: combines two seeds into one statement.
 *
 * <p>Every other operator edits a single seed, so a mutant can never be structurally deeper than the
 * seed it came from. That is a hard ceiling, and the corpus sets it low: measured over the 21947
 * queries the fuzzer reads, the median is 91 characters, 89% contain no join, 86.5% no subquery and
 * 96.5% no CTE. Chaining edits does not lift it either -- three quarters of all edits replace a leaf,
 * so stacking them yields a different leaf, not a deeper statement.
 *
 * <p>Splicing lifts it because the depth comes from outside: the result is at least as deep as both
 * inputs plus the construct joining them. With ~16000 seeds the pairing space is far larger than the
 * corpus itself, and neither input has to be complicated for the pair to be.
 *
 * <p>Both forms here are deliberately type-agnostic. An arity- or type-matched form such as UNION
 * fails in the analyzer far more often than it produces anything, and a mutant that dies in the
 * analyzer has exercised nothing:
 * <ul>
 *   <li><b>CROSS JOIN a derived table.</b> No ON clause to satisfy, and it adds a join and a subquery
 *       in one edit.</li>
 *   <li><b>EXISTS in the WHERE clause.</b> EXISTS ignores the subquery's arity and types entirely, so
 *       it applies to any pair that resolves in the same schema.</li>
 * </ul>
 *
 * <p>Siblings come from the same corpus file, which is what makes this work at all: the file's
 * statements were written against one schema, so the spliced query resolves. Splicing across files
 * would name tables that do not exist there and die in name resolution, which is already the largest
 * category of analyzer rejection.
 */
public class SpliceMutation implements Mutation {

    private static final long SQL_MODE = SqlModeHelper.MODE_DEFAULT;

    /** Longer than this and the pair is mostly re-parsing cost rather than new shape. */
    private static final int MAX_SIBLING_TEXT = 4000;

    @Override
    public String name() {
        return "M10-splice";
    }

    @Override
    public String apply(QueryStatement stmt, AstMutationFuzzerTest.Pool pool, Random rnd) {
        if (!(stmt.getQueryRelation() instanceof SelectRelation)) {
            return null;
        }
        SelectRelation select = (SelectRelation) stmt.getQueryRelation();

        QueryStatement sibling = pickSibling(pool, rnd);
        if (sibling == null) {
            return null;
        }

        // A derived table needs a name, and the FROM clause has to be wrappable at all.
        if (rnd.nextBoolean() && select.getRelation() != null) {
            return crossJoinDerived(select, sibling, rnd);
        }
        return existsPredicate(select, sibling, rnd);
    }

    /**
     * Picks a sibling seed and parses it fresh.
     *
     * <p>Parsed here rather than cached because the operator contract forbids sharing a node between
     * two trees: the spliced statement is analyzed, and an aliased node would be corrupted for whoever
     * holds the other reference.
     */
    private static QueryStatement pickSibling(AstMutationFuzzerTest.Pool pool, Random rnd) {
        List<String> siblings = pool.siblingSeeds;
        if (siblings.isEmpty()) {
            return null;
        }
        for (int attempt = 0; attempt < 4; attempt++) {
            String text = siblings.get(rnd.nextInt(siblings.size()));
            if (text == null || text.length() > MAX_SIBLING_TEXT) {
                continue;
            }
            QueryStatement parsed = parseQuery(text);
            if (parsed != null && parsed.getQueryRelation() != null) {
                return parsed;
            }
        }
        return null;
    }

    private static String crossJoinDerived(SelectRelation select, QueryStatement sibling, Random rnd) {
        Relation from = select.getRelation();
        SubqueryRelation derived = new SubqueryRelation(sibling);
        // Without an alias the derived table has no name to resolve against, and the parser rejects it.
        derived.setAlias(new TableName(null, "spliced_" + Math.abs(rnd.nextInt(100000))));

        JoinOperator op = rnd.nextInt(100) < 80 ? JoinOperator.CROSS_JOIN : JoinOperator.INNER_JOIN;
        // INNER JOIN with no ON clause means the same thing as CROSS JOIN here and exercises a different
        // branch of the planner; anything with a real ON clause would need types the tree does not carry.
        JoinRelation joined = new JoinRelation(op, from, derived, null, false);
        select.setRelation(joined);
        return "cross-join-derived " + op + " <- " + abbrev(sibling);
    }

    private static String existsPredicate(SelectRelation select, QueryStatement sibling, Random rnd) {
        boolean negated = rnd.nextInt(100) < 35;
        ExistsPredicate exists = new ExistsPredicate(new Subquery(sibling), negated);

        Expr where = select.getWhereClause();
        if (where == null) {
            select.setWhereClause(exists);
        } else {
            select.setWhereClause(new CompoundPredicate(CompoundPredicate.Operator.AND, where, exists));
        }
        return (negated ? "not-exists" : "exists") + " <- " + abbrev(sibling);
    }

    private static QueryStatement parseQuery(String sql) {
        try {
            List<StatementBase> parsed = SqlParser.parse(sql, SQL_MODE);
            if (parsed.isEmpty() || !(parsed.get(0) instanceof QueryStatement)) {
                return null;
            }
            return (QueryStatement) parsed.get(0);
        } catch (Throwable t) {
            return null;
        }
    }

    private static String abbrev(QueryStatement stmt) {
        try {
            String sql = AstToSQLBuilder.toSQL(stmt);
            if (sql == null) {
                return "<?>";
            }
            String flat = sql.replaceAll("\\s+", " ").trim();
            return flat.length() > 120 ? flat.substring(0, 120) + " ..." : flat;
        } catch (Throwable t) {
            return "<unrenderable sibling>";
        }
    }
}
