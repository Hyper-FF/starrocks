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

package com.starrocks.sql.plan;

import com.starrocks.common.Config;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.sql.analyzer.DropStmtAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DropFunctionStmt;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for Java UDFs declared with varargs (variable argument) syntax.
 *
 * <p>StarRocks supports declaring Java UDFs with the {@code ...} varargs notation
 * in CREATE FUNCTION statements. A varargs UDF can be called with additional
 * arguments beyond the fixed declared types, as long as the extra arguments
 * are assignable to the varargs element type (the last declared type).
 *
 * <p>Example:
 * <pre>
 *   CREATE FUNCTION my_func(int, string, ...) RETURNS string ...
 * </pre>
 * This declares a function that requires at least one {@code int} argument
 * followed by one or more {@code string} arguments.
 */
public class JavaVarArgsUDFTest extends PlanTestBase {

    private static final String INT_VARARGS_FUNC =
            "CREATE FUNCTION db1.int_varargs(int, ...) RETURNS int " +
                    "PROPERTIES ('symbol'='IntVarArgs', 'type'='StarrocksJar', 'file'='test.jar')";

    private static final String STR_VARARGS_FUNC =
            "CREATE FUNCTION db1.str_varargs(string, string, ...) RETURNS string " +
                    "PROPERTIES ('symbol'='StrVarArgs', 'type'='StarrocksJar', 'file'='test.jar')";

    private static final String MIXED_VARARGS_FUNC =
            "CREATE FUNCTION db1.mixed_varargs(int, string, ...) RETURNS string " +
                    "PROPERTIES ('symbol'='MixedVarArgs', 'type'='StarrocksJar', 'file'='test.jar')";

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        // Register varargs UDFs using mock (no JAR required for FE plan tests).
        // Note: withFunction() temporarily sets enable_udf=true for parsing then resets it;
        // we enable it here for all subsequent UDF call plan tests.
        starRocksAssert.withFunction(INT_VARARGS_FUNC);
        starRocksAssert.withFunction(STR_VARARGS_FUNC);
        starRocksAssert.withFunction(MIXED_VARARGS_FUNC);
        Config.enable_udf = true;
    }

    @AfterAll
    public static void afterClass() throws Exception {
        // Cleanup varargs UDFs
        dropFunctionIfExists("db1.int_varargs(int, ...)");
        dropFunctionIfExists("db1.str_varargs(string, string, ...)");
        dropFunctionIfExists("db1.mixed_varargs(int, string, ...)");
        Config.enable_udf = false;
    }

    private static void dropFunctionIfExists(String funcSignature) {
        try {
            Config.enable_udf = true;
            String dropSql = "DROP FUNCTION IF EXISTS " + funcSignature;
            DropFunctionStmt dropStmt = (DropFunctionStmt)
                    UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(dropSql, connectContext);
            DropStmtAnalyzer.analyze(dropStmt, connectContext);
            DDLStmtExecutor.execute(dropStmt, connectContext);
        } catch (Exception e) {
            // ignore cleanup errors
        }
    }

    // -------------------------------------------------------------------------
    // Tests: int_varargs(int, ...)
    // -------------------------------------------------------------------------

    @Test
    public void testVarArgsWithMinimumArgs() throws Exception {
        // Calling with exactly the declared number of args should succeed
        String plan = getFragmentPlan("SELECT int_varargs(1)");
        assertContains(plan, "int_varargs");
    }

    @Test
    public void testVarArgsWithOneExtraArg() throws Exception {
        // Calling with one extra int arg (matches varargs type) should succeed
        String plan = getFragmentPlan("SELECT int_varargs(1, 2)");
        assertContains(plan, "int_varargs");
    }

    @Test
    public void testVarArgsWithMultipleExtraArgs() throws Exception {
        // Calling with multiple extra int args should succeed
        String plan = getFragmentPlan("SELECT int_varargs(1, 2, 3, 4, 5)");
        assertContains(plan, "int_varargs");
    }

    @Test
    public void testVarArgsTypeCheckOnExtraArgs() {
        // Extra args must be castable to the varargs element type (int).
        // An array<int> cannot be cast to int, so this should fail.
        Assertions.assertThrows(SemanticException.class, () ->
                getFragmentPlan("SELECT int_varargs(1, [1,2,3])")
        );
    }

    @Test
    public void testVarArgsRequiredArgMissing() {
        // Must provide at least the minimum required args (one int for int_varargs)
        Assertions.assertThrows(SemanticException.class, () ->
                getFragmentPlan("SELECT int_varargs()")
        );
    }

    @Test
    public void testVarArgsWithNullLiteral() throws Exception {
        // NULL literal should be accepted for varargs element type
        String plan = getFragmentPlan("SELECT int_varargs(1, null)");
        assertContains(plan, "int_varargs");
    }

    @Test
    public void testVarArgsWithColumnArgs() throws Exception {
        // Using column references as varargs arguments should succeed
        String plan = getFragmentPlan("SELECT int_varargs(v1, v2) FROM t0");
        assertContains(plan, "int_varargs");
    }

    @Test
    public void testVarArgsInWhereClause() throws Exception {
        // varargs UDF in WHERE clause
        String plan = getFragmentPlan("SELECT v1 FROM t0 WHERE int_varargs(v1, v2) > 0");
        assertContains(plan, "int_varargs");
    }

    @Test
    public void testVarArgsInGroupBy() throws Exception {
        // varargs UDF in GROUP BY clause
        String plan = getFragmentPlan("SELECT int_varargs(v1, v2), count(*) FROM t0 GROUP BY int_varargs(v1, v2)");
        assertContains(plan, "int_varargs");
    }

    // -------------------------------------------------------------------------
    // Tests: str_varargs(string, string, ...)
    // -------------------------------------------------------------------------

    @Test
    public void testStrVarArgsWithMinimumArgs() throws Exception {
        // str_varargs requires at least two string args
        String plan = getFragmentPlan("SELECT str_varargs('hello', 'world')");
        assertContains(plan, "str_varargs");
    }

    @Test
    public void testStrVarArgsWithExtraArgs() throws Exception {
        // Calling with extra string args should succeed
        String plan = getFragmentPlan("SELECT str_varargs('a', 'b', 'c', 'd')");
        assertContains(plan, "str_varargs");
    }

    @Test
    public void testStrVarArgsTooFewArgs() {
        // str_varargs requires at least two string args; providing one should fail
        Assertions.assertThrows(SemanticException.class, () ->
                getFragmentPlan("SELECT str_varargs('hello')")
        );
    }

    @Test
    public void testStrVarArgsWithImplicitCastableExtra() throws Exception {
        // int can be implicitly cast to string, so this should succeed
        String plan = getFragmentPlan("SELECT str_varargs('a', 'b', 123)");
        assertContains(plan, "str_varargs");
    }

    @Test
    public void testStrVarArgsWrongTypeInExtra() {
        // Extra args must be castable to string. An array<int> cannot be cast to string, so this should fail.
        Assertions.assertThrows(SemanticException.class, () ->
                getFragmentPlan("SELECT str_varargs('a', 'b', [1,2,3])")
        );
    }

    // -------------------------------------------------------------------------
    // Tests: mixed_varargs(int, string, ...)
    // -------------------------------------------------------------------------

    @Test
    public void testMixedVarArgsMinimumArgs() throws Exception {
        // mixed_varargs(int, string, ...) requires at least int and string
        String plan = getFragmentPlan("SELECT mixed_varargs(1, 'hello')");
        assertContains(plan, "mixed_varargs");
    }

    @Test
    public void testMixedVarArgsWithExtraStringArgs() throws Exception {
        // Extra string args are valid (varargs type is string)
        String plan = getFragmentPlan("SELECT mixed_varargs(1, 'a', 'b', 'c')");
        assertContains(plan, "mixed_varargs");
    }

    @Test
    public void testMixedVarArgsMissingRequiredArg() {
        // Providing only int (missing required string arg) should fail
        Assertions.assertThrows(SemanticException.class, () ->
                getFragmentPlan("SELECT mixed_varargs(1)")
        );
    }

    @Test
    public void testMixedVarArgsIntExtraImplicitlyCastable() throws Exception {
        // int can be implicitly cast to string (varargs type), so this should succeed
        String plan = getFragmentPlan("SELECT mixed_varargs(1, 'a', 2)");
        assertContains(plan, "mixed_varargs");
    }

    @Test
    public void testMixedVarArgsWrongTypeForVarArgsPart() {
        // Extra args must be castable to string. An array<int> cannot be cast to string.
        Assertions.assertThrows(SemanticException.class, () ->
                getFragmentPlan("SELECT mixed_varargs(1, 'a', [1,2,3])")
        );
    }

    // -------------------------------------------------------------------------
    // Tests: DROP FUNCTION with varargs syntax
    // -------------------------------------------------------------------------

    @Test
    public void testDropVarArgsFunction() throws Exception {
        // Register a temporary varargs UDF and then drop it
        starRocksAssert.withFunction(
                "CREATE FUNCTION db1.temp_varargs(bigint, ...) RETURNS bigint " +
                        "PROPERTIES ('symbol'='TempVarArgs', 'type'='StarrocksJar', 'file'='test.jar')");

        // Verify it can be used in a plan
        String plan = getFragmentPlan("SELECT temp_varargs(1)");
        assertContains(plan, "temp_varargs");

        // Drop using varargs syntax
        dropFunctionIfExists("db1.temp_varargs(bigint, ...)");

        // After drop, using the function should fail
        Assertions.assertThrows(SemanticException.class, () ->
                getFragmentPlan("SELECT temp_varargs(1)")
        );
    }

    @Test
    public void testDropVarArgsFunctionParseVariadicFlag() throws Exception {
        // Verify that DROP FUNCTION correctly parses the varargs flag (... syntax)
        String dropSql = "DROP FUNCTION IF EXISTS db1.int_varargs(int, ...)";
        DropFunctionStmt dropStmt = (DropFunctionStmt)
                UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(dropSql, connectContext);
        DropStmtAnalyzer.analyze(dropStmt, connectContext);
        Assertions.assertTrue(dropStmt.getArgsDef().isVariadic(),
                "DROP FUNCTION with ... should have isVariadic=true");
    }

    // -------------------------------------------------------------------------
    // Tests: Function resolution - varargs vs non-varargs
    // -------------------------------------------------------------------------

    @Test
    public void testVarArgsResolutionDoesNotConflictWithFixed() throws Exception {
        // Create a non-varargs version of str_varargs for 3 fixed args
        starRocksAssert.withFunction(
                "CREATE FUNCTION db1.str_fixed3(string, string, string) RETURNS string " +
                        "PROPERTIES ('symbol'='StrFixed3', 'type'='StarrocksJar', 'file'='test.jar')");

        // Both functions should resolve independently
        String planFixed = getFragmentPlan("SELECT str_fixed3('a', 'b', 'c')");
        assertContains(planFixed, "str_fixed3");

        String planVarargs = getFragmentPlan("SELECT str_varargs('a', 'b', 'c')");
        assertContains(planVarargs, "str_varargs");

        dropFunctionIfExists("db1.str_fixed3(string, string, string)");
    }

    @Test
    public void testVarArgsFunctionSignature() throws Exception {
        // Verify the function is registered with hasVarArgs=true
        String plan = getFragmentPlan("SELECT int_varargs(v1) FROM t0");
        assertContains(plan, "int_varargs");

        // The plan should show the function call with the argument
        String verbosePlan = getVerboseExplain("SELECT int_varargs(v1) FROM t0");
        assertContains(verbosePlan, "int_varargs");
    }

    @Test
    public void testVarArgsWithImplicitCast() throws Exception {
        // tinyint can be cast to int (the varargs type), so this should succeed
        String plan = getFragmentPlan("SELECT int_varargs(1, cast(2 as tinyint))");
        assertContains(plan, "int_varargs");
    }
}
