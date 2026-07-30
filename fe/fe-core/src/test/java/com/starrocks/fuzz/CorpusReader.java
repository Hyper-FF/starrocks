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

import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared corpus handling for the fuzz harnesses: turns a SQL-Tester {@code T/} case or a plain
 * {@code .sql} fixture into individual statements, and applies the DDL among them to a live
 * in-process catalog so the remaining statements have tables to resolve against.
 */
public final class CorpusReader {

    private CorpusReader() {
    }

    /**
     * Strips SQL-Tester directive lines and comments, then splits on literal-aware semicolons.
     * Statements carrying {@code ${...}} interpolation are dropped — they are not valid SQL.
     */
    public static List<String> extractStatements(String text) {
        StringBuilder cleaned = new StringBuilder();
        for (String line : text.split("\n", -1)) {
            String t = line.trim();
            if (t.isEmpty() || t.startsWith("--") || t.startsWith("#")
                    || t.startsWith("function:") || t.startsWith("shell:") || t.startsWith("spark:")
                    || t.startsWith("hive:") || t.startsWith("trino:") || t.startsWith("[UC]")) {
                continue;
            }
            cleaned.append(line).append('\n');
        }
        List<String> out = new ArrayList<>();
        for (String piece : splitStatements(cleaned.toString())) {
            String t = piece.trim();
            if (t.isEmpty() || t.contains("${") || t.length() > 200_000) {
                continue;
            }
            out.add(t);
        }
        return out;
    }

    /** Splits on ';' while respecting '..', "..", `..`, line comments and block comments. */
    public static List<String> splitStatements(String sql) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        char quote = 0;
        boolean lineComment = false;
        boolean blockComment = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            char next = i + 1 < sql.length() ? sql.charAt(i + 1) : 0;
            if (lineComment) {
                if (c == '\n') {
                    lineComment = false;
                    cur.append(c);
                }
                continue;
            }
            if (blockComment) {
                if (c == '*' && next == '/') {
                    blockComment = false;
                    i++;
                }
                continue;
            }
            if (quote != 0) {
                cur.append(c);
                if (c == '\\' && quote != '`') {
                    if (next != 0) {
                        cur.append(next);
                        i++;
                    }
                } else if (c == quote) {
                    quote = 0;
                }
                continue;
            }
            if (c == '-' && next == '-') {
                lineComment = true;
                continue;
            }
            if (c == '/' && next == '*') {
                blockComment = true;
                i++;
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') {
                quote = c;
                cur.append(c);
                continue;
            }
            if (c == ';') {
                out.add(cur.toString());
                cur.setLength(0);
                continue;
            }
            cur.append(c);
        }
        out.add(cur.toString());
        return out;
    }

    /** Statements that build catalog state the rest of the file depends on. */
    public static boolean isSchemaSetup(StatementBase ast) {
        return ast instanceof CreateTableStmt
                || ast instanceof CreateViewStmt
                || ast instanceof CreateMaterializedViewStatement
                || ast instanceof CreateTableAsSelectStmt
                || ast instanceof AlterTableStmt
                || ast instanceof DropTableStmt;
    }

    /** Best effort: a corpus file may reference external catalogs or unsupported properties. */
    public static boolean applySchemaSetup(StarRocksAssert srAssert, String sql, StatementBase ast) {
        try {
            if (ast instanceof CreateTableStmt) {
                srAssert.withTable(sql);
            } else if (ast instanceof CreateViewStmt) {
                srAssert.withView(sql);
            } else if (ast instanceof CreateMaterializedViewStatement) {
                srAssert.withMaterializedView(sql);
            } else {
                srAssert.ddl(sql);
            }
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
}
