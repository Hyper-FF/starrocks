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

package com.starrocks.sql.parser;

import com.google.common.base.Preconditions;
import com.starrocks.sql.ast.Identifier;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;

public class AstBuilderUtils {
    public static Identifier getIdentifier(
            com.starrocks.sql.parser.StarRocksParser.IdentifierContext identifierContext) {
        if (identifierContext instanceof com.starrocks.sql.parser.StarRocksParser.BackQuotedIdentifierContext) {
            Identifier backQuotedIdentifier = new Identifier(unquoteBackQuoted(identifierContext.getText()),
                    createPos(identifierContext));
            backQuotedIdentifier.setBackQuoted(true);
            return backQuotedIdentifier;
        } else {
            return new Identifier(identifierContext.getText(), createPos(identifierContext));
        }
    }

    /**
     * Read back the text of a BACKQUOTED_IDENTIFIER token, whose lexer rule is {@code '`' ( ~'`' | '``' )* '`'}:
     * drop the delimiters, then collapse each doubled backtick into the single one it stands for. This is the
     * inverse of {@code ParseUtil#backquote}, so a name that contains a backtick survives a deparse/reparse
     * round trip instead of silently losing it.
     */
    public static String unquoteBackQuoted(String text) {
        return text.substring(1, text.length() - 1).replace("``", "`");
    }

    protected static NodePosition createPos(ParserRuleContext context) {
        Preconditions.checkState(context != null);
        return createPos(context.start, context.stop);
    }

    static NodePosition createPos(Token start, Token stop) {
        if (start == null) {
            return NodePosition.ZERO;
        }

        if (stop == null) {
            return new NodePosition(start.getLine(), start.getCharPositionInLine());
        }

        return new NodePosition(start, stop);
    }
}
