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

package com.starrocks.sql.optimizer.transformer;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;

public class TransformerContext {
    private final ColumnRefFactory columnRefFactory;
    private final ConnectContext session;

    private final ExpressionMapping outer;
    /**
     * The enclosing query's mapping, used *only* to translate a JOIN's ON predicate.
     * <p>
     * A relation in a FROM clause is deliberately planned with an empty {@link #outer} so that it cannot see
     * the enclosing query (StarRocks has no correlated derived tables). A JOIN's ON predicate is the one
     * exception: the analyzer resolves it against a scope chained onto the enclosing query's scope, so it can
     * legitimately hand back a field of the outer relation. Translating such a field needs the outer plan's
     * column refs, which is what this mapping carries. It is null whenever there is no enclosing query.
     */
    private final ExpressionMapping onPredicateOuter;
    private final CTETransformerContext cteContext;
    private final MVTransformerContext mvTransformerContext;

    public TransformerContext(
            ColumnRefFactory columnRefFactory,
            ConnectContext session,
            MVTransformerContext mvTransformerContext) {
        this(columnRefFactory, session,
                new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields())),
                new CTETransformerContext(session.getSessionVariable().getCboCTEMaxLimit()), mvTransformerContext);
    }

    public TransformerContext(
            ColumnRefFactory columnRefFactory,
            ConnectContext session,
            ExpressionMapping outer,
            CTETransformerContext cteContext,
            MVTransformerContext mvTransformerContext) {
        this(columnRefFactory, session, outer, null, cteContext, mvTransformerContext);
    }

    public TransformerContext(
            ColumnRefFactory columnRefFactory,
            ConnectContext session,
            ExpressionMapping outer,
            ExpressionMapping onPredicateOuter,
            CTETransformerContext cteContext,
            MVTransformerContext mvTransformerContext) {
        this.columnRefFactory = columnRefFactory;
        this.session = session;
        this.outer = outer;
        this.onPredicateOuter = onPredicateOuter;
        this.cteContext = cteContext;
        this.mvTransformerContext = mvTransformerContext;
    }

    public ColumnRefFactory getColumnRefFactory() {
        return columnRefFactory;
    }

    public ConnectContext getSession() {
        return session;
    }

    public ExpressionMapping getOuter() {
        return outer;
    }

    public ExpressionMapping getOnPredicateOuter() {
        return onPredicateOuter;
    }

    public CTETransformerContext getCteContext() {
        return cteContext;
    }

    public MVTransformerContext getMVTransformerContext() {
        return mvTransformerContext;
    }
}
