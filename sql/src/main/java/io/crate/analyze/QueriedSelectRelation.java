/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.JoinPair;
import io.crate.common.collections.Lists2;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class QueriedSelectRelation implements AnalyzedRelation {

    private final List<AnalyzedRelation> from;
    private final List<JoinPair> joinPairs;
    private final QuerySpec querySpec;
    private final boolean isDistinct;

    public QueriedSelectRelation(boolean isDistinct,
                                 List<AnalyzedRelation> from,
                                 List<JoinPair> joinPairs,
                                 QuerySpec querySpec) {
        assert from.size() >= 1 : "QueriedSelectRelation must have at least 1 relation in FROM";
        this.isDistinct = isDistinct;
        this.from = from;
        this.joinPairs = joinPairs;
        this.querySpec = querySpec;
    }

    public List<AnalyzedRelation> from() {
        return from;
    }

    @Override
    public boolean isDistinct() {
        return isDistinct;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitQueriedSelectRelation(this, context);
    }

    @Override
    public QualifiedName getQualifiedName() {
        throw new UnsupportedOperationException(
            "QueriedSelectRelation has no name. It must be beneath an aliased-relation to be addressable by name");
    }

    @Nonnull
    @Override
    public List<Symbol> outputs() {
        return querySpec.outputs();
    }

    @Override
    public WhereClause where() {
        return querySpec.where();
    }

    @Override
    public List<Symbol> groupBy() {
        return querySpec.groupBy();
    }

    @Nullable
    @Override
    public HavingClause having() {
        return querySpec.having();
    }

    @Nullable
    @Override
    public OrderBy orderBy() {
        return querySpec.orderBy();
    }

    @Nullable
    @Override
    public Symbol limit() {
        return querySpec.limit();
    }

    @Nullable
    @Override
    public Symbol offset() {
        return querySpec.offset();
    }

    @Override
    public String toString() {
        return "SELECT "
               + Lists2.joinOn(", ", outputs(), x -> Symbols.pathFromSymbol(x).sqlFqn())
               + " FROM ("
               + Lists2.joinOn(", ", from, x -> x.getQualifiedName().toString())
               + ')';
    }

    public List<JoinPair> joinPairs() {
        return joinPairs;
    }
}
