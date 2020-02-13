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

package io.crate.analyze.relations;

import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AliasedAnalyzedRelation implements AnalyzedRelation, FieldResolver {

    private final AnalyzedRelation relation;
    private final QualifiedName qualifiedName;
    private final Map<ColumnIdent, ColumnIdent> aliasToColumnMapping;
    private final ArrayList<Symbol> outputs;

    public AliasedAnalyzedRelation(AnalyzedRelation relation, QualifiedName relationAlias) {
        this(relation, relationAlias, List.of());
    }

    AliasedAnalyzedRelation(AnalyzedRelation relation, QualifiedName relationAlias, List<String> columnAliases) {
        this.relation = relation;
        qualifiedName = relationAlias;
        aliasToColumnMapping = new HashMap<>(columnAliases.size());
        this.outputs = new ArrayList<>(relation.outputs().size());
        for (int i = 0; i < relation.outputs().size(); i++) {
            Symbol childOutput = relation.outputs().get(i);
            ScopedSymbol scopedSymbol = new ScopedSymbol(
                qualifiedName, Symbols.pathFromSymbol(childOutput), childOutput.valueType());
            if (i < columnAliases.size()) {
                String alias = columnAliases.get(i);
                outputs.add(new AliasSymbol(alias, scopedSymbol));
            } else {
                outputs.add(scopedSymbol);
            }
        }
    }

    public AnalyzedRelation relation() {
        return relation;
    }

    @Override
    public Symbol getField(ColumnIdent path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        for (Symbol output : outputs) {
            if (Symbols.pathFromSymbol(output).equals(path)) {
                return output;
            }
            if (output instanceof AliasSymbol) {
                AliasSymbol aliasSymbol = (AliasSymbol) output;
                if (new ColumnIdent(aliasSymbol.alias()).equals(path)) {
                    return aliasSymbol.symbol();
                }
            }
        }
        return null;
    }

    @Override
    public QualifiedName getQualifiedName() {
        return qualifiedName;
    }

    @Nonnull
    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public WhereClause where() {
        return WhereClause.MATCH_ALL;
    }

    @Override
    public List<Symbol> groupBy() {
        return List.of();
    }

    @Nullable
    @Override
    public HavingClause having() {
        return null;
    }

    @Nullable
    @Override
    public OrderBy orderBy() {
        return null;
    }

    @Nullable
    @Override
    public Symbol limit() {
        return null;
    }

    @Nullable
    @Override
    public Symbol offset() {
        return null;
    }

    @Override
    public boolean isDistinct() {
        return false;
    }

    @Override
    public String toString() {
        return relation + " AS " + qualifiedName;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitAliasedAnalyzedRelation(this, context);
    }

    @Nullable
    @Override
    public Symbol resolveField(ScopedSymbol field) {
        return relation.getField(field.column(), Operation.READ);
    }
}
