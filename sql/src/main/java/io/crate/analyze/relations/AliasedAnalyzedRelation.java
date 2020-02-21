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

import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <pre>{@code <relation> AS alias}</pre>
 *
 * This relation only provides a different name for an inner relation.
 * The {@link #outputs()} of the aliased-relation are {@link ScopedSymbol}s,
 * a 1:1 mapping of the outputs of the inner relation but associated with the aliased-relation.;
 */
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
            ColumnIdent childColumn = Symbols.pathFromSymbol(childOutput);
            if (i < columnAliases.size()) {
                ColumnIdent alias = new ColumnIdent(columnAliases.get(i));
                aliasToColumnMapping.put(alias, childColumn);
                outputs.add(new ScopedSymbol(qualifiedName, alias, childOutput.valueType()));
            } else {
                outputs.add(new ScopedSymbol(qualifiedName, childColumn, childOutput.valueType()));
            }
        }
    }

    public AnalyzedRelation relation() {
        return relation;
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
        var idx = outputs.indexOf(field);
        if (idx < 0) {
            throw new IllegalArgumentException(field + " does not belong to " + getQualifiedName());
        }
        return relation.outputs().get(idx);
    }
}
