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
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.Table;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

public class AnalyzedCopyFromReturnSummary extends AnalyzedCopyFrom implements AnalyzedRelation {

    private final QualifiedName qualifiedName;
    private final List<ScopedSymbol> fields;

    AnalyzedCopyFromReturnSummary(DocTableInfo tableInfo,
                                  Table<Symbol> table,
                                  GenericProperties<Symbol> properties,
                                  Symbol uri) {
        super(tableInfo, table, properties, uri);
        qualifiedName = new QualifiedName(Arrays.asList(tableInfo.ident().schema(), tableInfo.ident().name()));
        this.fields = List.of(
            new ScopedSymbol(qualifiedName, new ColumnIdent("node"), ObjectType.builder()
                .setInnerType("id", DataTypes.STRING)
                .setInnerType("name", DataTypes.STRING)
                .build()),
            new ScopedSymbol(qualifiedName, new ColumnIdent("uri"), DataTypes.STRING),
            new ScopedSymbol(qualifiedName, new ColumnIdent("success_count"), DataTypes.LONG),
            new ScopedSymbol(qualifiedName, new ColumnIdent("error_count"), DataTypes.LONG),
            new ScopedSymbol(qualifiedName, new ColumnIdent("errors"), ObjectType.untyped())
        );
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        throw new UnsupportedOperationException(
            getClass().getCanonicalName() + " is virtual relation, visiting it is unsupported");
    }

    @Override
    public ScopedSymbol getField(ColumnIdent path, Operation operation)
        throws UnsupportedOperationException, ColumnUnknownException {
        throw new UnsupportedOperationException("getField is unsupported on internal relation for copy from return");
    }

    @Override
    public QualifiedName getQualifiedName() {
        return qualifiedName;
    }

    @Nonnull
    @Override
    public List<Symbol> outputs() {
        return List.copyOf(fields);
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
}
