/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.analyze.relations;

import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractTableRelation<T extends TableInfo> implements AnalyzedRelation, FieldResolver {

    protected final T tableInfo;
    private final List<Symbol> outputs;
    private final QualifiedName qualifiedName;

    public AbstractTableRelation(T tableInfo) {
        this(tableInfo, new QualifiedName(Arrays.asList(tableInfo.ident().schema(), tableInfo.ident().name())));
    }

    public AbstractTableRelation(T tableInfo, QualifiedName qualifiedName) {
        this.tableInfo = tableInfo;
        this.qualifiedName = qualifiedName;
        this.outputs = List.copyOf(tableInfo.columns());
    }

    @Override
    public boolean isDistinct() {
        return false;
    }

    public T tableInfo() {
        return tableInfo;
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

    @Nullable
    public Reference getField(ColumnIdent path) {
        return tableInfo.getReadReference(path);
    }

    @Override
    public QualifiedName getQualifiedName() {
        return qualifiedName;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '{' + this.qualifiedName + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractTableRelation<?> that = (AbstractTableRelation<?>) o;

        if (!tableInfo.equals(that.tableInfo)) return false;
        if (!qualifiedName.equals(that.qualifiedName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tableInfo.hashCode();
        result = 31 * result + qualifiedName.hashCode();
        return result;
    }

    @Override
    @Nullable
    public Reference resolveField(ScopedSymbol field) {
        if (field.relation().equals(qualifiedName)) {
            return getField(field.column());
        }
        return null;
    }
}
