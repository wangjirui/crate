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

import io.crate.expression.scalar.SubscriptFunctions;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;
import io.crate.types.ObjectType;

import javax.annotation.Nullable;

public final class Fields {

    @Nullable
    public static Symbol getFromSourceWithNewScope(RelationName relationName,
                                                   ColumnIdent column,
                                                   Operation operation,
                                                   AnalyzedRelation source) {
        if (operation != Operation.READ) {
            throw new UnsupportedOperationException(operation + " is not supported on " + relationName);
        }
        Symbol field = source.getField(column, operation);
        if (field != null) {
            return new ScopedSymbol(relationName, column, field.valueType());
        }
        if (column.isTopLevel()) {
            return null;
        }
        ColumnIdent rootColumn = column.getRoot();
        Symbol rootSymbol = source.getField(rootColumn, operation);
        if (rootSymbol == null || rootSymbol.valueType().id() != ObjectType.ID) {
            return null;
        }
        return SubscriptFunctions.makeObjectSubscript(
            new ScopedSymbol(relationName, rootColumn, rootSymbol.valueType()),
            column
        );
    }
}
