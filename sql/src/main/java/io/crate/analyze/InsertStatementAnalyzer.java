/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.core.StringUtils;
import io.crate.core.collections.StringObjectMaps;
import io.crate.exceptions.CrateException;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.*;
import org.apache.lucene.util.BytesRef;

import java.util.*;

public class InsertStatementAnalyzer extends DataStatementAnalyzer<InsertAnalysis> {

    @Override
    public Symbol visitInsert(Insert node, InsertAnalysis context) {
        process(node.table(), context);

        if (context.table().isAlias() && !context.table().isPartitioned()) {
            throw new IllegalArgumentException("Table alias not allowed in INSERT statement.");
        }
        int maxValuesLength = node.maxValuesLength();
        int numColumns = node.columns().size() == 0 ? maxValuesLength : node.columns().size();
        // allocate columnsLists
        context.columns(new ArrayList<Reference>(numColumns));
        context.partitionedByColumns(
                new ArrayList<Reference>(context.table().partitionedByColumns().size())
        );

        if (node.columns().size() == 0) { // no columns given in statement

            if (maxValuesLength > context.table().columns().size()) {
                throw new IllegalArgumentException("too many values");
            }

            int i = 0;
            for (ReferenceInfo columnInfo : context.table().columns()) {
                if (i >= maxValuesLength) { break; }
                addColumn(columnInfo.ident().columnIdent().name(), context, i);
                i++;
            }

        } else {
            if (maxValuesLength > node.columns().size()) {
                throw new IllegalArgumentException("too few values");
            }
            for (int i = 0; i < node.columns().size(); i++) {
                addColumn(node.columns().get(i), context, i);
            }
        }

        if (!context.table().hasAutoGeneratedPrimaryKey() && context.primaryKeyColumnIndices().size() == 0) {
            throw new IllegalArgumentException("Primary key is required but is missing from the insert statement");
        }
        String clusteredBy = context.table().clusteredBy();
        if (clusteredBy != null && !clusteredBy.equalsIgnoreCase("_id") && context.routingColumnIndex() < 0) {
            throw new IllegalArgumentException("Clustered by value is required but is missing from the insert statement");
        }

        for (ValuesList valuesList : node.valuesLists()) {
            process(valuesList, context);
        }

        return null;
    }

    private Reference addColumn(String column, InsertAnalysis context, int i) {
        assert context.table() != null;
        return addColumn(new ReferenceIdent(context.table().ident(), column), context, i);
    }

    private Reference addColumn(ReferenceIdent ident, InsertAnalysis context, int i) {
        String column = ident.columnIdent().name();
        Preconditions.checkArgument(!column.startsWith("_"), "Inserting system columns is not allowed");

        // set primary key index if found
        if (StringUtils.pathListContainsPrefix(context.table().primaryKey(), column)) {
            context.addPrimaryKeyColumnIdx(i);
        }

        // set routing if found
        String routing = context.table().clusteredBy();
        if (routing != null && StringUtils.pathListContainsPrefix(Arrays.asList(routing), column)) {
            context.routingColumnIndex(i);
        }

        // ensure that every column is only listed once
        Reference columnReference = context.allocateUniqueReference(ident);
        if (context.table().partitionedByColumns().contains(columnReference.info())) {
            context.partitionedByColumns().add(columnReference);
            context.addPartitionedByIndex(i);
        } else {
            context.columns().add(columnReference);
        }
        return columnReference;
    }

    @Override
    protected Symbol visitTable(Table node, InsertAnalysis context) {
        Preconditions.checkState(context.table() == null, "inserting into multiple tables is not supported");
        context.editableTable(TableIdent.of(node));
        return null;
    }

    @Override
    public Symbol visitValuesList(ValuesList node, InsertAnalysis context) {

        Map<String, Object> sourceMap = new HashMap<>(node.values().size());
        Map<String, String> partitionMap = null;
        List<String> primaryKeyValues = new ArrayList<>();
        String routingValue = null;

        if (node.values().size() != context.columns().size() + context.partitionedByColumns().size()) {
            throw new IllegalArgumentException("incorrect number of values");
        }

        if (context.table().isPartitioned()) {
            partitionMap = context.newPartitionMap();
        }

        int i = 0;
        int partitionedIdx = 0;
        int columnIdx = 0;
        for (Expression expression : node.values()) {
            // TODO: instead of doing a type guessing and then a conversion this could
            // be improved by using the dataType from the column Reference as a hint
            Symbol valuesSymbol = process(expression, context);
            boolean isPartitionColumn = context.partitionedByIndices().contains(i);

            // implicit type conversion
            Reference column;
            if (isPartitionColumn) {
                column = context.partitionedByColumns().get(partitionedIdx++);
            } else {
                column = context.columns().get(columnIdx++);
            }
            String columnName = column.info().ident().columnIdent().name();

            try {
                valuesSymbol = context.normalizeInputForReference(valuesSymbol, column);
            } catch (IllegalArgumentException|UnsupportedOperationException e) {
                throw new ValidationException(column.info().ident().columnIdent().fqn(), e);
            }


            try {
                Object value = ((io.crate.operation.Input)valuesSymbol).value();
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                if (isPartitionColumn && partitionMap != null) {
                    partitionMap.put(columnName, value.toString());
                } else {
                    sourceMap.put(
                            columnName,
                            value
                    );
                }
                if (context.primaryKeyColumnIndices().contains(i)) {
                    int idx = context.table().primaryKey().indexOf(columnName);
                    Object pkValue = value;
                    if (idx < 0) {
                        // oh look, a nested primary key!
                        String pkColumnName = StringUtils.getPathByPrefix(context.table.primaryKey(), columnName);
                        assert value instanceof Map;
                        pkValue = StringObjectMaps.getByPath((Map<String, Object>)value, pkColumnName);
                        idx = context.table.primaryKey().indexOf(pkColumnName); // set index to correct column
                    }
                    if (pkValue == null) {
                        throw new IllegalArgumentException("Primary key value must not be NULL");
                    }

                    if (primaryKeyValues.size() > idx) {
                        primaryKeyValues.add(idx, pkValue.toString());
                    } else {
                        primaryKeyValues.add(pkValue.toString());
                    }
                }
                if (i == context.routingColumnIndex()) {
                    Object clusteredByValue = value;
                    if (!columnName.equals(context.table().clusteredBy())) {
                        // oh my gosh! A nested clustered by value!!!
                        assert value instanceof Map;
                        clusteredByValue = StringObjectMaps.getByPath((Map<String, Object>)value, context.table().clusteredBy());
                    }
                    if (clusteredByValue == null) {
                        throw new IllegalArgumentException("Clustered by value must not be NULL");
                    }

                    routingValue = clusteredByValue.toString();
                }
            } catch (ClassCastException e) {
                // symbol is no input
                throw new CrateException(String.format("invalid value '%s' in insert statement", valuesSymbol.toString()));
            }

            i++;
        }
        context.sourceMaps().add(sourceMap);
        context.addIdAndRouting(primaryKeyValues, routingValue);

        return null;
    }
}
