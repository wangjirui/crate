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

package io.crate.planner.operators;

import io.crate.analyze.OrderBy;
import io.crate.analyze.relations.FieldResolver;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RelationName;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * https://en.wikipedia.org/wiki/Relational_algebra#Rename_(%CF%81)
 *
 * This operator can be used as a bridge
 *
 * <pre>
 *     {@code
 *                      outputs: [Reference(x)]
 *                         |
 *          SELECT x FROM tbl as t;
 *                 |
 *                ScopedSymbol(relation=t, x)
 *
 *          Rename does two things:
 *              - Rename the relation (tbl=t)
 *              - Act as bridge for outputs. (in the example above from ScopedSymbol to Reference)
 *     }
 * </pre>
 */
public final class Rename extends ForwardingLogicalPlan implements FieldResolver {

    private final List<Symbol> outputs;
    private final FieldResolver fieldResolver;
    final RelationName name;

    public Rename(List<Symbol> outputs, RelationName name, FieldResolver fieldResolver, LogicalPlan source) {
        super(source);
        /* Rename is supposed so be a 1:1 mapping - E.g. SELECT x, y FROM (select x, y, z ...) as t
         * `as t` doesn't have a "selection".
         *
         * But we support eager "selection" propagation in the LogicalPlan builder, so the inner relation can return
         * `[x, y]` instead of `[x, y, z]`,
         *
         * SELECT x > 1 FROM (select x, y, z ...) AS t
         *  --> SELECT x > 1 FROM (select x > 1 ...) AS t
         */
        ArrayList<Symbol> adaptedOutputs = new ArrayList<>();
        for (Symbol output : outputs) {
            // fieldResolver.resolveField()
        }
        this.outputs = outputs;
        this.name = name;
        this.fieldResolver = fieldResolver;
    }

    public RelationName name() {
        return name;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public ExecutionPlan build(PlannerContext plannerContext,
                               ProjectionBuilder projectionBuilder,
                               int limit,
                               int offset,
                               @Nullable OrderBy order,
                               @Nullable Integer pageSizeHint,
                               Row params,
                               SubQueryResults subQueryResults) {
        return source.build(plannerContext, projectionBuilder, limit, offset, order, pageSizeHint, params, subQueryResults);
    }

    @Override
    public LogicalPlan replaceSources(List<LogicalPlan> sources) {
        return new Rename(outputs, name, fieldResolver, Lists2.getOnlyElement(sources));
    }

    @Override
    public <C, R> R accept(LogicalPlanVisitor<C, R> visitor, C context) {
        return visitor.visitRename(this, context);
    }

    @Nullable
    @Override
    public Symbol resolveField(ScopedSymbol field) {
        return fieldResolver.resolveField(field);
    }

    @Override
    public Set<RelationName> getRelationNames() {
        return Set.of(name);
    }
}
