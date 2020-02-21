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

import io.crate.analyze.AnalyzedInsertStatement;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AliasedAnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.AnalyzedView;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.common.collections.Lists2;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.MultiPhaseExecutor;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.projection.builder.SplitPoints;
import io.crate.execution.dsl.projection.builder.SplitPointsBuilder;
import io.crate.execution.engine.NodeOperationTreeGenerator;
import io.crate.expression.symbol.FieldReplacer;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.consumer.InsertFromSubQueryPlanner;
import io.crate.planner.optimizer.Optimizer;
import io.crate.planner.optimizer.rule.DeduplicateOrder;
import io.crate.planner.optimizer.rule.MergeAggregateAndCollectToCount;
import io.crate.planner.optimizer.rule.MergeFilterAndCollect;
import io.crate.planner.optimizer.rule.MergeFilters;
import io.crate.planner.optimizer.rule.MoveFilterBeneathFetchOrEval;
import io.crate.planner.optimizer.rule.MoveFilterBeneathGroupBy;
import io.crate.planner.optimizer.rule.MoveFilterBeneathHashJoin;
import io.crate.planner.optimizer.rule.MoveFilterBeneathNestedLoop;
import io.crate.planner.optimizer.rule.MoveFilterBeneathOrder;
import io.crate.planner.optimizer.rule.MoveFilterBeneathProjectSet;
import io.crate.planner.optimizer.rule.MoveFilterBeneathUnion;
import io.crate.planner.optimizer.rule.MoveFilterBeneathWindowAgg;
import io.crate.planner.optimizer.rule.MoveOrderBeneathFetchOrEval;
import io.crate.planner.optimizer.rule.MoveOrderBeneathNestedLoop;
import io.crate.planner.optimizer.rule.MoveOrderBeneathUnion;
import io.crate.planner.optimizer.rule.RemoveRedundantFetchOrEval;
import io.crate.planner.optimizer.rule.RewriteCollectToGet;
import io.crate.planner.optimizer.rule.RewriteFilterOnOuterJoinToInnerJoin;
import io.crate.planner.optimizer.rule.RewriteGroupByKeysLimitToTopNDistinct;
import io.crate.statistics.TableStats;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import static io.crate.expression.symbol.SelectSymbol.ResultType.SINGLE_COLUMN_SINGLE_VALUE;

/**
 * Planner which can create a {@link ExecutionPlan} using intermediate {@link LogicalPlan} nodes.
 */
public class LogicalPlanner {

    public static final int NO_LIMIT = -1;

    private final Optimizer optimizer;
    private final TableStats tableStats;
    private final Visitor statementVisitor = new Visitor();
    private final Functions functions;
    private final Optimizer writeOptimizer;

    public LogicalPlanner(Functions functions, TableStats tableStats, Supplier<Version> minNodeVersionInCluster) {
        this.optimizer = new Optimizer(
            List.of(
                new RemoveRedundantFetchOrEval(),
                new MergeAggregateAndCollectToCount(),
                new MergeFilters(),
                new MoveFilterBeneathFetchOrEval(),
                new MoveFilterBeneathOrder(),
                new MoveFilterBeneathProjectSet(),
                new MoveFilterBeneathHashJoin(),
                new MoveFilterBeneathNestedLoop(),
                new MoveFilterBeneathUnion(),
                new MoveFilterBeneathGroupBy(),
                new MoveFilterBeneathWindowAgg(),
                new MergeFilterAndCollect(),
                new RewriteFilterOnOuterJoinToInnerJoin(functions),
                new MoveOrderBeneathUnion(),
                new MoveOrderBeneathNestedLoop(),
                new MoveOrderBeneathFetchOrEval(),
                new DeduplicateOrder(),
                new RewriteCollectToGet(functions),
                new RewriteGroupByKeysLimitToTopNDistinct()
            ),
            minNodeVersionInCluster
        );
        this.writeOptimizer = new Optimizer(
            List.of(new RewriteInsertFromSubQueryToInsertFromValues()),
            minNodeVersionInCluster
        );
        this.tableStats = tableStats;
        this.functions = functions;
    }

    public LogicalPlan plan(AnalyzedStatement statement, PlannerContext plannerContext) {
        return statementVisitor.process(statement, plannerContext);
    }

    public LogicalPlan planSubSelect(SelectSymbol selectSymbol, PlannerContext plannerContext) {
        CoordinatorTxnCtx txnCtx = plannerContext.transactionContext();
        AnalyzedRelation relation = selectSymbol.relation();

        final int fetchSize;
        final java.util.function.Function<LogicalPlan, LogicalPlan> maybeApplySoftLimit;
        if (selectSymbol.getResultType() == SINGLE_COLUMN_SINGLE_VALUE) {
            // SELECT (SELECT foo FROM t)
            //         ^^^^^^^^^^^^^^^^^
            // The subquery must return at most 1 row, if more than 1 row is returned semantics require us to throw an error.
            // So we limit the query to 2 if there is no limit to avoid retrieval of many rows while being able to validate max1row
            fetchSize = 2;
            maybeApplySoftLimit = relation.limit() == null
                ? plan -> new Limit(plan, Literal.of(2L), Literal.of(0L))
                : plan -> plan;
        } else {
            fetchSize = 0;
            maybeApplySoftLimit = plan -> plan;
        }
        PlannerContext subSelectPlannerContext = PlannerContext.forSubPlan(plannerContext, fetchSize);
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner(s -> planSubSelect(s, subSelectPlannerContext));
        LogicalPlan plan = plan(
            relation,
            subqueryPlanner,
            functions,
            txnCtx,
            Set.of(),
            tableStats,
            subSelectPlannerContext.params()
        );

        plan = tryOptimizeForInSubquery(selectSymbol, relation, plan);
        LogicalPlan optimizedPlan = optimizer.optimize(maybeApplySoftLimit.apply(plan), tableStats, txnCtx);
        return new RootRelationBoundary(optimizedPlan);
    }

    // In case the subselect is inside an IN() or = ANY() apply a "natural" OrderBy to optimize
    // the building of TermInSetQuery which does a sort on the collection of values.
    // See issue https://github.com/crate/crate/issues/6755
    // If the output values are already sorted (even in desc order) no optimization is needed
    private LogicalPlan tryOptimizeForInSubquery(SelectSymbol selectSymbol, AnalyzedRelation relation, LogicalPlan planBuilder) {
        if (selectSymbol.getResultType() == SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES) {
            OrderBy relationOrderBy = relation.orderBy();
            Symbol firstOutput = relation.outputs().get(0);
            if ((relationOrderBy == null || relationOrderBy.orderBySymbols().get(0).equals(firstOutput) == false)
                && DataTypes.PRIMITIVE_TYPES.contains(firstOutput.valueType())) {
                return Order.create(planBuilder, new OrderBy(Collections.singletonList(firstOutput)));
            }
        }
        return planBuilder;
    }


    public LogicalPlan normalizeAndPlan(AnalyzedRelation relation,
                                        PlannerContext plannerContext,
                                        SubqueryPlanner subqueryPlanner,
                                        Set<PlanHint> hints) {
        CoordinatorTxnCtx coordinatorTxnCtx = plannerContext.transactionContext();
        LogicalPlan logicalPlan = plan(
            relation,
            subqueryPlanner,
            functions,
            coordinatorTxnCtx,
            hints,
            tableStats,
            plannerContext.params());
        return optimizer.optimize(logicalPlan, tableStats, coordinatorTxnCtx);
    }

    static LogicalPlan plan(AnalyzedRelation relation,
                            SubqueryPlanner subqueryPlanner,
                            Functions functions,
                            CoordinatorTxnCtx txnCtx,
                            Set<PlanHint> hints,
                            TableStats tableStats,
                            Row params) {
        // TODO: With this change we could probably also follow up on trimming the AnalyzedRelation
        // it would only contain `outputs` and the other properties would only be available
        // in the specific implementations
        var planBuilder = new PlanBuilder(
            subqueryPlanner,
            functions,
            txnCtx,
            hints,
            tableStats,
            params
        );
        return MultiPhase.createIfNeeded(
            relation.accept(planBuilder, new PlanBuilderContext(relation.outputs(), WhereClause.MATCH_ALL)),
            relation,
            subqueryPlanner
        );
    }

    static class PlanBuilderContext {

        final List<Symbol> toCollect;
        final WhereClause whereClause;

        // used to pass on splitPoints.toCollect and whereClause from QueriedSelectRelation to the child
        // We could also not do that and instead rely on optimization rules
        // - Collect operator would default to outputs of the TableRelation; would need column pruning rule
        // - WhereClause would be added as filter initially and would be pushed into Collect via rule
        public PlanBuilderContext(List<Symbol> toCollect, WhereClause whereClause) {
            this.toCollect = toCollect;
            this.whereClause = whereClause;
        }
    }


    static class PlanBuilder extends AnalyzedRelationVisitor<PlanBuilderContext, LogicalPlan> {

        private final SubqueryPlanner subqueryPlanner;
        private final Functions functions;
        private final CoordinatorTxnCtx txnCtx;
        private final Set<PlanHint> hints;
        private final TableStats tableStats;
        private final Row params;

        private PlanBuilder(SubqueryPlanner subqueryPlanner,
                            Functions functions,
                            CoordinatorTxnCtx txnCtx,
                            Set<PlanHint> hints,
                            TableStats tableStats,
                            Row params) {
            this.subqueryPlanner = subqueryPlanner;
            this.functions = functions;
            this.txnCtx = txnCtx;
            this.hints = hints;
            this.tableStats = tableStats;
            this.params = params;
        }

        @Override
        public LogicalPlan visitAnalyzedRelation(AnalyzedRelation relation, PlanBuilderContext context) {
            throw new UnsupportedOperationException(relation.getClass().getSimpleName() + " NYI");
        }

        @Override
        public LogicalPlan visitTableFunctionRelation(TableFunctionRelation relation, PlanBuilderContext context) {
            return TableFunction.create(relation, context.toCollect, context.whereClause);
        }

        @Override
        public LogicalPlan visitDocTableRelation(DocTableRelation relation, PlanBuilderContext context) {
            return Collect.create(relation, context.toCollect, context.whereClause, hints, tableStats, params);
        }

        @Override
        public LogicalPlan visitTableRelation(TableRelation relation, PlanBuilderContext context) {
            return Collect.create(relation, context.toCollect, context.whereClause, hints, tableStats, params);
        }

        @Override
        public LogicalPlan visitAliasedAnalyzedRelation(AliasedAnalyzedRelation relation, PlanBuilderContext context) {
            var child = relation.relation();
            // required remap here might be an indication that we should remove the PlanBuilderContext
            // and settle for optimization rules
            var remapScopedSymbols = FieldReplacer.bind(relation::resolveField);
            var newCtx = new PlanBuilderContext(
                Lists2.map(context.toCollect, remapScopedSymbols),
                context.whereClause.map(remapScopedSymbols)
            );
            var source = child.accept(this, newCtx);
            return new Rename(context.toCollect, relation.getQualifiedName(), source);
        }

        @Override
        public LogicalPlan visitView(AnalyzedView view, PlanBuilderContext context) {
            var child = view.relation();
            // required remap here might be an indication that we should remove the PlanBuilderContext
            // and settle for optimization rules
            var remapScopedSymbols = FieldReplacer.bind(view::resolveField);
            var newCtx = new PlanBuilderContext(
                Lists2.map(context.toCollect, remapScopedSymbols),
                context.whereClause.map(remapScopedSymbols)
            );
            var source = child.accept(this, newCtx);
            return new Rename(context.toCollect, view.getQualifiedName(), source);
        }

        @Override
        public LogicalPlan visitUnionSelect(UnionSelect union, PlanBuilderContext context) {
            var lhsRel = union.left();
            var rhsRel = union.right();
            return new Union(
                lhsRel.accept(this, new PlanBuilderContext(lhsRel.outputs(), lhsRel.where())),
                rhsRel.accept(this, new PlanBuilderContext(rhsRel.outputs(), rhsRel.where())),
                union.outputs()
            );
        }

        @Override
        public LogicalPlan visitQueriedSelectRelation(QueriedSelectRelation relation, PlanBuilderContext context) {
            SplitPoints splitPoints = SplitPointsBuilder.create(relation);
            var newCtx = new PlanBuilderContext(splitPoints.toCollect(), relation.where());
            LogicalPlan source = JoinPlanBuilder.buildJoinTree(
                relation.from(),
                relation.joinPairs(),
                rel -> rel.accept(this, newCtx)
            );
            HavingClause having = relation.having();
            return
                Eval.create(
                    Limit.create(
                        Order.create(
                            Distinct.create(
                                ProjectSet.create(
                                    WindowAgg.create(
                                        Filter.create(
                                            groupByOrAggregate(
                                                source,
                                                relation.groupBy(),
                                                splitPoints.aggregates(),
                                                tableStats
                                            ),
                                            having == null ? null : context.whereClause.add(having.queryOrFallback())
                                        ),
                                        splitPoints.windowFunctions()
                                    ),
                                    splitPoints.tableFunctions()
                                ),
                                relation.isDistinct(),
                                relation.outputs(),
                                tableStats
                            ),
                            relation.orderBy()
                        ),
                        relation.limit(),
                        relation.offset()
                    ),
                    context.toCollect
                );
        }
    }

    private static LogicalPlan groupByOrAggregate(LogicalPlan source,
                                                  List<Symbol> groupKeys,
                                                  List<Function> aggregates,
                                                  TableStats tableStats) {
        if (!groupKeys.isEmpty()) {
            long numExpectedRows = GroupHashAggregate.approximateDistinctValues(source.numExpectedRows(), tableStats, groupKeys);
            return new GroupHashAggregate(source, groupKeys, aggregates, numExpectedRows);
        }
        if (!aggregates.isEmpty()) {
            return new HashAggregate(source, aggregates);
        }
        return source;
    }

    public static Set<Symbol> extractColumns(Symbol symbol) {
        LinkedHashSet<Symbol> columns = new LinkedHashSet<>();
        RefVisitor.visitRefs(symbol, columns::add);
        FieldsVisitor.visitFields(symbol, columns::add);
        return columns;
    }

    public static Set<Symbol> extractColumns(Collection<? extends Symbol> symbols) {
        LinkedHashSet<Symbol> columns = new LinkedHashSet<>();
        for (Symbol symbol : symbols) {
            RefVisitor.visitRefs(symbol, columns::add);
            FieldsVisitor.visitFields(symbol, columns::add);
        }
        return columns;
    }

    public static void execute(LogicalPlan logicalPlan,
                               DependencyCarrier executor,
                               PlannerContext plannerContext,
                               RowConsumer consumer,
                               Row params,
                               SubQueryResults subQueryResults,
                               boolean enableProfiling) {
        if (logicalPlan.dependencies().isEmpty()) {
            doExecute(logicalPlan, executor, plannerContext, consumer, params, subQueryResults, enableProfiling);
        } else {
            MultiPhaseExecutor.execute(logicalPlan.dependencies(), executor, plannerContext, params)
                .whenComplete((valueBySubQuery, failure) -> {
                    if (failure == null) {
                        doExecute(logicalPlan, executor, plannerContext, consumer, params, valueBySubQuery, false);
                    } else {
                        consumer.accept(null, failure);
                    }
                });
        }
    }

    private static void doExecute(LogicalPlan logicalPlan,
                                  DependencyCarrier dependencies,
                                  PlannerContext plannerContext,
                                  RowConsumer consumer,
                                  Row params,
                                  SubQueryResults subQueryResults,
                                  boolean enableProfiling) {
        NodeOperationTree nodeOpTree;
        try {
            nodeOpTree = getNodeOperationTree(logicalPlan, dependencies, plannerContext, params, subQueryResults);
        } catch (Throwable t) {
            consumer.accept(null, t);
            return;
        }
        executeNodeOpTree(
            dependencies,
            plannerContext.transactionContext(),
            plannerContext.jobId(),
            consumer,
            enableProfiling,
            nodeOpTree
        );
    }

    public static NodeOperationTree getNodeOperationTree(LogicalPlan logicalPlan,
                                                         DependencyCarrier executor,
                                                         PlannerContext plannerContext,
                                                         Row params,
                                                         SubQueryResults subQueryResults) {
        ExecutionPlan executionPlan = logicalPlan.build(
            plannerContext, executor.projectionBuilder(), -1, 0, null, null, params, subQueryResults);
        return NodeOperationTreeGenerator.fromPlan(executionPlan, executor.localNodeId());
    }

    public static void executeNodeOpTree(DependencyCarrier dependencies,
                                         TransactionContext txnCtx,
                                         UUID jobId,
                                         RowConsumer consumer,
                                         boolean enableProfiling,
                                         NodeOperationTree nodeOpTree) {
        dependencies.phasesTaskFactory()
            .create(jobId, Collections.singletonList(nodeOpTree), enableProfiling)
            .execute(consumer, txnCtx);
    }

    private class Visitor extends AnalyzedStatementVisitor<PlannerContext, LogicalPlan> {

        @Override
        protected LogicalPlan visitAnalyzedStatement(AnalyzedStatement analyzedStatement, PlannerContext context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "Cannot create LogicalPlan from AnalyzedStatement \"%s\"  - not supported.", analyzedStatement));
        }

        @Override
        public LogicalPlan visitSelectStatement(AnalyzedRelation relation, PlannerContext context) {
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> planSubSelect(s, context));
            LogicalPlan logicalPlan = normalizeAndPlan(relation, context, subqueryPlanner, Set.of());
            return new RootRelationBoundary(logicalPlan);
        }

        @Override
        protected LogicalPlan visitAnalyzedInsertStatement(AnalyzedInsertStatement statement, PlannerContext context) {
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> planSubSelect(s, context));
            return writeOptimizer.optimize(
                InsertFromSubQueryPlanner.plan(
                    statement,
                    context,
                    LogicalPlanner.this,
                    subqueryPlanner),
                tableStats,
                context.transactionContext()
            );
        }
    }
}
