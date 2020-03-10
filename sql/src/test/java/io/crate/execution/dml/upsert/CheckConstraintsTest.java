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

package io.crate.execution.dml.upsert;

import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.expression.InputFactory;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.TableElement;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Testing {@linkplain io.crate.execution.dml.upsert.CheckConstraints},
 * as well as various cases related to:
 *
 * <pre>
 *     CONSTRAINT &lt;name&gt; CHECK &lt;boolean expression&gt;
 * </pre>
 */
public class CheckConstraintsTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor sqlExecutor;
    private DocTableInfo docTableInfo;
    private CheckConstraints checkConstraints;
    private TransactionContext txnCtx;

    @Before
    public void setUpExecutor() throws Exception {
        sqlExecutor = SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE t (" +
                      "    id int," +
                      "    qty int," +
                      "    sentinel boolean CONSTRAINT sentinel CHECK(sentinel)," +
                      "    CONSTRAINT id_is_even CHECK(id % 2 = 0))")
            .build();
        docTableInfo = sqlExecutor.resolveTableInfo("t");
        txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        checkConstraints = new CheckConstraints(
            txnCtx,
            new InputFactory(sqlExecutor.functions()),
            FromSourceRefResolver.WITHOUT_PARTITIONED_BY_REFS,
            docTableInfo);
    }

    @Test
    public void test_validate_fails_when_check_expr_is_false() throws Exception {
        expectedException.expectMessage(
            "Failed CONSTRAINT sentinel CHECK (\"sentinel\") and values {id=280278, qty=42, sentinel=false}");
        checkConstraints.validate(mapOf(
            "id", 280278,
            "qty", 42,
            "sentinel", false));
    }

    @Test
    public void test_validate_succeeds_when_check_expr_is_true() throws Exception {
        checkConstraints.validate(mapOf(
            "id", 280278,
            "qty", 42,
            "sentinel", true));
    }

    @Test
    public void test_validate_succeeds_when_check_expr_is_null() throws Exception {
        checkConstraints.validate(mapOf(
            "id", 280278,
            "qty", 42,
            "sentinel", null));
    }

    @Test
    public void test_cannot_have_two_check_constraints_of_same_name() throws Exception {
        expectedException.expectMessage(
            "a check constraint of the same name is already declared [id_is_even]");
        SQLExecutor.builder(clusterService)
            .addTable("CREATE TABLE t (" +
                      "    id int CONSTRAINT id_is_even CHECK(id % 2 = 0)," +
                      "    qty int," +
                      "    CONSTRAINT id_is_even CHECK(id % 2 = 0))")
            .build();
    }

    @Test
    public void test_expression_analyzer_on_check_constraints_containing_fields() {
        FieldProvider fieldProvider = FieldProvider.FIELDS_AS_LITERAL;
        // ^ ^ the culprit

        ExpressionAnalyzer analyzer = new ExpressionAnalyzer(
            sqlExecutor.functions(),
            (CoordinatorTxnCtx) txnCtx,
            ParamTypeHints.EMPTY,
            fieldProvider,
            null);
        ExpressionAnalysisContext exprAnalysisCtx = new ExpressionAnalysisContext();
        Expression checkExpression = SqlParser.createExpression("qty >= 0");
        String checkExpressionStr = ExpressionFormatter.formatExpression(checkExpression);
        TableElement<Expression> checkConstraint = new CheckConstraint<>("chk1", checkExpression, checkExpressionStr);
        TableElement<Symbol> checkConstraintSym = checkConstraint.map(y -> analyzer.convert(y, exprAnalysisCtx));
        // ^ ^ boom!

    }

    private static Map<String, Object> mapOf(Object... items) {
        if (items == null || items.length % 2 != 0) {
            throw new IllegalArgumentException("expected even number of items");
        }
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i=0; i < items.length - 1; i+=2) {
            Object k = items[i];
            if (k == null) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "item[%d] represents a key and those can't be null", i));
            }
            map.put(k.toString(), items[i+1]);
        }
        return map;
    }
}
