package org.apache.rya.rdftriplestore.evaluation;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

/**
 * TODO: This might be a very bad thing. It may force all AND and not allow ORs?. Depends on how they do the bindings.
 * Class SeparateFilterJoinsVisitor
 * Date: Apr 11, 2011
 * Time: 10:16:15 PM
 */
public class SeparateFilterJoinsVisitor extends AbstractQueryModelVisitor<Exception> {
    @Override
    public void meet(final Filter node) throws Exception {
        super.meet(node);

        final ValueExpr condition = node.getCondition();
        final TupleExpr arg = node.getArg();
        if (!(arg instanceof Join)) {
            return;
        }

        final Join join = (Join) arg;
        final TupleExpr leftArg = join.getLeftArg();
        final TupleExpr rightArg = join.getRightArg();

        if (leftArg instanceof StatementPattern && rightArg instanceof StatementPattern) {
            final Filter left = new Filter(leftArg, condition);
            final Filter right = new Filter(rightArg, condition);
            node.replaceWith(new Join(left, right));
        }

    }
}
