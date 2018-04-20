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

import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

/**
 * Class ReorderJoinVisitor
 * Date: Apr 11, 2011
 * Time: 10:16:15 PM
 */
public class PushJoinDownVisitor extends AbstractQueryModelVisitor<Exception> {
    @Override
    public void meet(final Join node) throws Exception {
        super.meet(node);

        final TupleExpr leftArg = node.getLeftArg();
        final TupleExpr rightArg = node.getRightArg();

        /**
         * if join(join(1, 2), join(3,4))
         * should be:
         * join(join(join(1,2), 3), 4)
         */
        if (leftArg instanceof Join && rightArg instanceof Join) {
            final Join leftJoin = (Join) leftArg;
            final Join rightJoin = (Join) rightArg;
            final TupleExpr right_LeftArg = rightJoin.getLeftArg();
            final TupleExpr right_rightArg = rightJoin.getRightArg();
            final Join inner = new Join(leftJoin, right_LeftArg);
            final Join outer = new Join(inner, right_rightArg);
            node.replaceWith(outer);
        }

    }
}
