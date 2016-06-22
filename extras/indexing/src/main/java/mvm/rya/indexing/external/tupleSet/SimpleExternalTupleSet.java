package mvm.rya.indexing.external.tupleSet;

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

import info.aduna.iteration.CloseableIteration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelVisitor;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

/**
 *  This a testing class to create mock pre-computed join nodes in order to
 *  test the {@link PrecompJoinOptimizer} for query planning.
 *
 */

public class SimpleExternalTupleSet extends ExternalTupleSet {

	public SimpleExternalTupleSet(Projection tuple) {
		this.setProjectionExpr(tuple);
		setSupportedVarOrders();
	}

	private void setSupportedVarOrders() {

		final Set<String> varSet = Sets.newHashSet();
		final Map<String, Set<String>> supportedVarOrders = new HashMap<>();
		String t = "";

		for (final String s : this.getTupleExpr().getAssuredBindingNames()) {
			if (t.length() == 0) {
				t = s;
			} else {
				t = t + VAR_ORDER_DELIM + s;
			}

			varSet.add(s);
			supportedVarOrders.put(t, new HashSet<String>(varSet));

		}
		this.setSupportedVariableOrderMap(supportedVarOrders);
	}

	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor)
			throws X {
		visitor.meetOther(this);
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
			BindingSet bindings) throws QueryEvaluationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getSignature() {
		return "(SimpleExternalTupleSet) "
				+ Joiner.on(", ")
						.join(this.getTupleExpr().getProjectionElemList()
								.getElements()).replaceAll("\\s+", " ");

	}

	@Override
	public boolean equals(Object other) {

		if (!(other instanceof SimpleExternalTupleSet)) {
			return false;
		} else {

			final SimpleExternalTupleSet arg = (SimpleExternalTupleSet) other;
			if (this.getTupleExpr().equals(arg.getTupleExpr())) {
				return true;
			} else {
				return false;
			}

		}

	}

}
