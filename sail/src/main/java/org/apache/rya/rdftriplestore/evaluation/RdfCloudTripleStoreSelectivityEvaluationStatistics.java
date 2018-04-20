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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.persist.RdfEvalStatsDAO;
import org.apache.rya.api.persist.joinselect.SelectivityEvalDAO;
import org.apache.rya.rdftriplestore.inference.DoNotExpandSP;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.StatementPattern;

public class RdfCloudTripleStoreSelectivityEvaluationStatistics<C extends RdfCloudTripleStoreConfiguration> extends RdfCloudTripleStoreEvaluationStatistics {

  // allows access to join selectivity and extending RdfCloudTripleStoreEvaluationStatistics allows for use of prospector
  private SelectivityEvalDAO<C> selectEvalStatsDAO; // TODO redundancy here as RdfCloudTripleStoreEvalStats object contains
                                                 // RdfEvalStatsDAO object

  protected double filterCard;
  C config; // TODO redundancy here as RdfCloudTripleStoreEvalStats object contains conf as well

  public RdfCloudTripleStoreSelectivityEvaluationStatistics(final C conf,
      final RdfEvalStatsDAO<C> prospector, final SelectivityEvalDAO<C> selectEvalStatsDAO) {

    super(conf, prospector);
    checkNotNull(selectEvalStatsDAO);

    try {
      this.selectEvalStatsDAO = selectEvalStatsDAO;
      this.config = conf; // TODO fix this!
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected CardinalityCalculator createCardinalityCalculator() {
    try {
      return new SelectivityCardinalityCalculator(this);
    } catch (final Exception e) {
      System.out.println(e);
      throw new RuntimeException(e);
    }
  }

  public class SelectivityCardinalityCalculator extends RdfCloudTripleStoreCardinalityCalculator {

    public SelectivityCardinalityCalculator(final RdfCloudTripleStoreSelectivityEvaluationStatistics<C> statistics) {
      super(statistics);
    }

    @Override
    public void meet(final Join node) {
      node.getLeftArg().visit(this);
      final double leftArgCost = cardinality;
      // System.out.println("Left cardinality is " + cardinality);
      node.getRightArg().visit(this);

      if (node.getLeftArg() instanceof FixedStatementPattern && node.getRightArg() instanceof DoNotExpandSP) {
        return;
      }

      try {
        final double selectivity = selectEvalStatsDAO.getJoinSelect(config, node.getLeftArg(), node.getRightArg());
//        System.out.println("CardCalc: left cost of " + node.getLeftArg() + " is " + leftArgCost + " right cost of "
//        + node.getRightArg() + " is " + cardinality);
//         System.out.println("Right cardinality is " + cardinality);
        cardinality += leftArgCost + leftArgCost * cardinality * selectivity;
//        System.out.println("CardCalc: Cardinality is " + cardinality);
//        System.out.println("CardCalc: Selectivity is " + selectivity);
        // System.out.println("Join cardinality is " + cardinality);

      } catch (final Exception e) {
        e.printStackTrace();
      }

    }




        @Override
        public double getCardinality(final StatementPattern node) {

            cardinality = super.getCardinality(node);

            // If sp contains all variables or is EmptyRDFtype, assign
            // cardinality
            // equal to table size
            if (cardinality == Double.MAX_VALUE || cardinality == Double.MAX_VALUE - 1) {
                try {
                    cardinality = selectEvalStatsDAO.getTableSize(config);
                } catch (final Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            return cardinality;
        }




  }

}
