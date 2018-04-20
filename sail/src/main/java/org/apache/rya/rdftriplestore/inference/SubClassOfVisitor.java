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
package org.apache.rya.rdftriplestore.inference;

import java.util.Collection;
import java.util.UUID;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.utils.NullableStatementImpl;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

/**
 * Class SubClassOfVisitor
 * Date: Mar 29, 2011
 * Time: 11:28:34 AM
 */
public class SubClassOfVisitor extends AbstractInferVisitor {

    public SubClassOfVisitor(final RdfCloudTripleStoreConfiguration conf, final InferenceEngine inferenceEngine) {
        super(conf, inferenceEngine);
        include = conf.isInferSubClassOf();
    }

    @Override
    protected void meetSP(final StatementPattern node) throws Exception {
        final StatementPattern sp = node.clone();
        final Var predVar = sp.getPredicateVar();
        final Var objVar = sp.getObjectVar();
        final Var conVar = sp.getContextVar();
        if (predVar != null && objVar != null && objVar.getValue() != null && RDF.TYPE.equals(predVar.getValue())
                && !EXPANDED.equals(conVar)) {
            /**
             * ?type sesame:directSubClassOf ub:Student . ?student rdf:type ?type +
             */
//            String s = UUID.randomUUID().toString();
//            Var typeVar = new Var(s);
//            StatementPattern subClassOf = new StatementPattern(typeVar, new Var("c-" + s, SESAME.DIRECTSUBCLASSOF), objVar, SUBCLASS_EXPANDED);
//            StatementPattern rdfType = new StatementPattern(sp.getSubjectVar(), sp.getPredicateVar(), typeVar, SUBCLASS_EXPANDED);
//            InferJoin join = new InferJoin(subClassOf, rdfType);
//            join.getProperties().put(InferConstants.INFERRED, InferConstants.TRUE);
//            node.replaceWith(join);

            final IRI subclassof_iri = (IRI) objVar.getValue();
            final Collection<IRI> parents = InferenceEngine.findParents(inferenceEngine.getSubClassOfGraph(), subclassof_iri);
            if (parents != null && parents.size() > 0) {
                final String s = UUID.randomUUID().toString();
                final Var typeVar = new Var(s);
                final FixedStatementPattern fsp = new FixedStatementPattern(typeVar, new Var("c-" + s, RDFS.SUBCLASSOF), objVar, conVar);
                parents.add(subclassof_iri);
                for (final IRI iri : parents) {
                    fsp.statements.add(new NullableStatementImpl(iri, RDFS.SUBCLASSOF, subclassof_iri));
                }

                final StatementPattern rdfType = new DoNotExpandSP(sp.getSubjectVar(), sp.getPredicateVar(), typeVar, conVar);
                final InferJoin join = new InferJoin(fsp, rdfType);
                join.getProperties().put(InferConstants.INFERRED, InferConstants.TRUE);
                node.replaceWith(join);
            }

//            if (parents != null && parents.size() > 0) {
//                StatementPatterns statementPatterns = new StatementPatterns();
//                statementPatterns.patterns.add(node);
//                Var subjVar = node.getSubjectVar();
//                for (IRI iri : parents) {
//                    statementPatterns.patterns.add(new StatementPattern(subjVar, predVar, new Var(objVar.getName(), iri)));
//                }
//                node.replaceWith(statementPatterns);
//            }

//            if (parents != null && parents.size() > 0) {
//                VarCollection vc = new VarCollection();
//                vc.setName(objVar.getName());
//                vc.values.add(objVar);
//                for (IRI iri : parents) {
//                    vc.values.add(new Var(objVar.getName(), iri));
//                }
//                Var subjVar = node.getSubjectVar();
//                node.replaceWith(new StatementPattern(subjVar, predVar, vc, node.getContextVar()));
//            }
        }
    }

}
