package org.apache.rya.rdftriplestore.inference;

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

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.Var;

/**
 * All predicates are changed
 * Class SubPropertyOfVisitor
 * Date: Mar 29, 2011
 * Time: 11:28:34 AM
 */
public class SymmetricPropertyVisitor extends AbstractInferVisitor {

    public SymmetricPropertyVisitor(RdfCloudTripleStoreConfiguration conf, InferenceEngine inferenceEngine) {
        super(conf, inferenceEngine);
        include = conf.isInferSymmetricProperty();
    }

    @Override
    protected void meetSP(StatementPattern node) throws Exception {
        StatementPattern sp = node.clone();

        final Var predVar = sp.getPredicateVar();
        IRI pred = (IRI) predVar.getValue();
        String predNamespace = pred.getNamespace();

        final Var objVar = sp.getObjectVar();
        final Var cntxtVar = sp.getContextVar();
        if (objVar != null &&
                !RDF.NAMESPACE.equals(predNamespace) &&
                !SESAME.NAMESPACE.equals(predNamespace) &&
                !RDFS.NAMESPACE.equals(predNamespace)
                && !EXPANDED.equals(cntxtVar)) {
            /**
             *
             * { ?a ?pred ?b .}\n" +
             "       UNION " +
             "      { ?b ?pred ?a }
             */

            IRI symmPropIri = (IRI) predVar.getValue();
            if(inferenceEngine.isSymmetricProperty(symmPropIri)) {
                Var subjVar = sp.getSubjectVar();
                Union union = new InferUnion();
                union.setLeftArg(sp);
                union.setRightArg(new StatementPattern(objVar, predVar, subjVar, cntxtVar));
                node.replaceWith(union);
            }
        }
    }
}
