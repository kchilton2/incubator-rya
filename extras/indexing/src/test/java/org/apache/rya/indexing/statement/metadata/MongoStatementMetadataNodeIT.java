package org.apache.rya.indexing.statement.metadata;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.indexing.statement.metadata.matching.StatementMetadataNode;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.mongodb.MongoITBase;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MongoStatementMetadataNodeIT extends MongoITBase {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private final String query = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix ano: <http://www.w3.org/2002/07/owl#annotated> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y where {_:blankNode rdf:type owl:Annotation; ano:Source <http://Joe>; "
            + "ano:Property <http://worksAt>; ano:Target ?x; <http://createdBy> ?y; <http://createdOn> \'2017-01-04\'^^xsd:date }";
    private final String query2 = "prefix owl: <http://www.w3.org/2002/07/owl#> prefix ano: <http://www.w3.org/2002/07/owl#annotated> prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> select ?x ?y where {_:blankNode rdf:type owl:Annotation; ano:Source ?x; "
            + "ano:Property <http://worksAt>; ano:Target ?y; <http://createdBy> ?x; <http://createdOn> \'2017-01-04\'^^xsd:date }";

    @Before
    public void init() throws Exception {
        final Set<RyaIRI> propertySet = new HashSet<RyaIRI>(Arrays.asList(new RyaIRI("http://createdBy"), new RyaIRI("http://createdOn")));
        conf.setUseStatementMetadata(true);
        conf.setStatementMetadataProperties(propertySet);
    }

    @Test
    public void simpleQueryWithoutBindingSet() throws Exception {
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();
            StatementMetadata metadata = new StatementMetadata();
            metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaType("Joe"));
            metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

            RyaStatement statement = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                    new RyaType("CoffeeShop"), new RyaIRI("http://context"), "", metadata);
            dao.add(statement);

            SPARQLParser parser = new SPARQLParser();
            ParsedQuery pq = parser.parseQuery(query, null);
            List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());

            StatementMetadataNode<?> node = new StatementMetadataNode<>(spList, conf);
            CloseableIteration<BindingSet, QueryEvaluationException> iteration = node.evaluate(new QueryBindingSet());

            QueryBindingSet bs = new QueryBindingSet();
            bs.addBinding("x", VF.createLiteral("CoffeeShop"));
            bs.addBinding("y", VF.createLiteral("Joe"));

            List<BindingSet> bsList = new ArrayList<>();
            while (iteration.hasNext()) {
                bsList.add(iteration.next());
            }

            Assert.assertEquals(1, bsList.size());
            Assert.assertEquals(bs, bsList.get(0));
            dao.delete(statement, conf);
        } finally {
            dao.destroy();
        }
    }

    /**
     * Tests if results are filtered correctly using the metadata properties. In
     * this case, the date for the ingested RyaStatement differs from the date
     * specified in the query.
     */
    @Test
    public void simpleQueryWithoutBindingSetInvalidProperty() throws Exception {
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();	

            StatementMetadata metadata = new StatementMetadata();
            metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaType("Doug"));
            metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-02-15"));

            RyaStatement statement = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                    new RyaType("CoffeeShop"), new RyaIRI("http://context"), "", metadata);
            dao.add(statement);

            SPARQLParser parser = new SPARQLParser();
            ParsedQuery pq = parser.parseQuery(query, null);
            List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
            StatementMetadataNode<MongoDBRdfConfiguration> node = new StatementMetadataNode<>(spList, conf);
            CloseableIteration<BindingSet, QueryEvaluationException> iteration = node.evaluate(new QueryBindingSet());

            List<BindingSet> bsList = new ArrayList<>();
            while (iteration.hasNext()) {
                bsList.add(iteration.next());
            }
            Assert.assertEquals(0, bsList.size());
            dao.delete(statement, conf);
        } finally {
            dao.destroy();
        }
    }

    @Test
    public void simpleQueryWithBindingSet() throws Exception {
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();
            StatementMetadata metadata = new StatementMetadata();
            metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaType("Joe"));
            metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

            RyaStatement statement1 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                    new RyaType("CoffeeShop"), new RyaIRI("http://context"), "", metadata);
            RyaStatement statement2 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                    new RyaType("HardwareStore"), new RyaIRI("http://context"), "", metadata);
            dao.add(statement1);
            dao.add(statement2);

            SPARQLParser parser = new SPARQLParser();
            ParsedQuery pq = parser.parseQuery(query, null);
            List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
            StatementMetadataNode<MongoDBRdfConfiguration> node = new StatementMetadataNode<>(spList, conf);

            QueryBindingSet bsConstraint = new QueryBindingSet();
            bsConstraint.addBinding("x", VF.createLiteral("CoffeeShop"));
            bsConstraint.addBinding("z", VF.createLiteral("Virginia"));

            CloseableIteration<BindingSet, QueryEvaluationException> iteration = node.evaluate(bsConstraint);

            QueryBindingSet expected = new QueryBindingSet();
            expected.addBinding("x", VF.createLiteral("CoffeeShop"));
            expected.addBinding("y", VF.createLiteral("Joe"));
            expected.addBinding("z", VF.createLiteral("Virginia"));

            List<BindingSet> bsList = new ArrayList<>();
            while (iteration.hasNext()) {
                bsList.add(iteration.next());
            }

            Assert.assertEquals(1, bsList.size());
            Assert.assertEquals(expected, bsList.get(0));

            dao.delete(statement1, conf);
            dao.delete(statement2, conf);
        } finally {
            dao.destroy();
        }
    }

    /**
     * Tests to see if correct result is passed back when a metadata statement
     * is joined with a StatementPattern statement (i.e. a common variable
     * appears in a StatementPattern statement and a metadata statement).
     * StatementPattern statements have either rdf:subject, rdf:predicate, or
     * rdf:object as the predicate while a metadata statement is any statement
     * in the reified query whose predicate is not rdf:type and not a
     * StatementPattern predicate.
     */
    @Test
    public void simpleQueryWithBindingSetJoinPropertyToSubject() throws Exception {
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();
            StatementMetadata metadata = new StatementMetadata();
            metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaIRI("http://Joe"));
            metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

            RyaStatement statement1 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                    new RyaType("CoffeeShop"), new RyaIRI("http://context"), "", metadata);
            RyaStatement statement2 = new RyaStatement(new RyaIRI("http://Bob"), new RyaIRI("http://worksAt"),
                    new RyaType("HardwareStore"), new RyaIRI("http://context"), "", metadata);
            dao.add(statement1);
            dao.add(statement2);

            SPARQLParser parser = new SPARQLParser();
            ParsedQuery pq = parser.parseQuery(query2, null);
            List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
            StatementMetadataNode<MongoDBRdfConfiguration> node = new StatementMetadataNode<>(spList, conf);

            List<BindingSet> bsCollection = new ArrayList<>();
            QueryBindingSet bsConstraint1 = new QueryBindingSet();
            bsConstraint1.addBinding("y", VF.createLiteral("CoffeeShop"));
            bsConstraint1.addBinding("z", VF.createLiteral("Virginia"));

            QueryBindingSet bsConstraint2 = new QueryBindingSet();
            bsConstraint2.addBinding("y", VF.createLiteral("HardwareStore"));
            bsConstraint2.addBinding("z", VF.createLiteral("Maryland"));
            bsCollection.add(bsConstraint1);
            bsCollection.add(bsConstraint2);

            CloseableIteration<BindingSet, QueryEvaluationException> iteration = node.evaluate(bsCollection);

            QueryBindingSet expected = new QueryBindingSet();
            expected.addBinding("y", VF.createLiteral("CoffeeShop"));
            expected.addBinding("x", VF.createIRI("http://Joe"));
            expected.addBinding("z", VF.createLiteral("Virginia"));

            List<BindingSet> bsList = new ArrayList<>();
            while (iteration.hasNext()) {
                bsList.add(iteration.next());
            }

            Assert.assertEquals(1, bsList.size());
            Assert.assertEquals(expected, bsList.get(0));

            dao.delete(statement1, conf);
            dao.delete(statement2, conf);
        } finally {
            dao.destroy();
        }
    }

    /**
     * Tests if the StatementMetadataNode joins BindingSet correctly for
     * variables appearing in metadata statements. In this case, the metadata
     * statements are (_:blankNode <http://createdOn 2017-01-04 ) and
     * (_:blankNode <http://createdBy> ?y). The variable ?y appears as the
     * object in the above metadata statement and its values are joined to the
     * constraint BindingSets in the example below.
     */
    @Test
    public void simpleQueryWithBindingSetJoinOnProperty() throws Exception {
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();
            StatementMetadata metadata = new StatementMetadata();
            metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaType("Joe"));
            metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

            RyaStatement statement1 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                    new RyaType("CoffeeShop"), new RyaIRI("http://context"), "", metadata);
            dao.add(statement1);

            SPARQLParser parser = new SPARQLParser();
            ParsedQuery pq = parser.parseQuery(query, null);
            List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
            StatementMetadataNode<MongoDBRdfConfiguration> node = new StatementMetadataNode<>(spList, conf);

            QueryBindingSet bsConstraint = new QueryBindingSet();
            bsConstraint.addBinding("x", VF.createLiteral("CoffeeShop"));
            bsConstraint.addBinding("y", VF.createLiteral("Doug"));

            CloseableIteration<BindingSet, QueryEvaluationException> iteration = node.evaluate(bsConstraint);

            List<BindingSet> bsList = new ArrayList<>();
            while (iteration.hasNext()) {
                bsList.add(iteration.next());
            }

            Assert.assertEquals(0, bsList.size());
            dao.delete(statement1, conf);
        } finally {
            dao.destroy();
        }
    }

    /**
     * Tests if StatementMetadataNode joins BindingSet values correctly for
     * variables appearing as the object in one of the StatementPattern
     * statements (in the case ?x appears as the Object in the statement
     * _:blankNode rdf:object ?x). StatementPattern statements have either
     * rdf:subject, rdf:predicate, or rdf:object as the predicate.
     */
    @Test
    public void simpleQueryWithBindingSetCollection() throws Exception {
        MongoDBRyaDAO dao = new MongoDBRyaDAO();
        try {
            dao.setConf(conf);
            dao.init();
            StatementMetadata metadata = new StatementMetadata();
            metadata.addMetadata(new RyaIRI("http://createdBy"), new RyaType("Joe"));
            metadata.addMetadata(new RyaIRI("http://createdOn"), new RyaType(XMLSchema.DATE, "2017-01-04"));

            RyaStatement statement1 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                    new RyaType("CoffeeShop"), new RyaIRI("http://context"), "", metadata);
            RyaStatement statement2 = new RyaStatement(new RyaIRI("http://Joe"), new RyaIRI("http://worksAt"),
                    new RyaType("HardwareStore"), new RyaIRI("http://context"), "", metadata);
            dao.add(statement1);
            dao.add(statement2);

            SPARQLParser parser = new SPARQLParser();
            ParsedQuery pq = parser.parseQuery(query, null);
            List<StatementPattern> spList = StatementPatternCollector.process(pq.getTupleExpr());
            StatementMetadataNode<MongoDBRdfConfiguration> node = new StatementMetadataNode<>(spList, conf);

            List<BindingSet> bsCollection = new ArrayList<>();
            QueryBindingSet bsConstraint1 = new QueryBindingSet();
            bsConstraint1.addBinding("x", VF.createLiteral("CoffeeShop"));
            bsConstraint1.addBinding("z", VF.createLiteral("Virginia"));

            QueryBindingSet bsConstraint2 = new QueryBindingSet();
            bsConstraint2.addBinding("x", VF.createLiteral("HardwareStore"));
            bsConstraint2.addBinding("z", VF.createLiteral("Maryland"));

            QueryBindingSet bsConstraint3 = new QueryBindingSet();
            bsConstraint3.addBinding("x", VF.createLiteral("BurgerShack"));
            bsConstraint3.addBinding("z", VF.createLiteral("Delaware"));
            bsCollection.add(bsConstraint1);
            bsCollection.add(bsConstraint2);
            bsCollection.add(bsConstraint3);

            CloseableIteration<BindingSet, QueryEvaluationException> iteration = node.evaluate(bsCollection);

            Set<BindingSet> expected = new HashSet<>();
            QueryBindingSet expected1 = new QueryBindingSet();
            expected1.addBinding("x", VF.createLiteral("CoffeeShop"));
            expected1.addBinding("y", VF.createLiteral("Joe"));
            expected1.addBinding("z", VF.createLiteral("Virginia"));

            QueryBindingSet expected2 = new QueryBindingSet();
            expected2.addBinding("x", VF.createLiteral("HardwareStore"));
            expected2.addBinding("y", VF.createLiteral("Joe"));
            expected2.addBinding("z", VF.createLiteral("Maryland"));
            expected.add(expected1);
            expected.add(expected2);

            Set<BindingSet> bsSet = new HashSet<>();
            while (iteration.hasNext()) {
                bsSet.add(iteration.next());
            }

            Assert.assertEquals(expected, bsSet);

            dao.delete(statement1, conf);
            dao.delete(statement2, conf);
        } finally {
            dao.destroy();
        }
    }
}
