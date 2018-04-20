/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.mongo;

import java.util.List;

import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.indexing.entity.EntityIndexOptimizer;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Property;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.query.EntityQueryNode;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.TypeStorage;
import org.apache.rya.mongodb.MongoITBase;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.helpers.StatementPatternCollector;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class MongoEntityIndex2IT extends MongoITBase {
    private static final Type PERSON_TYPE =
            new Type(new RyaIRI("urn:person"),
                ImmutableSet.<RyaIRI>builder()
                    .add(new RyaIRI("urn:name"))
                    .add(new RyaIRI("urn:age"))
                    .add(new RyaIRI("urn:eye"))
                    .build());
    private static final RyaIRI RYA_PERSON_TYPE = new RyaIRI("urn:person");

    private EntityIndexOptimizer optimizer;
    private EntityStorage entityStorage;

    @Before
    public void beforeClass() throws Exception {
        optimizer = new EntityIndexOptimizer();
        optimizer.setConf(conf);

        final TypeStorage typeStorage = optimizer.getTypeStorage();
        typeStorage.create(PERSON_TYPE);

        final Entity entity = Entity.builder()
                .setSubject(new RyaIRI("urn:SSN:111-11-1111"))
                .setExplicitType(RYA_PERSON_TYPE)
                .setProperty(RYA_PERSON_TYPE, new Property(new RyaIRI("urn:age"), new RyaType("25")))
                .setProperty(RYA_PERSON_TYPE, new Property(new RyaIRI("urn:eye"), new RyaType("blue")))
                .setProperty(RYA_PERSON_TYPE, new Property(new RyaIRI("urn:name"), new RyaType("bob")))
                .build();
        entityStorage = optimizer.getEntityStorage();
        entityStorage.create(entity);
    }

    @Test()
    public void queryIsFullEntity() throws Exception {
        // A pattern that has two different subjects.
        final String query = "SELECT * WHERE { " +
                "<urn:SSN:111-11-1111> <" + RDF.TYPE + "> <urn:person> ."+
                "<urn:SSN:111-11-1111> <urn:age> ?age . " +
                "<urn:SSN:111-11-1111> <urn:name> ?name . " +
                "<urn:SSN:111-11-1111> <urn:eye> ?eye . " +
            "}";

        final EntityQueryNode expected = new EntityQueryNode(PERSON_TYPE, getSPs(query), entityStorage);
        assertOptimizer(query, expected);
    }

    @Test()
    public void queryIsPartEntity() throws Exception {
        // A pattern that has two different subjects.
        final String query = "SELECT * WHERE { " +
                "<urn:SSN:111-11-1111> <" + RDF.TYPE + "> <urn:person> ."+
                "<urn:SSN:111-11-1111> <urn:age> ?age . " +
            "}";

        final EntityQueryNode expected = new EntityQueryNode(PERSON_TYPE, getSPs(query), entityStorage);
        assertOptimizer(query, expected);
    }

    @Test()
    public void queryIsPartEntityandExtra() throws Exception {
        // A pattern that has two different subjects.
        final String query = "SELECT * WHERE { " +
                "<urn:SSN:111-11-1111> <" + RDF.TYPE + "> <urn:person> ."+
                "<urn:SSN:111-11-1111> <urn:age> ?age . " +
                "<urn:SSN:222-22-2222> <urn:age> ?age . " +
                "<urn:SSN:222-22-2222> <urn:eye> ?eye . " +
                "<urn:SSN:222-22-2222> <urn:name> ?name . " +
            "}";

        final String expectedQuery = "SELECT * WHERE { " +
                "<urn:SSN:111-11-1111> <" + RDF.TYPE + "> <urn:person> ."+
                "<urn:SSN:111-11-1111> <urn:age> ?age . " +
            "}";

        final EntityQueryNode expected = new EntityQueryNode(PERSON_TYPE, getSPs(expectedQuery), entityStorage);
        assertOptimizer(query, expected);
    }

    @Test()
    public void queryIsFullEntityWithExtra() throws Exception {
        // A pattern that has two different subjects.
        final String query = "SELECT * WHERE { " +
                "<urn:SSN:111-11-1111> <" + RDF.TYPE + "> <urn:person> ."+
                "<urn:SSN:111-11-1111> <urn:age> ?age . " +
                "<urn:SSN:111-11-1111> <urn:eye> ?eye . " +
                "<urn:SSN:111-11-1111> <urn:name> ?name . " +
                "<urn:SSN:222-22-2222> <urn:age> ?age . " +
                "<urn:SSN:222-22-2222> <urn:eye> ?eye . " +
                "<urn:SSN:222-22-2222> <urn:name> ?name . " +
            "}";

        final String expectedQuery = "SELECT * WHERE { " +
                "<urn:SSN:111-11-1111> <" + RDF.TYPE + "> <urn:person> ."+
                "<urn:SSN:111-11-1111> <urn:age> ?age . " +
                "<urn:SSN:111-11-1111> <urn:eye> ?eye . " +
                "<urn:SSN:111-11-1111> <urn:name> ?name . " +
            "}";

        final EntityQueryNode expected = new EntityQueryNode(PERSON_TYPE, getSPs(expectedQuery), entityStorage);
        assertOptimizer(query, expected);
    }

    @Test()
    public void queryIsFullEntityWithOptional() throws Exception {
        // A pattern that has two different subjects.
        final String query = "SELECT * WHERE { " +
                "<urn:SSN:111-11-1111> <" + RDF.TYPE + "> <urn:person> ."+
                "<urn:SSN:111-11-1111> <urn:age> ?age . " +
                "<urn:SSN:111-11-1111> <urn:eye> ?eye . " +
                "<urn:SSN:111-11-1111> <urn:name> ?name . " +
                "  OPTIONAL{" +
                "    <urn:SSN:222-22-2222> <urn:age> ?age . " +
                "    <urn:SSN:222-22-2222> <urn:eye> ?eye . " +
                "    <urn:SSN:222-22-2222> <urn:name> ?name . " +
                " } . " +
            "}";

        final String expectedQuery = "SELECT * WHERE { " +
                "<urn:SSN:111-11-1111> <" + RDF.TYPE + "> <urn:person> ."+
                "<urn:SSN:111-11-1111> <urn:age> ?age . " +
                "<urn:SSN:111-11-1111> <urn:eye> ?eye . " +
                "<urn:SSN:111-11-1111> <urn:name> ?name . " +
            "}";

        final EntityQueryNode expected = new EntityQueryNode(PERSON_TYPE, getSPs(expectedQuery), entityStorage);
        assertOptimizer(query, expected);
    }

    @Test()
    public void queryIsSplitEntityWithOptional() throws Exception {
        // A pattern that has two different subjects.
        final String query = "SELECT * WHERE { " +
                "<urn:SSN:111-11-1111> <" + RDF.TYPE + "> <urn:person> ."+
                "<urn:SSN:111-11-1111> <urn:age> ?age . " +
                "<urn:SSN:111-11-1111> <urn:eye> ?eye . " +
                "  OPTIONAL{" +
                "    <urn:SSN:111-11-1111> <urn:name> ?name . " +
                " } . " +
            "}";

        final String expectedQuery = "SELECT * WHERE { " +
                "<urn:SSN:111-11-1111> <" + RDF.TYPE + "> <urn:person> ."+
                "<urn:SSN:111-11-1111> <urn:age> ?age . " +
                "<urn:SSN:111-11-1111> <urn:eye> ?eye . " +
            "}";

        final EntityQueryNode expected = new EntityQueryNode(PERSON_TYPE, getSPs(expectedQuery), entityStorage);
        assertOptimizer(query, expected);
    }

    @Test()
    public void queryEntityInOptional() throws Exception {
        // A pattern that has two different subjects.
        final String query = "SELECT * WHERE { " +
                "<urn:SSN:111-11-1111> <urn:age> ?age . " +
                "<urn:SSN:111-11-1111> <urn:eye> ?eye . " +
                "  OPTIONAL{" +
                "    <urn:SSN:111-11-1111> <" + RDF.TYPE + "> <urn:person> ."+
                "    <urn:SSN:111-11-1111> <urn:name> ?name . " +
                " } . " +
            "}";

        final String expectedQuery = "SELECT * WHERE { " +
                "<urn:SSN:111-11-1111> <" + RDF.TYPE + "> <urn:person> ."+
                "<urn:SSN:111-11-1111> <urn:name> ?name . " +
            "}";

        final EntityQueryNode expected = new EntityQueryNode(PERSON_TYPE, getSPs(expectedQuery), entityStorage);
        assertOptimizer(query, expected);
    }

    private static List<StatementPattern> getSPs(final String sparql) throws MalformedQueryException {
        final StatementPatternCollector spCollector = new StatementPatternCollector();
        new SPARQLParser().parseQuery(sparql, null).getTupleExpr().visit(spCollector);
        return spCollector.getStatementPatterns();
    }

    private void assertOptimizer(final String query, final EntityQueryNode expected) throws Exception {
        final SPARQLParser parser = new SPARQLParser();
        final TupleExpr expr = parser.parseQuery(query, null).getTupleExpr();

        optimizer.optimize(expr, null, null);
        expr.visit(new EntityFetchingAsserterVisitor(expected));
    }

    private class EntityFetchingAsserterVisitor extends AbstractQueryModelVisitor<Exception> {
        private final EntityQueryNode expected;
        public EntityFetchingAsserterVisitor(final EntityQueryNode expected) {
            this.expected = expected;
        }
        @Override
        protected void meetNode(final QueryModelNode node) throws Exception {
            if(node instanceof EntityQueryNode) {
                Assert.assertEquals(expected, node);
            } else {
                super.meetNode(node);
            }
        }
    }
}

