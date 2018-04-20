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
package org.apache.rya.streams.kafka.processors.join;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.rya.api.function.join.NaturalJoin;
import org.apache.rya.api.function.projection.RandomUUIDFactory;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.RyaStreamsTestUtil;
import org.apache.rya.streams.kafka.processors.ProcessorResult;
import org.apache.rya.streams.kafka.processors.ProcessorResult.UnaryResult;
import org.apache.rya.streams.kafka.processors.join.JoinProcessorSupplier.JoinProcessor;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetDeserializer;
import org.apache.rya.streams.kafka.topology.TopologyFactory;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Integration tests the methods of {@link JoinProcessor}.
 */
public class JoinProcessorIT {

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(true);

    @Test(expected = IllegalArgumentException.class)
    public void badAllVars() throws IllegalArgumentException {
        new JoinProcessorSupplier(
                "NATURAL_JOIN",
                new NaturalJoin(),
                Lists.newArrayList("employee"),
                Lists.newArrayList("person", "employee", "business"),
                result -> ProcessorResult.make( new UnaryResult(result) ));
    }

    @Test
    public void newLeftResult() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(ryaInstance, queryId);

        // Setup a topology.
        final String query =
                "SELECT * WHERE { " +
                    "?person <urn:talksTo> ?employee ." +
                    "?employee <urn:worksAt> ?business" +
                " }";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create some statements that generate a bunch of right SP results.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:TacoPlace")), "a&b") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Charlie"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:BurgerJoint")), "a") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Eve"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:CoffeeShop")), "b") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:BurgerJoint")), "b|c") );

        // Add a statement that will generate a left result that joins with some of those right results.
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob")), "c") );

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("employee", vf.createIRI("urn:Bob"));
        bs.addBinding("business", vf.createIRI("urn:TacoPlace"));
        expected.add( new VisibilityBindingSet(bs, "a&b&c") );

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("employee", vf.createIRI("urn:Bob"));
        bs.addBinding("business", vf.createIRI("urn:BurgerJoint"));
        expected.add( new VisibilityBindingSet(bs, "c&(b|c)") );

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void newRightResult() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(ryaInstance, queryId);

        // Setup a topology.
        final String query =
                "SELECT * WHERE { " +
                    "?person <urn:talksTo> ?employee ." +
                    "?employee <urn:worksAt> ?business" +
                " }";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create some statements that generate a bunch of right SP results.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:TacoPlace")), "a&b") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Charlie"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:BurgerJoint")), "a") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Eve"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:CoffeeShop")), "b") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:BurgerJoint")), "b|c") );

        // Add a statement that will generate a left result that joins with some of those right results.
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob")), "c") );

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("employee", vf.createIRI("urn:Bob"));
        bs.addBinding("business", vf.createIRI("urn:TacoPlace"));
        expected.add( new VisibilityBindingSet(bs, "a&b&c") );

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("employee", vf.createIRI("urn:Bob"));
        bs.addBinding("business", vf.createIRI("urn:BurgerJoint"));
        expected.add( new VisibilityBindingSet(bs, "c&(b|c)") );

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void newResultsBothSides() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(ryaInstance, queryId);

        // Setup a topology.
        final String query =
                "SELECT * WHERE { " +
                    "?person <urn:talksTo> ?employee ." +
                    "?employee <urn:worksAt> ?business" +
                " }";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create some statements that generate a bunch of right SP results.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:TacoPlace")), "a&b") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob")), "c") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Charlie"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:BurgerJoint")), "a") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Eve"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:CoffeeShop")), "b") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:BurgerJoint")), "b|c") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Charlie")), "c") );

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("employee", vf.createIRI("urn:Bob"));
        bs.addBinding("business", vf.createIRI("urn:TacoPlace"));
        expected.add( new VisibilityBindingSet(bs, "a&b&c") );

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("employee", vf.createIRI("urn:Bob"));
        bs.addBinding("business", vf.createIRI("urn:BurgerJoint"));
        expected.add( new VisibilityBindingSet(bs, "c&(b|c)") );

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Bob"));
        bs.addBinding("employee", vf.createIRI("urn:Charlie"));
        bs.addBinding("business", vf.createIRI("urn:BurgerJoint"));
        expected.add( new VisibilityBindingSet(bs, "a&c") );

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void manyJoins() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(ryaInstance, queryId);

        // Setup a topology.
        final String query =
                "SELECT * WHERE { " +
                    "?person <urn:talksTo> ?employee ." +
                    "?employee <urn:worksAt> ?business ." +
                    "?employee <urn:hourlyWage> ?wage ." +
                " }";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create some statements that generate a bunch of right SP results.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob")), "a") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:BurgerJoint")), "a") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:hourlyWage"), vf.createLiteral(7.25)), "a") );

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("employee", vf.createIRI("urn:Bob"));
        bs.addBinding("business", vf.createIRI("urn:BurgerJoint"));
        bs.addBinding("wage", vf.createLiteral(7.25));
        expected.add( new VisibilityBindingSet(bs, "a") );

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, statements, expected, VisibilityBindingSetDeserializer.class);
    }

    @Test
    public void leftJoin() throws Exception {
        // Enumerate some topics that will be re-used
        final String ryaInstance = UUID.randomUUID().toString();
        final UUID queryId = UUID.randomUUID();
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(ryaInstance, queryId);

        // Setup a topology.
        final String query =
                "SELECT * WHERE { " +
                    "?person <urn:talksTo> ?employee ." +
                    "OPTIONAL{ ?employee <urn:worksAt> ?business } " +
                " }";
        final TopologyFactory factory = new TopologyFactory();
        final TopologyBuilder builder = factory.build(query, statementsTopic, resultsTopic, new RandomUUIDFactory());

        // Create some statements that generate a result that includes the optional value as well as one that does not.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final List<VisibilityStatement> statements = new ArrayList<>();
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob")), "a") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:TacoPlace")), "b") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Charlie")), "c") );
        statements.add( new VisibilityStatement(
                vf.createStatement(vf.createIRI("urn:David"), vf.createIRI("urn:worksAt"), vf.createIRI("urn:BurgerJoint")), "d") );

        // Make the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("employee", vf.createIRI("urn:Bob"));
        expected.add( new VisibilityBindingSet(bs, "a") );

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("employee", vf.createIRI("urn:Bob"));
        bs.addBinding("business", vf.createIRI("urn:TacoPlace"));
        expected.add( new VisibilityBindingSet(bs, "a&b") );

        bs = new MapBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Bob"));
        bs.addBinding("employee", vf.createIRI("urn:Charlie"));
        expected.add( new VisibilityBindingSet(bs, "c") );

        // Run the test.
        RyaStreamsTestUtil.runStreamProcessingTest(kafka, statementsTopic, resultsTopic, builder, statements, expected, VisibilityBindingSetDeserializer.class);
    }
}