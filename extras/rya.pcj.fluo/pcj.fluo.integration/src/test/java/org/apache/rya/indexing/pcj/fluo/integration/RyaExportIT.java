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
package org.apache.rya.indexing.pcj.fluo.integration;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.indexing.pcj.fluo.api.CreateFluoPcj;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

/**
 * Performs integration tests over the Fluo application geared towards Rya PCJ exporting.
 */
public class RyaExportIT extends RyaExportITBase {

    @Test
    public void resultsExported() throws Exception {
        final String sparql =
                "SELECT ?customer ?worker ?city " +
                "{ " +
                  "FILTER(?customer = <http://Alice>) " +
                  "FILTER(?city = <http://London>) " +
                  "?customer <http://talksTo> ?worker. " +
                  "?worker <http://livesIn> ?city. " +
                  "?worker <http://worksAt> <http://Chipotle>. " +
                "}";

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                new RyaStatement(new RyaIRI("http://Alice"), new RyaIRI("http://talksTo"), new RyaIRI("http://Bob")),
                new RyaStatement(new RyaIRI("http://Bob"), new RyaIRI("http://livesIn"), new RyaIRI("http://London")),
                new RyaStatement(new RyaIRI("http://Bob"), new RyaIRI("http://worksAt"), new RyaIRI("http://Chipotle")),

                new RyaStatement(new RyaIRI("http://Alice"), new RyaIRI("http://talksTo"), new RyaIRI("http://Charlie")),
                new RyaStatement(new RyaIRI("http://Charlie"), new RyaIRI("http://livesIn"), new RyaIRI("http://London")),
                new RyaStatement(new RyaIRI("http://Charlie"), new RyaIRI("http://worksAt"), new RyaIRI("http://Chipotle")),

                new RyaStatement(new RyaIRI("http://Alice"), new RyaIRI("http://talksTo"), new RyaIRI("http://David")),
                new RyaStatement(new RyaIRI("http://David"), new RyaIRI("http://livesIn"), new RyaIRI("http://London")),
                new RyaStatement(new RyaIRI("http://David"), new RyaIRI("http://worksAt"), new RyaIRI("http://Chipotle")),

                new RyaStatement(new RyaIRI("http://Alice"), new RyaIRI("http://talksTo"), new RyaIRI("http://Eve")),
                new RyaStatement(new RyaIRI("http://Eve"), new RyaIRI("http://livesIn"), new RyaIRI("http://Leeds")),
                new RyaStatement(new RyaIRI("http://Eve"), new RyaIRI("http://worksAt"), new RyaIRI("http://Chipotle")),

                new RyaStatement(new RyaIRI("http://Frank"), new RyaIRI("http://talksTo"), new RyaIRI("http://Alice")),
                new RyaStatement(new RyaIRI("http://Frank"), new RyaIRI("http://livesIn"), new RyaIRI("http://London")),
                new RyaStatement(new RyaIRI("http://Frank"), new RyaIRI("http://worksAt"), new RyaIRI("http://Chipotle")));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("customer", vf.createIRI("http://Alice"));
        bs.addBinding("worker", vf.createIRI("http://Bob"));
        bs.addBinding("city", vf.createIRI("http://London"));
        expected.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("customer", vf.createIRI("http://Alice"));
        bs.addBinding("worker", vf.createIRI("http://Charlie"));
        bs.addBinding("city", vf.createIRI("http://London"));
        expected.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("customer", vf.createIRI("http://Alice"));
        bs.addBinding("worker", vf.createIRI("http://David"));
        bs.addBinding("city", vf.createIRI("http://London"));
        expected.add(bs);

        // Create the PCJ table.
        final Connector accumuloConn = super.getAccumuloConnector();
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, getRyaInstanceName());
        final String pcjId = pcjStorage.createPcj(sparql);

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            // Tell the Fluo app to maintain the PCJ.
            new CreateFluoPcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, getRyaInstanceName());

            // Stream the data into Fluo.
            new InsertTriples().insert(fluoClient, streamedTriples, Optional.absent());

            // Fetch the exported results from Accumulo once the observers finish working.
            super.getMiniFluo().waitForObservers();

            // Fetch expected results from the PCJ table that is in Accumulo.
            final Set<BindingSet> results = Sets.newHashSet( pcjStorage.listResults(pcjId) );

            // Verify the end results of the query match the expected results.
            assertEquals(expected, results);
        }
    }
}