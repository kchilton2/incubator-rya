/**
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
package org.apache.rya.indexing.accumulo.statistics;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.sail.config.RyaSailFactory;
import org.apache.rya.test.accumulo.AccumuloITBase;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.Test;

/**
 * Integration tests for the methods of {@link AccumuloStatementCountIndexer} to ensure both the methods produce the
 * desired result as well as to ensure the indexer is properly integrated with Sail.
 */
public class AccumuloStatementCountIndexerIT extends AccumuloITBase {

    private RyaClient makeRyaClient() throws Exception {
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());
        return AccumuloRyaClientFactory.build(connectionDetails, super.getConnector());
    }

    private Sail makeSail() throws Exception {
        final AccumuloRdfConfiguration ryaConf = new AccumuloRdfConfiguration();
        ryaConf.setTablePrefix( getRyaInstanceName() );
        ryaConf.setAccumuloUser( getUsername() );
        ryaConf.setAccumuloPassword( getPassword() );
        ryaConf.setAccumuloInstance( getInstanceName() );
        ryaConf.setAccumuloZookeepers( getZookeepers() );

        return RyaSailFactory.getInstance(ryaConf);
    }

    @Test
    public void storeStatement() throws Exception {
        // Install an instance of Rya with the statements feature enabled.
        final String ryaInstanceName = getRyaInstanceName();
        final RyaClient client = makeRyaClient();

        client.getInstall().install(ryaInstanceName, InstallConfiguration.builder()
                .setMaintainStatementCounts(true)
                .build());

        // Write a statement to the Rya instance.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Resource contextA = vf.createIRI("urn:contextA");

        final Sail sail = makeSail();
        try {
            final Statement statement = vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Alice"), contextA);
            new SailRepository(sail).getConnection().add(statement);
        } finally {
            sail.shutDown();
        }

        // Verify the count for the statement's context was incremented.
        assertEquals(1, client.getStatementCount().get().getStatementCount(ryaInstanceName, contextA));
    }

    @Test
    public void storeStatements() throws Exception {
        // Install an instance of Rya with the statements feature enabled.
        final String ryaInstanceName = getRyaInstanceName();
        final RyaClient client = makeRyaClient();

        client.getInstall().install(ryaInstanceName, InstallConfiguration.builder()
                .setMaintainStatementCounts(true)
                .build());

        // Write statements to the Rya instance.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Resource contextA = vf.createIRI("urn:contextA");
        final Resource contextB = vf.createIRI("urn:contextB");

        final List<Statement> statements = new ArrayList<>();
        // Context A statements.
        statements.add( vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob"), contextA));
        statements.add( vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Charlie"), contextA));
        statements.add( vf.createStatement(vf.createIRI("urn:Charlie"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:David"), contextA));
        statements.add( vf.createStatement(vf.createIRI("urn:David"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Eve"), contextA));
        statements.add( vf.createStatement(vf.createIRI("urn:Eve"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Frank"), contextA));

        // Context B stat
        statements.add( vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob"), contextB));
        statements.add( vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Charlie"), contextB));
        statements.add( vf.createStatement(vf.createIRI("urn:Charlie"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:David"), contextB));

        final Sail sail = makeSail();
        try {
            new SailRepository(sail).getConnection().add(statements);
        } finally {
            sail.shutDown();
        }

        // Verify the count for the statement's context was incremented.
        assertEquals(5, client.getStatementCount().get().getStatementCount(ryaInstanceName, contextA));
        assertEquals(3, client.getStatementCount().get().getStatementCount(ryaInstanceName, contextB));
    }

    @Test
    public void deleteStatement() throws Exception {
        // Install an instance of Rya with the statements feature enabled.
        final String ryaInstanceName = getRyaInstanceName();
        final RyaClient client = makeRyaClient();

        client.getInstall().install(ryaInstanceName, InstallConfiguration.builder()
                .setMaintainStatementCounts(true)
                .build());

        // Write a statement to the Rya instance.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Resource contextA = vf.createIRI("urn:contextA");

        final Sail sail = makeSail();
        try {
            // Write a statement to Rya.
            final Statement statement = vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Alice"), contextA);
            final SailRepository sailRepo = new SailRepository(sail);
            SailRepositoryConnection conn = sailRepo.getConnection();
            conn.add(statement);
            conn.commit();

            // Show it was counted.
            assertEquals(1, client.getStatementCount().get().getStatementCount(ryaInstanceName, contextA));

            // Delete the statement from Rya.
            conn = sailRepo.getConnection();
            conn.remove(statement);
            conn.commit();

            // Show the count was decremented.
            assertEquals(0, client.getStatementCount().get().getStatementCount(ryaInstanceName, contextA));
        } finally {
            sail.shutDown();
        }
    }

    @Test
    public void deleteGraph() throws Exception {
        // Install an instance of Rya with the statements feature enabled.
        final String ryaInstanceName = getRyaInstanceName();
        final RyaClient client = makeRyaClient();

        client.getInstall().install(ryaInstanceName, InstallConfiguration.builder()
                .setMaintainStatementCounts(true)
                .build());

        // Write statements to the Rya instance.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Resource contextA = vf.createIRI("urn:contextA");
        final Resource contextB = vf.createIRI("urn:contextB");

        final List<Statement> statements = new ArrayList<>();
        // Context A statements.
        statements.add( vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob"), contextA));
        statements.add( vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Charlie"), contextA));
        statements.add( vf.createStatement(vf.createIRI("urn:Charlie"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:David"), contextA));
        statements.add( vf.createStatement(vf.createIRI("urn:David"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Eve"), contextA));
        statements.add( vf.createStatement(vf.createIRI("urn:Eve"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Frank"), contextA));

        // Context B stat
        statements.add( vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob"), contextB));
        statements.add( vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Charlie"), contextB));
        statements.add( vf.createStatement(vf.createIRI("urn:Charlie"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:David"), contextB));

        final Sail sail = makeSail();

        try {
            // Write the statements to Rya.
            final SailRepository sailRepo = new SailRepository(sail);
            SailRepositoryConnection conn = sailRepo.getConnection();
            conn.add(statements);
            conn.commit();

            // Show the counts were incremented.
            assertEquals(5, client.getStatementCount().get().getStatementCount(ryaInstanceName, contextA));
            assertEquals(3, client.getStatementCount().get().getStatementCount(ryaInstanceName, contextB));

            // Drop all statements that are part of contextA.
            conn = sailRepo.getConnection();
            conn.clear(contextA);
            conn.commit();

            // Show the contextA's count was dropped to 0.
            assertEquals(0, client.getStatementCount().get().getStatementCount(ryaInstanceName, contextA));
            assertEquals(3, client.getStatementCount().get().getStatementCount(ryaInstanceName, contextB));

        } finally {
            sail.shutDown();
        }
    }
}