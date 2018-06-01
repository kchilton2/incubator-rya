/**
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
package org.apache.rya.api.client.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.sail.config.RyaSailFactory;
import org.apache.rya.test.accumulo.AccumuloITBase;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.Test;

/**
 * Integration tests for the methods of {@link AccumuloGetStatementCount}.
 */
public class AccumuloGetStatementCountIT extends AccumuloITBase {

    private RyaClient getRyaClient() throws Exception {
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        return AccumuloRyaClientFactory.build(connectionDetails, getConnector());
    }

    /**
     * The repository must throw an exception when the Rya instance does not exist.
     */
    @Test(expected = InstanceDoesNotExistException.class)
    public void isEnabled_instanceDoesNotExist() throws Exception {
        getRyaClient().getStatementCount().get().isEnabled("not-an-instance");
    }

    /**
     * When Rya's details say the feature is enabled, then the interactor must also say it is enabled.
     */
    @Test
    public void isEnabled_ryaDetailsSayYes() throws Exception {
        // Install an instance of Rya.
        final String ryaInstanceName = getRyaInstanceName();
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setMaintainStatementCounts(true)
                .build();

        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final RyaClient client = AccumuloRyaClientFactory.build(connectionDetails, super.getConnector());
        client.getInstall().install(ryaInstanceName, installConfig);

        // Check to see if the feature is enabled using the interactor.
        assertTrue( client.getStatementCount().get().isEnabled(ryaInstanceName) );
    }

    /**
     * When Rya's details say the feature is disabled, then the interactor must also say it is disabled.
     */
    @Test
    public void isEnabled_ryaDetailsSayNo() throws Exception {
        // Install an instance of Rya.
        final String ryaInstanceName = getRyaInstanceName();
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setMaintainStatementCounts(false)
                .build();

        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final RyaClient client = AccumuloRyaClientFactory.build(connectionDetails, super.getConnector());
        client.getInstall().install(ryaInstanceName, installConfig);

        // Check to see if the feature is enabled using the interactor.
        assertFalse( client.getStatementCount().get().isEnabled(ryaInstanceName) );
    }

    /**
     * The repository must throw an exception when the Rya instance does not exist.
     */
    @Test(expected = InstanceDoesNotExistException.class)
    public void getStatementCount_instanceDoesNotExist() throws Exception {
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final RyaClient client = AccumuloRyaClientFactory.build(connectionDetails, super.getConnector());
        client.getStatementCount().get().isEnabled("bad_instance_name");
    }

    /**
     * 0 is returned when nothing has been stored for the context yet.
     */
    @Test
    public void getStatementCount_noValueStoredForContext() throws Exception {
        // Install an instance of Rya.
        final String ryaInstanceName = getRyaInstanceName();
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setMaintainStatementCounts(true)
                .build();

        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final RyaClient client = AccumuloRyaClientFactory.build(connectionDetails, super.getConnector());
        client.getInstall().install(ryaInstanceName, installConfig);

        // Fetch a count using the interactor.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final long count = client.getStatementCount().get().getStatementCount(ryaInstanceName, vf.createIRI("urn:contextA"));
        assertEquals(0, count);
    }

    /**
     * Ensure the correct count was created from a series of statements that were written to Rya.
     */
    @Test
    public void getStatementCount_valueStoredForContext() throws Exception {
        // Install an instance of Rya.
        final String ryaInstanceName = getRyaInstanceName();
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setMaintainStatementCounts(true)
                .build();

        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final RyaClient client = AccumuloRyaClientFactory.build(connectionDetails, super.getConnector());
        client.getInstall().install(ryaInstanceName, installConfig);

        // Insert a statement.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Resource contextA = vf.createIRI("urn:contextA");

        final AccumuloRdfConfiguration ryaConf = new AccumuloRdfConfiguration();
        ryaConf.setTablePrefix( getRyaInstanceName() );
        ryaConf.setAccumuloUser( getUsername() );
        ryaConf.setAccumuloPassword( getPassword() );
        ryaConf.setAccumuloInstance( getInstanceName() );
        ryaConf.setAccumuloZookeepers( getZookeepers() );

        final Sail sail = RyaSailFactory.getInstance(ryaConf);
        try {
            final Statement statement = vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Alice"), contextA);
            new SailRepository(sail).getConnection().add(statement);
        } finally {
            sail.shutDown();
        }

        // Verify the count for the statement's context was incremented.
        assertEquals(1, client.getStatementCount().get().getStatementCount(ryaInstanceName, contextA));
    }
}