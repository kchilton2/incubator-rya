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
package org.apache.rya.api.client.accumulo;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.rya.accumulo.statistics.AccumuloStatementCountsRepository;
import org.apache.rya.api.client.Install.DuplicateInstanceNameException;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetailsRepository.NotInitializedException;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.statistics.StatementCountsRepository;
import org.apache.rya.test.accumulo.AccumuloITBase;
import org.junit.Test;

/**
 * Integration tests the methods of {@link AccumuloInstall}.
 */
public class AccumuloInstallIT extends AccumuloITBase {

    @Test
    public void install() throws AccumuloException, AccumuloSecurityException, DuplicateInstanceNameException, RyaClientException, NotInitializedException, RyaDetailsRepositoryException {
        // Install an instance of Rya.
        final String instanceName = getRyaInstanceName();
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
        		getUsername(),
        		getPassword().toCharArray(),
        		getInstanceName(),
        		getZookeepers());

        final RyaClient ryaClient = AccumuloRyaClientFactory.build(connectionDetails, getConnector());

        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(false)
                .setEnableEntityCentricIndex(false)
                .setEnableFreeTextIndex(false)
                .setEnableTemporalIndex(false)
                .setEnablePcjIndex(false)
                .setEnableGeoIndex(false)
                .build();


        ryaClient.getInstall().install(instanceName, installConfig);

        // Check that the instance exists.
        assertTrue( ryaClient.getInstanceExists().exists(instanceName) );
    }

    @Test
    public void install_withIndexers() throws AccumuloException, AccumuloSecurityException, DuplicateInstanceNameException, RyaClientException, NotInitializedException, RyaDetailsRepositoryException {
        // Install an instance of Rya.
        final String instanceName = getRyaInstanceName();
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
        		getUsername(),
        		getPassword().toCharArray(),
        		getInstanceName(),
        		getZookeepers());

        final RyaClient ryaClient = AccumuloRyaClientFactory.build(connectionDetails, getConnector());

        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(true)
                .setEnableEntityCentricIndex(true)
                .setEnableFreeTextIndex(true)
                .setEnableTemporalIndex(true)
                .setEnablePcjIndex(true)
                .setEnableGeoIndex(true)
                .build();

        ryaClient.getInstall().install(instanceName, installConfig);

        // Check that the instance exists.
        assertTrue( ryaClient.getInstanceExists().exists(instanceName) );
    }

    @Test(expected = DuplicateInstanceNameException.class)
    public void install_alreadyExists() throws DuplicateInstanceNameException, RyaClientException, AccumuloException, AccumuloSecurityException {
        // Install an instance of Rya.
        final String instanceName = getRyaInstanceName();
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
        		getUsername(),
        		getPassword().toCharArray(),
        		getInstanceName(),
        		getZookeepers());

        final RyaClient ryaClient = AccumuloRyaClientFactory.build(connectionDetails, getConnector());

        final InstallConfiguration installConfig = InstallConfiguration.builder().build();
        ryaClient.getInstall().install(instanceName, installConfig);

        // Install it again.
        ryaClient.getInstall().install(instanceName, installConfig);
    }

    @Test
    public void install_withStatementCountMaintenance() throws Exception {
        // Install an instance of Rya.
        final String ryaInstanceName = getRyaInstanceName();
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final RyaClient ryaClient = AccumuloRyaClientFactory.build(connectionDetails, getConnector());

        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(false)
                .setMaintainStatementCounts(true)
                .setEnableEntityCentricIndex(false)
                .setEnableFreeTextIndex(false)
                .setEnableTemporalIndex(false)
                .setEnablePcjIndex(false)
                .setEnableGeoIndex(false)
                .build();

        // Show the statement counts repository is not installed.
        final StatementCountsRepository stmtCountsRepo =
                AccumuloStatementCountsRepository.makeReadOnly(super.getConnector(), ryaInstanceName);
        assertFalse(stmtCountsRepo.isInstalled());

        // Perform the install.
        ryaClient.getInstall().install(ryaInstanceName, installConfig);

        // Show the statement counts repository is installed.
        assertTrue(stmtCountsRepo.isInstalled());
    }

    @Test
    public void install_withoutStatementCountMaintenance() throws Exception {
        // Install an instance of Rya.
        final String ryaInstanceName = getRyaInstanceName();
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final RyaClient ryaClient = AccumuloRyaClientFactory.build(connectionDetails, getConnector());

        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(false)
                .setMaintainStatementCounts(false)
                .setEnableEntityCentricIndex(false)
                .setEnableFreeTextIndex(false)
                .setEnableTemporalIndex(false)
                .setEnablePcjIndex(false)
                .setEnableGeoIndex(false)
                .build();

        // Show the statement counts repository is not installed.
        final StatementCountsRepository stmtCountsRepo =
                AccumuloStatementCountsRepository.makeReadOnly(super.getConnector(), ryaInstanceName);
        assertFalse(stmtCountsRepo.isInstalled());

        // Perform the install.
        ryaClient.getInstall().install(ryaInstanceName, installConfig);

        // Show the statement counts repository is installed.
        assertFalse(stmtCountsRepo.isInstalled());
    }
}