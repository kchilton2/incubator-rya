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

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.test.accumulo.AccumuloITBase;
import org.apache.rya.api.client.ListInstances;
import org.apache.rya.api.client.RyaClientException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Integration tests the methods of {@link AccumuloListInstances}.
 */
public class AccumuloListInstancesIT extends AccumuloITBase {

    @Before
    public void setup() throws Exception {
        // this is a bit of a hack to clear any existing instances before
        // adding the instances we want to exist for testing the list command
        final TableOperations tableOps = getConnector().tableOperations();
        final SecurityOperations secOps = getConnector().securityOperations();
        secOps.grantSystemPermission("root", SystemPermission.DROP_TABLE);

        for (final String tableName : getConnector().tableOperations().list()) {
            if (!tableName.startsWith("accumulo.")) {
                tableOps.delete(tableName);
            }
        }
    }

    @Test
    public void listInstances_hasRyaDetailsTable() throws AccumuloException, AccumuloSecurityException, RyaClientException {
        // Install a few instances of Rya using the install command.
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final Install install = new AccumuloInstall(connectionDetails, getConnector());
        install.install("instance1_", InstallConfiguration.builder().build());
        install.install("instance2_", InstallConfiguration.builder().build());
        install.install("instance3_", InstallConfiguration.builder().build());

        // Fetch the list and verify it matches what is expected.
        final ListInstances listInstances = new AccumuloListInstances(connectionDetails, getConnector());
        final List<String> instances = listInstances.listInstances();
        Collections.sort(instances);

        final List<String> expected = Lists.newArrayList("instance1_", "instance2_", "instance3_");
        assertEquals(expected, instances);
    }
}