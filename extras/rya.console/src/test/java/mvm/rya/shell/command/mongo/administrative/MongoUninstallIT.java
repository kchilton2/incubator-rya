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
package mvm.rya.shell.command.mongo.administrative;

import org.junit.Assert;
import org.junit.Test;

import com.mongodb.MongoException;

import mvm.rya.api.instance.RyaDetailsRepository.NotInitializedException;
import mvm.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import mvm.rya.shell.MongoITBase;
import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;
import mvm.rya.shell.command.administrative.Install;
import mvm.rya.shell.command.administrative.Install.DuplicateInstanceNameException;
import mvm.rya.shell.command.administrative.Install.InstallConfiguration;
import mvm.rya.shell.command.administrative.InstanceExists;
import mvm.rya.shell.command.administrative.Uninstall;

/**
 * Integration tests the methods of {@link MongoInstall}.
 */
public class MongoUninstallIT extends MongoITBase {

    @Test
    public void uninstall() throws MongoException, DuplicateInstanceNameException, CommandException, NotInitializedException, RyaDetailsRepositoryException {
        // Install an instance of Rya.
        final String instanceName = "testInstance_";
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(true)
                .setEnableEntityCentricIndex(true)
                .setEnableFreeTextIndex(true)
                .setEnableTemporalIndex(true)
                .setEnablePcjIndex(true)
                .setEnableGeoIndex(true)
                .setFluoPcjAppName("fluo_app_name")
                .build();

        final Install install = new MongoInstall(getConnectionDetails(), getTestClient());
        install.install(instanceName, installConfig);

        // Uninstall the instance
        final Uninstall uninstall = new MongoUninstall(getConnectionDetails(), getTestClient());
        uninstall.uninstall(instanceName);

        // Check that the instance no longer exists.
        final InstanceExists instanceExists = new MongoInstanceExists(getConnectionDetails(), getTestClient());
        Assert.assertFalse(instanceExists.exists(instanceName));
    }

    @Test(expected = InstanceDoesNotExistException.class)
    public void uninstall_instanceDoesNotExists() throws InstanceDoesNotExistException, CommandException, MongoException {
        // Install an instance of Rya.
        final String instanceName = "testInstance_";

        // Uninstall the instance
        final Uninstall uninstall = new MongoUninstall(getConnectionDetails(), getTestClient());
        uninstall.uninstall(instanceName);
    }
}