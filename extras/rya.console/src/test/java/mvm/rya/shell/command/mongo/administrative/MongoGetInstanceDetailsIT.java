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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Date;

import org.apache.accumulo.core.client.TableExistsException;
import org.junit.Test;

import com.google.common.base.Optional;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;

import mvm.rya.api.instance.RyaDetails;
import mvm.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import mvm.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import mvm.rya.api.instance.RyaDetails.GeoIndexDetails;
import mvm.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails;
import mvm.rya.api.instance.RyaDetails.ProspectorDetails;
import mvm.rya.api.instance.RyaDetails.TemporalIndexDetails;
import mvm.rya.shell.MongoITBase;
import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;
import mvm.rya.shell.command.administrative.GetInstanceDetails;
import mvm.rya.shell.command.administrative.Install;
import mvm.rya.shell.command.administrative.Install.DuplicateInstanceNameException;
import mvm.rya.shell.command.administrative.Install.InstallConfiguration;

/**
 * Tests the methods of {@link MongoGetInstanceDetails}.
 */
public class MongoGetInstanceDetailsIT extends MongoITBase {

    @Test
    public void getDetails() throws MongoException, DuplicateInstanceNameException, CommandException {
        final String instanceName = "instance";
        // Install an instance of Rya.
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(true)
                .setEnableEntityCentricIndex(true)
                .setEnableFreeTextIndex(true)
                .setEnableTemporalIndex(true)
                .setEnableGeoIndex(true)
                .build();

        final Install install = new MongoInstall(getConnectionDetails(), getTestClient());
        install.install(instanceName, installConfig);

        // Verify the correct details were persisted.
        final GetInstanceDetails getInstanceDetails = new MongoGetInstanceDetails(getConnectionDetails(), getTestClient());
        final Optional<RyaDetails> details = getInstanceDetails.getDetails(instanceName);

        final RyaDetails expectedDetails = RyaDetails.builder()
                .setRyaInstanceName(instanceName)

                // The version depends on how the test is packaged, so just grab whatever was stored.
                .setRyaVersion( details.get().getRyaVersion() )

                .setGeoIndexDetails( new GeoIndexDetails(true) )
                .setTemporalIndexDetails(new TemporalIndexDetails(true) )
                .setFreeTextDetails( new FreeTextIndexDetails(true) )
                .setEntityCentricIndexDetails( new EntityCentricIndexDetails(false) )
                .setPCJIndexDetails(
                        PCJIndexDetails.builder()
                            .setEnabled(false))
                .setProspectorDetails( new ProspectorDetails(Optional.<Date>absent()) )
                .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.<Date>absent()) )
                .build();

        assertEquals(expectedDetails, details.get());
    }

    @Test(expected = InstanceDoesNotExistException.class)
    public void getDetails_instanceDoesNotExist() throws MongoException, InstanceDoesNotExistException, CommandException {
        final GetInstanceDetails getInstanceDetails = new MongoGetInstanceDetails(getConnectionDetails(), getTestClient());
        getInstanceDetails.getDetails("instance_name");
    }

    @Test
    public void getDetails_instanceDoesNotHaveDetails() throws MongoException, InstanceDoesNotExistException, CommandException, TableExistsException {
        // Mimic a pre-details rya install.
        final String instanceName = "instance_name";

        getTestClient().getDB(instanceName).createCollection("rya_triples", new BasicDBObject());

        // Verify that the operation returns empty.
        final GetInstanceDetails getInstanceDetails = new MongoGetInstanceDetails(getConnectionDetails(), getTestClient());
        final Optional<RyaDetails> details = getInstanceDetails.getDetails(instanceName);
        assertFalse( details.isPresent() );
    }
}