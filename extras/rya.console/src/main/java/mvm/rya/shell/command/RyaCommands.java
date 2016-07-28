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
package mvm.rya.shell.command;

import static java.util.Objects.requireNonNull;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;

import org.apache.accumulo.core.client.Connector;

import com.google.common.base.Optional;

import mvm.rya.shell.command.accumulo.AccumuloConnectionDetails;
import mvm.rya.shell.command.accumulo.administrative.AccumuloCreatePCJ;
import mvm.rya.shell.command.accumulo.administrative.AccumuloDeletePCJ;
import mvm.rya.shell.command.accumulo.administrative.AccumuloGetInstanceDetails;
import mvm.rya.shell.command.accumulo.administrative.AccumuloInstall;
import mvm.rya.shell.command.accumulo.administrative.AccumuloInstanceExists;
import mvm.rya.shell.command.accumulo.administrative.AccumuloListInstances;
import mvm.rya.shell.command.accumulo.administrative.AccumuloUninstall;
import mvm.rya.shell.command.administrative.CreatePCJ;
import mvm.rya.shell.command.administrative.DeletePCJ;
import mvm.rya.shell.command.administrative.GetInstanceDetails;
import mvm.rya.shell.command.administrative.Install;
import mvm.rya.shell.command.administrative.InstanceExists;
import mvm.rya.shell.command.administrative.ListInstances;
import mvm.rya.shell.command.administrative.Uninstall;

/**
 * Provides access to a set of initialized Rya commands.
 */
@Immutable
@ParametersAreNonnullByDefault
public class RyaCommands {
    // Administrative commands that may be invoked. These are initialized whenever a store is connected to.
    private final Install install;
    private final Optional<CreatePCJ> createPcj;
    private final Optional<DeletePCJ> deletePcj;
    private final GetInstanceDetails getInstanceDetails;
    private final InstanceExists instanceExists;
    private final ListInstances listInstances;
    private final Uninstall uninstall;

    /**
     * To construct an instance of this class, use one of the factory methods.
     */
    private RyaCommands(
            final Install install,
            final Optional<CreatePCJ> createPcj,
            final Optional<DeletePCJ> deletePcj,
            final GetInstanceDetails getInstanceDetails,
            final InstanceExists instanceExists,
            final ListInstances listInstances,
            final Uninstall uninstall) {
        this.install = requireNonNull(install);
        this.createPcj = requireNonNull(createPcj);
        this.deletePcj = requireNonNull(deletePcj);
        this.getInstanceDetails = requireNonNull(getInstanceDetails);
        this.instanceExists = requireNonNull(instanceExists);
        this.listInstances = requireNonNull(listInstances);
        this.uninstall = requireNonNull(uninstall);
    }

    /**
     * @return An instance of {@link Install} that is connected to a Rya storage.
     */
    public Install getInstall() {
        return install;
    }

    /**
     * @return An instance of {@link CreatePCJ} that is connected to a Rya storage
     *   if the Rya instance supports PCJ indexing.
     */
    public Optional<CreatePCJ> getCreatePCJ() {
        return createPcj;
    }

    /**
     * @return An instance of {@link DeletePCJ} that is connected to a Rya storage
     *   if the Rya instance supports PCJ indexing.
     */
    public Optional<DeletePCJ> getDeletePCJ() {
        return deletePcj;
    }

    /**
     * @return An instance of {@link GetInstanceDetails} that is connected to a Rya storage.
     */
    public GetInstanceDetails getGetInstanceDetails() {
        return getInstanceDetails;
    }

    /**
     * @return An instance of {@link ListInstances} that is connected to a Rya storage.
     */
    public ListInstances getListInstances() {
        return listInstances;
    }

    /**
     * @return An instance of {@link InstanceExists} that is connected to a Rya storage.
     */
    public InstanceExists getInstanceExists() {
        return instanceExists;
    }

    /**
     * @return An instance of {@link Uninstall} that is connected to a Rya storage.
     */
    public Uninstall getUninstall() {
        return uninstall;
    }

    /**
     * Initialize a set of {@link RyaCommands} that will interact with an instance of Accumulo.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - The Accumulo connector the commands will use. (not null)
     * @return The initialized commands.
     */
    public static RyaCommands buildAccumuloCommands(
            final AccumuloConnectionDetails connectionDetails,
            final Connector connector) {
        requireNonNull(connectionDetails);
        requireNonNull(connector);

        // Build the RyaCommands option with the initialized commands.
        return new RyaCommands(
                new AccumuloInstall(connectionDetails, connector),
                Optional.<CreatePCJ>of( new AccumuloCreatePCJ(connectionDetails, connector) ),
                Optional.<DeletePCJ>of( new AccumuloDeletePCJ(connectionDetails, connector) ),
                new AccumuloGetInstanceDetails(connectionDetails, connector),
                new AccumuloInstanceExists(connectionDetails, connector),
                new AccumuloListInstances(connectionDetails, connector),
                new AccumuloUninstall(connectionDetails, connector));
    }
}