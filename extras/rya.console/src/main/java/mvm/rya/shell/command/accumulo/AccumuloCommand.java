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
package mvm.rya.shell.command.accumulo;

import static java.util.Objects.requireNonNull;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.instance.RyaDetails;
import mvm.rya.indexing.accumulo.ConfigUtils;

/**
 * An abstract class that holds onto Accumulo access information. Extend this
 * when implementing a command that interacts with Accumulo.
 */
@ParametersAreNonnullByDefault
public abstract class AccumuloCommand {

    private final AccumuloConnectionDetails connectionDetails;
    private final Connector connector;
    private final Authorizations auths;

    /**
     * Constructs an instance of {@link AccumuloCommand}.
     *
     * Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - Provides programatic access to the instance of Accumulo
     *   that hosts Rya instance. (not null)
     * @param auths - The authorizations that will be used when interacting with
     *   the instance of Accumulo. (not null)
     */
    public AccumuloCommand(
            final AccumuloConnectionDetails connectionDetails,
            final Connector connector,
            final Authorizations auths) {
        this.connectionDetails = requireNonNull( connectionDetails );
        this.connector = requireNonNull(connector);
        this.auths = requireNonNull(auths);
    }

    /**
     * @return Details about the values that were used to create the connector to the cluster. (not null)
     */
    public AccumuloConnectionDetails getAccumuloConnectionDetails() {
        return connectionDetails;
    }

    /**
     * @return Provides programatic access to the instance of Accumulo that hosts Rya instance.
     */
    public Connector getConnector() {
        return connector;
    }

    /**
     * @return The authorizations that will be used when interacting with
     *   the instance of Accumulo.
     */
    public Authorizations getAuths() {
        return auths;
    }
    
    /**
     * Builds a {@link AccumuloRdfConfiguration} object that will be used by the
     * Rya DAO to initialize all of the tables it will need.
     *
     * @param connectionDetails - Indicates how to connect to Accumulo. (not null)
     * @param details - Indicates what needs to be installed. (not null)
     * @return A Rya Configuration object that can be used to perform the install.
     */
    protected static AccumuloRdfConfiguration makeRyaConfig(final AccumuloConnectionDetails connectionDetails, final RyaDetails details) {
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

        // The Rya Instance Name is used as a prefix for the index tables in Accumulo.
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, details.getRyaInstanceName());

        // Enable the indexers that the instance is configured to use.
        conf.set(ConfigUtils.USE_PCJ, "" + details.getPCJIndexDetails().isEnabled() );
        conf.set(ConfigUtils.USE_GEO, "" + details.getGeoIndexDetails().isEnabled() );
        conf.set(ConfigUtils.USE_FREETEXT, "" + details.getFreeTextIndexDetails().isEnabled() );
        conf.set(ConfigUtils.USE_TEMPORAL, "" + details.getTemporalIndexDetails().isEnabled() );
        conf.set(ConfigUtils.USE_ENTITY, "" + details.getEntityCentricIndexDetails().isEnabled());

        // XXX The Accumulo implementation of the secondary indices make need all
        //     of the accumulo connector's parameters to initialize themselves, so
        //     we need to include them here. It would be nice if the secondary
        //     indexers used the connector that is provided to them instead of
        //     building a new one.
        conf.set(ConfigUtils.CLOUDBASE_USER, connectionDetails.getUsername());
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, new String(connectionDetails.getPassword()));
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, connectionDetails.getInstanceName());
        conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, connectionDetails.getZookeepers());

        // This initializes the living indexers that will be used by the application and
        // caches them within the configuration object so that they may be used later.
        ConfigUtils.setIndexers(conf);

        return conf;
    }

}