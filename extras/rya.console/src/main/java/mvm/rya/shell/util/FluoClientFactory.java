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
package mvm.rya.shell.util;

import static java.util.Objects.requireNonNull;

import javax.annotation.ParametersAreNonnullByDefault;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;

/**
 * TODO doc
 */
@ParametersAreNonnullByDefault
public class FluoClientFactory {

    /**
     * TODO doc
     *
     * @param username
     * @param password
     * @param instanceName
     * @param zookeeperHostnames
     * @param fluoAppName
     * @return
     */
    public FluoClient connect(
            final String username,
            final String password,
            final String instanceName,
            final String zookeeperHostnames,
            final String fluoAppName) {
        requireNonNull(username);
        requireNonNull(password);
        requireNonNull(instanceName);
        requireNonNull(zookeeperHostnames);
        requireNonNull(fluoAppName);

        final FluoConfiguration fluoConfig = new FluoConfiguration();

        // Fluo configuration values.
        fluoConfig.setApplicationName( fluoAppName );
        fluoConfig.setInstanceZookeepers( zookeeperHostnames + "/fluo" );

        // Accumulo Connection Stuff.
        fluoConfig.setAccumuloZookeepers( zookeeperHostnames );
        fluoConfig.setAccumuloInstance( instanceName );
        fluoConfig.setAccumuloUser( username );
        fluoConfig.setAccumuloPassword( password );

        // Connect the client.
        return FluoFactory.newClient(fluoConfig);
    }
}