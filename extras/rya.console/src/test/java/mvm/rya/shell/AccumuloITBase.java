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
package mvm.rya.shell;

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ClientCnxn;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.google.common.io.Files;

import mvm.rya.shell.command.accumulo.AccumuloConnectionDetails;

/**
 * The base class for any integration test that needs to use a {@link MiniAccumuloCluster}.
 */
public class AccumuloITBase {
    private static final Logger log = Logger.getLogger(AccumuloITBase.class);

    private static final String ACCUMULO_USERNAME = "root";
    private static final String ACCUMULO_PASSWORD = "password";

    private MiniAccumuloCluster cluster = null;

    @BeforeClass
    public static void killLoudLogs() {
        Logger.getLogger(ClientCnxn.class).setLevel(Level.ERROR);
    }

    @Before
    public void setupTest() throws IOException, InterruptedException {
        final MiniAccumuloConfig cfg = new MiniAccumuloConfig(Files.createTempDir(), ACCUMULO_PASSWORD);
        cluster = new MiniAccumuloCluster(cfg);
        cluster.start();
    }

    @After
    public void cleanup() {
        if(cluster != null) {
            try {
                log.info("Shutting down MiniAccumuloCluster.");
                cluster.stop();
            } catch (IOException | InterruptedException e) {
                log.error("Problem while stopping MiniAccumuloCluster.", e);
            }
        }
    }

    public AccumuloConnectionDetails getConnectionDetails() {
        return new AccumuloConnectionDetails(
                ACCUMULO_USERNAME,
                ACCUMULO_PASSWORD.toCharArray(),
                cluster.getInstanceName(),
                cluster.getZooKeepers());
    }

    public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
        return cluster.getConnector(ACCUMULO_USERNAME, ACCUMULO_PASSWORD);
    }



//    /**
//     * TODO doc
//     */
//    private MiniAccumuloClusterInstance cluster = null;
//
//    /**
//     * Details about the values that were used to create the connector to the cluster.
//     */
//    private AccumuloConnectionDetails connectionDetails = null;
//
//    @BeforeClass
//    public static void killLoudLogs() {
//        Logger.getLogger(ClientCnxn.class).setLevel(Level.ERROR);
//    }
//
//    @Before
//    public void startMiniAccumulo() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
//        cluster = new MiniAccumuloClusterInstance();
//        cluster.startMiniAccumulo();
//
//        connectionDetails = new AccumuloConnectionDetails(
//                cluster.getUsername(),
//                cluster.getPassword().toCharArray(),
//                cluster.getInstanceName(),
//                cluster.getZookeepers());
//    }
//
//    @After
//    public void stopMiniAccumulo() throws IOException, InterruptedException {
//        cluster.stopMiniAccumulo();
//    }
//
//    /**
//     * @return A mini Accumulo cluster that can be used to test the commands against.
//     */
//    public MiniAccumuloCluster getTestCluster() {
//        return cluster.getCluster();
//    }
//
//    /**
//     * @return An accumulo connector that is connected to the mini cluster.
//     */
//    public Connector getConnector() throws AccumuloException, AccumuloSecurityException {
//        return cluster.getConnector();
//    }
//
//    /**
//     * @return Details about the values that were used to create the connector to the cluster.
//     */
//    public AccumuloConnectionDetails getConnectionDetails() {
//        return connectionDetails;
//    }
}