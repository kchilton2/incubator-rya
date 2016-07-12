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

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import de.flapdoodle.embed.mongo.tests.MongodForTestsFactory;
import mvm.rya.shell.command.mongo.MongoConnectionDetails;

/**
 * The base class for any integration test that needs to use a {@link MiniAccumuloCluster}.
 */
public class MongoITBase {
    private static MongoClient client;

    /**
     * Details about the values that were used to create the connector to mongo.
     */
    private static MongoConnectionDetails connectionDetails = null;

    @BeforeClass
    public static void startMongo() throws IOException, InterruptedException, MongoException {
        final MongodForTestsFactory factory = new MongodForTestsFactory();
        client = factory.newMongo();
        connectionDetails = new MongoConnectionDetails(
            "username",
            "pswd".toCharArray(),
            "rya",
            "localhost");
    }

    @Before
    public void cleanMongo() {
        for(final String db : client.getDatabaseNames()) {
            client.dropDatabase(db);
        }
    }

    @AfterClass
    public static void stopMongo() throws IOException, InterruptedException {
        client.close();
    }

    /**
     * @return An in-memory mongo client that can be used to test the commands against.
     */
    public MongoClient getTestClient() {
        return client;
    }

    /**
     * @return Details about the values that were used to create the connector to mongo.
     */
    public MongoConnectionDetails getConnectionDetails() {
        return connectionDetails;
    }
}