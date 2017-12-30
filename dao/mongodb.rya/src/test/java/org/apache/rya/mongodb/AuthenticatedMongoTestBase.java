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
package org.apache.rya.mongodb;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.bson.Document;
import org.junit.Before;
import org.junit.BeforeClass;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;

/**
 * A base class that may be used when implementing Mongo DB tests that use the
 * JUnit framework.
 */
public abstract class AuthenticatedMongoTestBase {
    private static EmbeddedMongoFactory FACTORY;
    private MongoClient mongoClient = null;

    /**
     * The Stateful config here will contain an admin client to mongo.
     */
    protected StatefulMongoDBRdfConfiguration conf;

    @BeforeClass
    public static void createFactory() throws Exception {
        FACTORY = EmbeddedMongoFactory.newFactory(true);
    }

    @Before
    public void setupTest() throws Exception {
        // Setup the configuration that will be used within the test.
        final MongoDBRdfConfiguration conf = new MongoDBRdfConfiguration( new Configuration() );
        conf.setBoolean("sc.useMongo", true);
        conf.setTablePrefix("test_");
        conf.setMongoDBName("testDB");
        conf.setMongoHostname(FACTORY.getMongoServerDetails().net().getServerAddress().getHostAddress());
        conf.setMongoPort(Integer.toString(FACTORY.getMongoServerDetails().net().getPort()));

        // Let tests update the configuration.
        updateConfiguration(conf);

        updateMongoUsers();

        mongoClient = FACTORY.newMongoClient(EmbeddedMongoFactory.DEFAULT_ADMIN_USER, EmbeddedMongoFactory.DEFAULT_ADMIN_PSWD);
        final List<MongoSecondaryIndex> indexers = conf.getInstances("ac.additional.indexers", MongoSecondaryIndex.class);
        this.conf = new StatefulMongoDBRdfConfiguration(conf, mongoClient, indexers);
    }

    protected MongoClient createAuthClient(final String username, final char[] password) throws Exception {
        final MongoClient authClient = FACTORY.newMongoClient(username, password);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                authClient.close();
            }
        });
        return authClient;
    }

    /**
     * Override this method if you would like to augment the configuration object that
     * will be used to initialize indexers and create the mongo client prior to running a test.
     *
     * @param conf - The configuration object that may be updated. (not null)
     */
    protected void updateConfiguration(final MongoDBRdfConfiguration conf) {
        // By default, do nothing.
    }

    protected abstract void updateMongoUsers() throws Exception;

    protected void addAdminUser(final String username, final char[] password) throws Exception {
        FACTORY.addAdmin(username, password);
    }

    protected void addDBUser(final String username, final char[] password, final String mongoDBName, final String adminUsername, final char[] adminPassword) throws IOException {
        FACTORY.addDBUser(username, password, mongoDBName, adminUsername, adminPassword);
    }

    protected void removeUser(final String username) throws Exception {
        FACTORY.removeUser(username);
    }

    protected void removeAllUsers(final String adminUsername, final char[] adminPassword) throws Exception {
        FACTORY.removeAllUsers(adminUsername, adminPassword);
    }

    /**
     * @return A {@link MongoClient} that is connected to the embedded instance of Mongo DB.
     */
    public MongoClient getAdminMongoClient() {
        return mongoClient;
    }

    /**
     * @return The Rya triples {@link MongoCollection}.
     */
    public MongoCollection<Document> getRyaCollectionAsAdmin() {
        return mongoClient.getDatabase(conf.getMongoDBName()).getCollection(conf.getTriplesCollectionName());
    }

    /**
     * @return The Rya triples {@link DBCollection}.
     */
    public DBCollection getRyaDbCollectionAsAdmin() {
        return mongoClient.getDB(conf.getMongoDBName()).getCollection(conf.getTriplesCollectionName());
    }
}