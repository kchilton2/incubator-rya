/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.mongodb;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.configuration.ConfigurationRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.annotation.ThreadSafe;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Mongo convention generally allows for a single instance of a {@link MongoClient}
 * throughout the life cycle of an application.  This MongoConnectorFactory lazy
 * loads a Mongo Client and uses the same one whenever {@link MongoConnectorFactory#getMongoClient(Configuration)}
 * is invoked.
 *
 * TODO talk about who uses this thing and when it is appropriate to not use it. also rename it as a singleton holder or something.
 *
 * TODO talk about how this is meant to make it so you can reuse a client through the lifecycle of an application.
 */
@ThreadSafe
@DefaultAnnotation(NonNull.class)
public final class MongoConnectorFactory {

    private static final ReentrantLock LOCK = new ReentrantLock();

    private static MongoClient mongoClient;

    /**
     * Connects a {@link MongoClient} using the provided {@link Configuration} if this object isn't
     * already holding a client. Returns the new or already existing client.
     *
     * @param conf - The {@link Configuration} that will be used to connect to a MongoDB Server if
     *   a new connection needs to be made. (not null)
     * @return The existing/new {@link MongoClient}.
     * @throws ConfigurationRuntimeException - Thrown if the configured port is invalid.
     * @throws MongoException  Couldn't connect to the MongoDB Server.
     */
    public static MongoClient getMongoClient(final Configuration conf) throws ConfigurationRuntimeException, MongoException {
        requireNonNull(conf);
        LOCK.lock();
        try {
            if(mongoClient == null) {
                mongoClient = createMongoClientForServer(conf);
            }
            return mongoClient;
        } finally {
            LOCK.unlock();
        }
    }

    /**
     * @return The held {@link MongoClient} if one has been created.
     */
    public static Optional<MongoClient> getMongoClient() {
        LOCK.lock();
        try {
            return Optional.ofNullable(mongoClient);
        } finally {
            LOCK.unlock();
        }
    }

    /**
     * Update the {@link MongoClient} that is held onto by this object. If one arleady exists, close
     * it before replacing it.
     *
     * @param mongoClient - The new client that will be held onto by this class. (not null)
     */
    public static void upsertMongoClient(final MongoClient mongoClient) {
        requireNonNull(mongoClient);
        LOCK.lock();
        try {
            closeMongoClient();
            MongoConnectorFactory.mongoClient = mongoClient;
        } finally {
            LOCK.unlock();
        }
    }

    /**
     * If this object is holding onto a {@link MongoClient}, then it is closed and let go.
     */
    public static void closeMongoClient() {
        LOCK.lock();
        try {
            if(mongoClient != null) {
                mongoClient.close();
                mongoClient = null;
            }
        } finally {
            LOCK.unlock();
        }
    }

    /**
     * Make a {@link MongoClient} that is connected to a specific Database if those configuration values are present.
     *
     * @param conf - Configuration containing connection parameters. (not null)
     * @throws ConfigurationRuntimeException - Thrown if the configured port is invalid.
     * @throws MongoException  Couldn't connect to the MongoDB Server.
     */
    private static MongoClient createMongoClientForServer(final Configuration conf) throws ConfigurationRuntimeException, MongoException {
        requireNonNull(conf);

        final MongoDBRdfConfiguration mongoConf = (conf instanceof MongoDBRdfConfiguration) ?
                (MongoDBRdfConfiguration) conf : new MongoDBRdfConfiguration(conf);

        // Set some options to speed up the timeouts if db server isn't available.
        final MongoClientOptions clientOptions = makeMongoClientOptions(conf);

        // Connect to a running MongoDB server.
        final int port;
        try {
            port = Integer.getInteger( mongoConf.getMongoPort() );
        } catch(final NumberFormatException e) {
            throw new ConfigurationRuntimeException("Port '" + mongoConf.getMongoPort() + "' must be an integer.");
        }

        final ServerAddress server = new ServerAddress(mongoConf.getMongoHostname(), port);

        // Connect to a specific MongoDB Database if that information is provided.
        final String username = mongoConf.getMongoUser();
        final String database = mongoConf.getRyaInstance();
        final String password = mongoConf.getMongoPassword();
        if(username != null && database != null && password != null) {
            final MongoCredential cred = MongoCredential.createCredential(username, database, password.toCharArray());
            return new MongoClient(server, Arrays.asList(cred), clientOptions);
        } else {
            return new MongoClient(server, clientOptions);
        }
    }

    /**
     * @param conf - Rya configutation. (not null)
     * @return MongoDB client options corresponding to the Rya Configuration.
     */
    private static MongoClientOptions makeMongoClientOptions(final Configuration conf) {
        requireNonNull(conf);

        final MongoClientOptions.Builder builder = MongoClientOptions.builder();

        // These are much quicker than defaults. Good for impatient humans.
        if (conf.getBoolean("timeoutFast", false)) {
            builder.connectTimeout(1000);
            builder.socketTimeout(1000);
            builder.serverSelectionTimeout(1000);
        }

        return builder.build();
    }
}