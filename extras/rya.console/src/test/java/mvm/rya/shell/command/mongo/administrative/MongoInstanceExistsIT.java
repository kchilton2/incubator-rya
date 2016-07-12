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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.client.TableExistsException;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import mvm.rya.mongodb.instance.MongoRyaInstanceDetailsRepository;
import mvm.rya.shell.MongoITBase;
import mvm.rya.shell.command.CommandException;

/**
 * Integration tests the methods of {@link MongoInstanceExists}.
 */
public class MongoInstanceExistsIT extends MongoITBase {

    @Test
    public void exists_ryaDetailsTable() throws MongoException, CommandException, TableExistsException {
        final MongoClient client = getTestClient();

        // Create the Rya instance's Rya details collection.
        final String instanceName = "test_instance_";
        client.getDB(instanceName).createCollection(MongoRyaInstanceDetailsRepository.INSTANCE_DETAILS_COLLECTION_NAME, new BasicDBObject());

        // Verify the command reports the instance exists.
        final MongoInstanceExists instanceExists = new MongoInstanceExists(getConnectionDetails(), client);
        assertTrue( instanceExists.exists(instanceName) );
    }

    @Test
    public void exists_dataTables() throws MongoException, CommandException, TableExistsException {
        final MongoClient client = getTestClient();

        // Create the Rya instance's Rya triples collection.
        final String instanceName = "test_instance_";
        client.getDB(instanceName).createCollection("rya_triples", new BasicDBObject());

        // Verify the command reports the instance exists.
        final MongoInstanceExists instanceExists = new MongoInstanceExists(getConnectionDetails(), client);
        assertTrue( instanceExists.exists(instanceName) );
    }

    @Test
    public void doesNotExist() throws CommandException, MongoException {
        // Verify the command reports the instance does not exists.
        final MongoInstanceExists instanceExists = new MongoInstanceExists(getConnectionDetails(), getTestClient());
        assertFalse( instanceExists.exists("some_instance") );
    }
}