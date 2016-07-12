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

import javax.annotation.ParametersAreNonnullByDefault;

import com.mongodb.MongoClient;

import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;
import mvm.rya.shell.command.administrative.DeletePCJ;
import mvm.rya.shell.command.mongo.MongoCommand;
import mvm.rya.shell.command.mongo.MongoConnectionDetails;

/**
 * An Mongo implementation of the {@link DeletePCJ} command.
 */
@ParametersAreNonnullByDefault
public class MongoDeletePCJ extends MongoCommand implements DeletePCJ {

    /**
     * Constructs an instance of {@link MongoDeletePCJ}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param client - Provides programatic access to the instance of Mongo
     *   that hosts Rya instance. (not null)
     */
    public MongoDeletePCJ(final MongoConnectionDetails connectionDetails, final MongoClient client) {
        super(connectionDetails, client);
    }

    @Override
    public void deletePCJ(final String instanceName, final String pcjId) throws InstanceDoesNotExistException, CommandException {
        throw new UnsupportedOperationException("Mongo does not support PCJ Indexing.");
    }
}