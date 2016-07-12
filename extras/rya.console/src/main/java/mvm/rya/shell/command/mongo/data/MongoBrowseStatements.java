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
package mvm.rya.shell.command.mongo.data;

import java.util.Iterator;

import javax.annotation.ParametersAreNonnullByDefault;

import org.openrdf.model.Statement;

import com.mongodb.MongoClient;

import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;
import mvm.rya.shell.command.data.BrowseStatements;
import mvm.rya.shell.command.mongo.MongoCommand;
import mvm.rya.shell.command.mongo.MongoConnectionDetails;

// TODO test, impl

/**
 * An Mongo implementation of the {@link BrowseStatements} command.
 */
@ParametersAreNonnullByDefault
public class MongoBrowseStatements extends MongoCommand implements BrowseStatements {

    /**
     * Constructs an instance of {@link MongoBrowseStatements}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param client - Provides programatic access to the instance of Mongo
     *   that hosts Rya instance. (not null)
     */
    public MongoBrowseStatements(final MongoConnectionDetails connectionDetails,final MongoClient client) {
        super(connectionDetails, client);
    }

    @Override
    public Iterator<Statement> browse(final String instanceName) throws InstanceDoesNotExistException, CommandException {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}