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

import static java.util.Objects.requireNonNull;

import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.base.Optional;
import com.mongodb.MongoClient;

import mvm.rya.api.instance.RyaDetails;
import mvm.rya.api.instance.RyaDetailsRepository;
import mvm.rya.api.instance.RyaDetailsRepository.NotInitializedException;
import mvm.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import mvm.rya.mongodb.instance.MongoRyaInstanceDetailsRepository;
import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;
import mvm.rya.shell.command.administrative.GetInstanceDetails;
import mvm.rya.shell.command.administrative.InstanceExists;
import mvm.rya.shell.command.mongo.MongoCommand;
import mvm.rya.shell.command.mongo.MongoConnectionDetails;

/**
 * An Mongo implementation of the {@link GetInstanceDetails} command.
 */
@ParametersAreNonnullByDefault
public class MongoGetInstanceDetails extends MongoCommand implements GetInstanceDetails {

    private final InstanceExists instanceExists;

    /**
     * Constructs an instance of {@link MongoGetInstanceDetails}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param client - Provides programatic access to the instance of Mongo
     *   that hosts Rya instance. (not null)
     */
    public MongoGetInstanceDetails(final MongoConnectionDetails connectionDetails, final MongoClient client) {
        super(connectionDetails, client);
        instanceExists = new MongoInstanceExists(connectionDetails, client);
    }

    @Override
    public Optional<RyaDetails> getDetails(final String ryaInstanceName) throws InstanceDoesNotExistException, CommandException {
        requireNonNull(ryaInstanceName);

        // Ensure the Rya instance exists.
        if(!instanceExists.exists(ryaInstanceName)) {
            throw new InstanceDoesNotExistException(String.format("There is no Rya instance named '%s'.", ryaInstanceName));
        }

        // If the instance has details, then return them.
        final RyaDetailsRepository detailsRepo = new MongoRyaInstanceDetailsRepository(getClient(), ryaInstanceName);
        try {
            return Optional.of( detailsRepo.getRyaInstanceDetails() );
        } catch (final NotInitializedException e) {
            return Optional.absent();
        } catch (final RyaDetailsRepositoryException e) {
            throw new CommandException("Could not fetch the Rya instance's details.", e);
        }
    }
}