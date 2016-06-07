package mvm.rya.shell.command.accumulo.administrative;

/*
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

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;

import mvm.rya.api.instance.RyaDetails;
import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;
import mvm.rya.shell.command.accumulo.AccumuloCommand;
import mvm.rya.shell.command.administrative.GetInstanceDetails;

// TODO impl, test

/**
 * An Accumulo implementation of the {@link GetInstanceDetails} command.
 */
@ParametersAreNonnullByDefault
public class AccumuloGetInstanceDetails extends AccumuloCommand implements GetInstanceDetails {

    /**
     * Constructs an instance of {@link AccumuloGetInstanceDetails}.
     *
     * @param connector - Provides programatic access to the instance of Accumulo
     *   that hosts Rya instance. (not null)
     * @param auths - The authorizations that will be used when interacting with
     *   the instance of Accumulo. (not null)
     */
    public AccumuloGetInstanceDetails(final Connector connector, final Authorizations auths) {
        super(connector, auths);
    }

    @Override
    public RyaDetails getDetails(final String instanceName) throws InstanceDoesNotExistException, CommandException {
        // TODO Auto-generated method stub

        // NOTE: Just return what the RyaDetailsRepository is storing.

        return null;
    }
}