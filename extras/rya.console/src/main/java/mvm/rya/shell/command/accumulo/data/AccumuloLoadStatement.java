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
package mvm.rya.shell.command.accumulo.data;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.openrdf.model.Statement;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.instance.RyaDetailsRepository;
import mvm.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;
import mvm.rya.shell.command.accumulo.AccumuloCommand;
import mvm.rya.shell.command.accumulo.AccumuloConnectionDetails;
import mvm.rya.shell.command.data.LoadStatement;

/**
 * An Accumulo implementation of the {@link LoadStatement} command.
 */
@ParametersAreNonnullByDefault
public class AccumuloLoadStatement extends AccumuloCommand implements LoadStatement {

    /**
     * Constructs an instance of {@link AccumuloLoadStatement}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - Provides programatic access to the instance of Accumulo
     *   that hosts Rya instance. (not null)
     * @param auths - The authorizations that will be used when interacting with
     *   the instance of Accumulo. (not null)
     */
    public AccumuloLoadStatement(final AccumuloConnectionDetails connectionDetails, final Connector connector, final Authorizations auths) {
        super(connectionDetails, connector, auths);
    }

    @Override
    public void loadStatement(final String instanceName, final Statement statement, final String columnVisibilities) throws InstanceDoesNotExistException, CommandException {
        Connector conn = getConnector();
        RyaDetailsRepository detailsRepo = new AccumuloRyaInstanceDetailsRepository(conn, instanceName);
        AccumuloRdfConfiguration ryaConfig;
		try {
			ryaConfig = makeRyaConfig(getAccumuloConnectionDetails(), detailsRepo.getRyaInstanceDetails());
	        final AccumuloRyaDAO ryaDao = new AccumuloRyaDAO();
	        ryaDao.setConf( ryaConfig );
	        ryaDao.setConnector(conn);
	        ryaDao.init();
	        RyaStatement ryaStatement = RdfToRyaConversions.convertStatement(statement);
	        // set the visibilities
	        ryaStatement.setColumnVisibility(columnVisibilities.getBytes());
	        ryaDao.add(ryaStatement);
	        ryaDao.flush();
		} catch (RyaDetailsRepositoryException | RyaDAOException e) {
			throw new CommandException(e);
		}

    }
}