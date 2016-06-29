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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.UnsupportedRDFormatException;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.instance.RyaDetailsRepository;
import mvm.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.rdftriplestore.RdfCloudTripleStore;
import mvm.rya.shell.command.CommandException;
import mvm.rya.shell.command.InstanceDoesNotExistException;
import mvm.rya.shell.command.accumulo.AccumuloCommand;
import mvm.rya.shell.command.accumulo.AccumuloConnectionDetails;
import mvm.rya.shell.command.data.LoadStatementsFile;

//TODO impl, test

/**
 * An Accumulo implementation of the {@link LoadStatementsFile} command.
 */
@ParametersAreNonnullByDefault
public class AccumuloLoadStatementsFile extends AccumuloCommand implements LoadStatementsFile {

    /**
     * Constructs an instance of {@link AccumuloLoadStatementsFile}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - Provides programatic access to the instance of Accumulo
     *   that hosts Rya instance. (not null)
     * @param auths - The authorizations that will be used when interacting with
     *   the instance of Accumulo. (not null)
     */
    public AccumuloLoadStatementsFile(final AccumuloConnectionDetails connectionDetails,final Connector connector, final Authorizations auths) {
        super(connectionDetails, connector, auths);
    }

    @Override
    public void load(final String instanceName, final Path statementsFile)
            throws InstanceDoesNotExistException, CommandException {
        // get an input stream   	

 		try {
	        Connector connector = getConnector();
	        RyaDetailsRepository detailsRepo = new AccumuloRyaInstanceDetailsRepository(connector, instanceName);
	        AccumuloRyaDAO dao = new AccumuloRyaDAO();
	        dao.setConnector(connector);
	        RdfCloudTripleStore store = new RdfCloudTripleStore();
			AccumuloRdfConfiguration conf = makeRyaConfig(getAccumuloConnectionDetails(), detailsRepo.getRyaInstanceDetails());
	        dao.setConf(conf);
	        dao.init();
	        store.setRyaDAO(dao);
	        
	        // get the rdf statements
	        
	        RDFFormat format = Rio.getParserFormatForFileName(statementsFile.getFileName().toFile().getName(), RDFFormat.NTRIPLES);
	        RDFParser parser = Rio.createParser(format);
	        InputStream in = Files.newInputStream(statementsFile);
	        StatementHandler handler = new StatementHandler();
	        parser.setRDFHandler(handler);
	        parser.parse(in, "");
	        Set<Statement> statements = handler.getStatements();
	        Set<RyaStatement> ryaStatements = new HashSet<RyaStatement>();
	        for (Statement statement : statements){
	        	// TODO not adding visibilities
	        	ryaStatements.add(RdfToRyaConversions.convertStatement(statement));
	        }
	        dao.add(ryaStatements.iterator());
	        dao.flush();
		} catch (RyaDetailsRepositoryException | RyaDAOException |
				UnsupportedRDFormatException | IOException | RDFParseException | RDFHandlerException e) {
			throw new CommandException(e);
		} 
      }

}