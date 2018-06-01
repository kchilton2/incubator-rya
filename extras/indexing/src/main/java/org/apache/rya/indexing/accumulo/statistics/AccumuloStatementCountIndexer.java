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
package org.apache.rya.indexing.accumulo.statistics;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.experimental.AccumuloIndexer;
import org.apache.rya.accumulo.statistics.AccumuloStatementCountsRepository;
import org.apache.rya.accumulo.statistics.BatchWriterSupplier;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.index.RyaSecondaryIndexer;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.api.statistics.StatementCountsRepository;
import org.apache.rya.api.statistics.StatementCountsRepository.Delta;
import org.apache.rya.api.statistics.StatementCountsRepository.StatementCountsException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link RyaSecondaryIndexer} that glues statement by count maintenance into the secondary indexing flow
 * of {@link AccumuloRyaDAO}. This indexer will flush the {@link BarchWriter} it provides to the
 * {@link StatementCountsRepository} only when the {@link MultiTableBatchWriter} the DAO uses does so.
 */
@DefaultAnnotation(NonNull.class)
public class AccumuloStatementCountIndexer implements AccumuloIndexer {
    private static final Logger log = LoggerFactory.getLogger(AccumuloStatementCountIndexer.class);

    /**
     * Make the Accumulo table name used by this indexer for a specific instance of Rya.
     *
     * @param ryaInstanceName -  The name of the Rya instance the table name is for. (not null)
     * @return The Accumulo table name used by this indexer for a specific instance of Rya.
     */
    public static String makeTableName(final String ryaInstanceName) {
        requireNonNull(ryaInstanceName);
        return ryaInstanceName + "_statement_counts_by_context";
    }

    // Values that are set through the setter methods prior to initialization.
    private AccumuloRdfConfiguration conf = null;
    private Connector connector = null;
    private MultiTableBatchWriter multiTableWriter = null;

    private Optional<StatementCountsRepository> stmtCountsRepo = Optional.empty();

    @Override
    public void setConf(final Configuration conf) {
        this.conf = new AccumuloRdfConfiguration(conf);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConnector(final Connector connector) {
        this.connector = connector;
    }

    @Override
    public void setMultiTableBatchWriter(final MultiTableBatchWriter multiTableWriter) throws IOException {
        this.multiTableWriter = multiTableWriter;
    }

    @Override
    public void init() {
        log.info("Initializing the AccumuloStatementCountIndexer.");

        // Ensure the required components were set prior to calling this method.
        requireNonNull(conf);
        requireNonNull(connector);
        requireNonNull(multiTableWriter);

        // Create a supplier that generates the repository's batch writer using the provided multi table batch writer.
        final BatchWriterSupplier bwSupplier = tableName -> {
            requireNonNull(tableName);
            return multiTableWriter.getBatchWriter(tableName);
        };

        stmtCountsRepo = Optional.of( AccumuloStatementCountsRepository.makeReadWrite(bwSupplier, connector, getTableName()) );
    }

    @Override
    public String getTableName() {
        return makeTableName(conf.getTablePrefix());
    }

    @Override
    public void storeStatement(final RyaStatement ryaStmt) throws IOException {
        requireNonNull(ryaStmt);
        requireInitialized();

        try {
            // Increment the count for the statement's context.
            final Statement statement = RyaToRdfConversions.convertStatement( ryaStmt );
            final Delta delta = new Delta(statement.getContext(), 1);
            stmtCountsRepo.get().updateCount( Lists.newArrayList(delta) );
        } catch (final StatementCountsException e) {
            throw new IOException("Unable to increment the statement's count.", e);
        }
    }

    @Override
    public void storeStatements(final Collection<RyaStatement> ryaStmts) throws IOException {
        requireNonNull(ryaStmts);
        requireInitialized();

        // Iterate through all of the statements and create a list of deltas.
        final List<Delta> deltas = new ArrayList<>( ryaStmts.size() );
        for(final RyaStatement ryaStmt : ryaStmts) {
            final Statement statement = RyaToRdfConversions.convertStatement( ryaStmt );
            deltas.add( new Delta(statement.getContext(), 1) );
        }

        // Forward that list to the repository.
        try {
            stmtCountsRepo.get().updateCount( deltas );
        } catch (final StatementCountsException e) {
            throw new IOException("", e);
        }
    }

    @Override
    public void deleteStatement(final RyaStatement ryaStmt) throws IOException {
        requireNonNull(ryaStmt);
        requireInitialized();

        // Decrement the count for the statement's context.
        try {
            final Statement statement = RyaToRdfConversions.convertStatement( ryaStmt );
            final Delta delta = new Delta(statement.getContext(), -1);
            stmtCountsRepo.get().updateCount( Lists.newArrayList(delta) );
        } catch (final StatementCountsException e) {
            throw new IOException("Unable to decrement the statement's count.", e);
        }
    }

    @Override
    public void dropGraph(final RyaIRI... graphs) {
        requireNonNull(graphs);
        requireInitialized();

        final StatementCountsRepository repo = stmtCountsRepo.get();
        for(final RyaIRI iri : graphs) {
            final IRI context = RyaToRdfConversions.convertIRI( iri );
            try {
                repo.deleteCount(context);
            } catch (final StatementCountsException e) {
                log.error("Unable to drop context " + context + " from the index.", e);
            }
        }
    }

    @Override
    public void purge(final RdfCloudTripleStoreConfiguration configuration) {
        requireNonNull(configuration);
        requireInitialized();

        try {
            stmtCountsRepo.get().deleteAllCounts();
        } catch (final StatementCountsException e) {
            log.error("Unable to purge the index.", e);
        }
    }

    @Override
    public void destroy() {
        // There are no extra resources to release. The batch writer is closed when the RyaDAO's multi-table one is.
        log.info("Destroying the AccumuloStatementCountIndexer.");
    }

    @Override
    public void dropAndDestroy() {
        final String tableName = getTableName();
        try {
            connector.tableOperations().delete( tableName );
        } catch(final TableNotFoundException e) {
            // This is okay, the table already does not exist.
        } catch (AccumuloException | AccumuloSecurityException e) {
            log.error("Unable to delete the accumult table named \"" + tableName + "\".", e);
        }
    }

    /**
     * @throws IllegalStateException - Indicates the indexer has not been initialized because a required component is missing.
     */
    private void requireInitialized() throws IllegalStateException {
        if(!stmtCountsRepo.isPresent()) {
            throw new IllegalStateException("The indexer does not appear to be initialized. Ensure init() was invoked.");
        }
    }

    // This methods don't appear to be used by the Accumulo implementation of the RyaDAO.
    @Override public void flush() throws IOException { }
    @Override public void close() throws IOException { }
    @Override public Set<IRI> getIndexablePredicates() { return null; }
}