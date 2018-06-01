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
package org.apache.rya.accumulo.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.rya.api.statistics.StatementCountsRepository;
import org.apache.rya.api.statistics.StatementCountsRepository.AlreadyInstalledException;
import org.apache.rya.api.statistics.StatementCountsRepository.Delta;
import org.apache.rya.test.accumulo.AccumuloITBase;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Integration tests for the methods of {@link AccumuloStatementCountsRepository}.
 */
public class AccumuloStatementCountsRepositoryIT extends AccumuloITBase {

    /**
     * The repository claims it is not installed when invoked on a repository that has never been created.
     */
    @Test
    public void isInstalled_false() throws Exception {
        final StatementCountsRepository repo =
                AccumuloStatementCountsRepository.makeReadOnly(super.getConnector(), super.getRyaInstanceName());

        assertFalse(repo.isInstalled());
    }

    /**
     * The repository claims it has been installed after the repository's install method is invoked.
     */
    @Test
    public void isInstalled_true() throws Exception {
        final StatementCountsRepository repo =
                AccumuloStatementCountsRepository.makeReadOnly(super.getConnector(), super.getRyaInstanceName());

        assertFalse(repo.isInstalled());
        repo.install();
        assertTrue(repo.isInstalled());
    }

    /**
     * The repository throws an exception if you try to install an already installed repository.
     */
    @Test
    public void install_alreadyCreated() throws Exception {
        final StatementCountsRepository repo =
                AccumuloStatementCountsRepository.makeReadOnly(super.getConnector(), super.getRyaInstanceName());

        assertFalse(repo.isInstalled());
        repo.install();

        // This test will only pass if the expected exception is thrown the second time you call install.
        boolean exceptionThrown = false;
        try {
            repo.install();
        } catch(final AlreadyInstalledException e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    /**
     * Uninstalling a repository that has not been installed doesn't change anything.
     */
    @Test
    public void uninstall_notInstalled() throws Exception {
        final StatementCountsRepository repo =
                AccumuloStatementCountsRepository.makeReadOnly(super.getConnector(), super.getRyaInstanceName());

        assertFalse(repo.isInstalled());
        repo.uninstall();
        assertFalse(repo.isInstalled());
    }

    /**
     * Uninstalling an installed repository removes it.
     */
    @Test
    public void uninstall() throws Exception {
        final StatementCountsRepository repo =
                AccumuloStatementCountsRepository.makeReadOnly(super.getConnector(), super.getRyaInstanceName());

        assertFalse(repo.isInstalled());
        repo.install();
        assertTrue(repo.isInstalled());
        repo.uninstall();
        assertFalse(repo.isInstalled());
    }

    /**
     * Fetching a count that hasn't been set returns an empty optional.
     */
    @Test
    public void getCount_absent() throws Exception {
        final String tableName = super.getRyaInstanceName();
        final Connector connector = super.getConnector();

        // We need to hold onto the BW that is supplied so that we can flush and shut it down.
        final AtomicReference<BatchWriter> bw = new AtomicReference<>();
        final BatchWriterSupplier bwSupplier = tableName1 -> {
            final BatchWriter batchWriter = connector.createBatchWriter(tableName1, new BatchWriterConfig());
            bw.set( batchWriter );
            return batchWriter;
        };

        try {
            final StatementCountsRepository repo = AccumuloStatementCountsRepository.makeReadWrite(bwSupplier, connector, tableName);

            repo.install();

            // Store a count for a context.
            final ValueFactory vf = SimpleValueFactory.getInstance();

            final List<Delta> deltas = Lists.newArrayList( new Delta(vf.createIRI("urn:contextA"), 5L) );
            repo.updateCount(deltas);

            // Flush the writer.
            bw.get().flush();

            // Fetch a count for a context that has not been stored.
            final Optional<Long> count = repo.getCount(vf.createIRI("urn:contextB"));
            assertFalse(count.isPresent());
        } finally {
            bw.get().close();
        }
    }

    /**
     * Apply a series of deltas to a variety of contexts and show that they are appropriately aggregated.
     */
    @Test
    public void getCount_summed() throws Exception {
        final String tableName = super.getRyaInstanceName();
        final Connector connector = super.getConnector();

        // We need to hold onto the BW that is supplied so that we can flush and shut it down.
        final AtomicReference<BatchWriter> bw = new AtomicReference<>();
        final BatchWriterSupplier bwSupplier = tableName1 -> {
            final BatchWriter batchWriter = connector.createBatchWriter(tableName1, new BatchWriterConfig());
            bw.set( batchWriter );
            return batchWriter;
        };

        try {
            final StatementCountsRepository repo = AccumuloStatementCountsRepository.makeReadWrite(bwSupplier, connector, tableName);

            repo.install();

            // Create a series of deltas that include both addition and subtraction.
            final ValueFactory vf = SimpleValueFactory.getInstance();
            final List<Delta> deltas = Lists.newArrayList(
                    new Delta(vf.createIRI("urn:contextA"), 1L),
                    new Delta(vf.createIRI("urn:contextA"), 5L),
                    new Delta(vf.createIRI("urn:contextA"), 2L),

                    new Delta(vf.createIRI("urn:contextB"), 2L),
                    new Delta(vf.createIRI("urn:contextB"), -1L),

                    new Delta(vf.createIRI("urn:contextC"), 15L));
            repo.updateCount(deltas);

            // Flush the writer.
            bw.get().flush();

            // Fetch a count for each of the stored contexts.
            assertEquals(new Long(8), repo.getCount(vf.createIRI("urn:contextA")).get());
            assertEquals(new Long(1), repo.getCount(vf.createIRI("urn:contextB")).get());
            assertEquals(new Long(15), repo.getCount(vf.createIRI("urn:contextC")).get());
        } finally {
            bw.get().close();
        }
    }

    /**
     * Deleting a context that alreaedy does not exist doesn't throw exceptions.
     */
    @Test
    public void deleteCount_contextAbsent() throws Exception {
        final String tableName = super.getRyaInstanceName();
        final Connector connector = super.getConnector();

        // We need to hold onto the BW that is supplied so that we can flush and shut it down.
        final AtomicReference<BatchWriter> bw = new AtomicReference<>();
        final BatchWriterSupplier bwSupplier = tableName1 -> {
            final BatchWriter batchWriter = connector.createBatchWriter(tableName1, new BatchWriterConfig());
            bw.set( batchWriter );
            return batchWriter;
        };

        try {
            final StatementCountsRepository repo = AccumuloStatementCountsRepository.makeReadWrite(bwSupplier, connector, tableName);

            repo.install();

            // Store a count for a context.
            final ValueFactory vf = SimpleValueFactory.getInstance();

            final List<Delta> deltas = Lists.newArrayList( new Delta(vf.createIRI("urn:contextA"), 5L) );
            repo.updateCount(deltas);

            // Delete a context that is not stored.
            repo.deleteCount( vf.createIRI("urn:contextB") );

            // Flush the writer.
            bw.get().flush();

            // Show the count still exists for the existing context.
            assertEquals(new Long(5), repo.getCount(vf.createIRI("urn:contextA")).get());

        } finally {
            bw.get().close();
        }
    }

    /**
     * You can delete a context from the database.
     */
    @Test
    public void deleteCount() throws Exception {
        final String tableName = super.getRyaInstanceName();
        final Connector connector = super.getConnector();

        // We need to hold onto the BW that is supplied so that we can flush and shut it down.
        final AtomicReference<BatchWriter> bw = new AtomicReference<>();
        final BatchWriterSupplier bwSupplier = tableName1 -> {
            final BatchWriter batchWriter = connector.createBatchWriter(tableName1, new BatchWriterConfig());
            bw.set( batchWriter );
            return batchWriter;
        };

        try {
            final StatementCountsRepository repo = AccumuloStatementCountsRepository.makeReadWrite(bwSupplier, connector, tableName);

            repo.install();

            // Store a count for a context.
            final ValueFactory vf = SimpleValueFactory.getInstance();

            final List<Delta> deltas = Lists.newArrayList( new Delta(vf.createIRI("urn:contextA"), 5L) );
            repo.updateCount(deltas);
            bw.get().flush();

            // Show it was stored.
            assertEquals(new Long(5), repo.getCount(vf.createIRI("urn:contextA")).get());

            // Delete it.
            repo.deleteCount( vf.createIRI("urn:contextA") );
            bw.get().flush();

            // Show the count still exists for the existing context.
            assertFalse( repo.getCount(vf.createIRI("urn:contextA")).isPresent() );

        } finally {
            bw.get().close();
        }
    }

    /**
     * Apply a series of deltas to a variety of contexts and show that they are appropriately aggregated.
     */
    @Test
    public void deleteAllCounts() throws Exception {
        final String tableName = super.getRyaInstanceName();
        final Connector connector = super.getConnector();

        // We need to hold onto the BW that is supplied so that we can flush and shut it down.
        final AtomicReference<BatchWriter> bw = new AtomicReference<>();
        final BatchWriterSupplier bwSupplier = tableName1 -> {
            final BatchWriter batchWriter = connector.createBatchWriter(tableName1, new BatchWriterConfig());
            bw.set( batchWriter );
            return batchWriter;
        };

        try {
            final StatementCountsRepository repo = AccumuloStatementCountsRepository.makeReadWrite(bwSupplier, connector, tableName);

            repo.install();

            // Create a series of deltas that include both addition and subtraction.
            final ValueFactory vf = SimpleValueFactory.getInstance();
            final List<Delta> deltas = Lists.newArrayList(
                    new Delta(vf.createIRI("urn:contextA"), 1L),
                    new Delta(vf.createIRI("urn:contextB"), 2L),
                    new Delta(vf.createIRI("urn:contextC"), 15L));
            repo.updateCount(deltas);

            // Flush the writer.
            bw.get().flush();

            // Show the counts were stored.
            assertEquals(new Long(1), repo.getCount(vf.createIRI("urn:contextA")).get());
            assertEquals(new Long(2), repo.getCount(vf.createIRI("urn:contextB")).get());
            assertEquals(new Long(15), repo.getCount(vf.createIRI("urn:contextC")).get());

            // Delete all of the counts.
            repo.deleteAllCounts();
            bw.get().flush();

            // Show none of the counts exist anymore.
            assertFalse( repo.getCount(vf.createIRI("urn:contextA")).isPresent() );
            assertFalse( repo.getCount(vf.createIRI("urn:contextB")).isPresent() );
            assertFalse( repo.getCount(vf.createIRI("urn:contextC")).isPresent() );

        } finally {
            bw.get().close();
        }
    }
}