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

import static java.util.Objects.requireNonNull;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.rya.api.statistics.StatementCountsRepository;
import org.eclipse.rdf4j.model.Resource;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Accumulo implementation of {@link StatementCountsRepository}. It has both a Read Only and a Read Write
 * method of being constructed. The batch writer is provided to the repository by the application so that
 * it may be tied to the {@link MultiTableBatchWriter} that is used to synchronize writes to the SPO tables.
 * If the repository is only being used to read values, then the writer doesn't need to be supplied.
 * <p/>
 * The table uses the Context value as the Row ID, a column named "count" to store
 * those counts, and a {@link LongCombiner.FIXED_LEN_ENCODER} encoded byte[] as the
 * value.
 * <p/>
 * A {@link SummingCombiner} combines the values that are writen to each row. Its priority
 * is set to "11".
 * <p/>
 */
@DefaultAnnotation(NonNull.class)
public class AccumuloStatementCountsRepository implements StatementCountsRepository {

    // Constants used to reference the count column in Accumulo.
    private static final Text COUNT_COLUMN = new Text("count");
    private static final Text NO_COLUMN_QUALIFIER = new Text("");

    // An encoder used to convert Long values into byte[] values.
    private static final Encoder<Long> LONG_ENCODER = LongCombiner.FIXED_LEN_ENCODER;

    private final Supplier<BatchWriter> bwSupplier;
    private final Connector connector;
    private final String tableName;

    /**
     * Constructs an instance of {@link AccumuloStatementCountsRepository}.
     *
     * @param bwSupplier - If the repository is RW, then this is the writer that will be used. If none is
     *   supplied, then this is a R only repo. (not null)
     * @param connector - Connects to the instance of Accumulo that hosts the repository. (not null)
     * @param tableName - Identifies the Accumulo table that backs the repository.(not null)
     */
    private AccumuloStatementCountsRepository(
            final Optional<BatchWriterSupplier> bwSupplier,
            final Connector connector,
            final String tableName) {
        requireNonNull(bwSupplier);
        this.connector = requireNonNull(connector);
        this.tableName = requireNonNull(tableName);

        if(!bwSupplier.isPresent()) {
            // The supplier will always return null if this is Read Only.
            this.bwSupplier = () -> null;
        } else {
            // The supplier will always return the first successfully created Batch Writer if this is Read Write.
            this.bwSupplier = Suppliers.memoize(() -> {
                try {
                    return bwSupplier.get().makeBatchWriter(this.tableName);
                } catch (final TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
                    throw new RuntimeException("Unable to create the Batch Writer.", e);
                }
            });
        }
    }

    /**
     * Creates a {@link AccumuloStatementCountsRepository} that is able to write values to and delete values from the repository.
     * <p/>
     * It is your responsibility to ensure the batch writer that is supplied by {@code bwSupplier} is periodically
     * flushed and ultimately closed.
     *
     * @param bwSupplier - Provides access to the {@link BatchWriter} that will be used to perform writes. (not null)
     * @param connector - Connects to the instance of Accumulo that hosts the repository. (not null)
     * @param tableName - Identifies the Accumulo table that backs the repository.(not null)e
     * @return A Read/Write instance of {@link AccumuloStatementCountsRepository}.
     */
    public static StatementCountsRepository makeReadWrite(
            final BatchWriterSupplier bwSupplier,
            final Connector connector,
            final String tableName) {
        return new AccumuloStatementCountsRepository(Optional.of(bwSupplier), connector, tableName);
    }

    /**
     * Creates a {@link AccumuloStatementCountsRepository} that is only able to read values from the repository.
     *
     * @param connector - Connects to the instance of Accumulo that hosts the repository. (not null)
     * @param tableName - Identifies the Accumulo table that backs the repository.(not null)e
     * @return A Read Only instance of {@link AccumuloStatementCountsRepository}.
     */
    public static StatementCountsRepository makeReadOnly(
            final Connector connector,
            final String tableName) {
        return new AccumuloStatementCountsRepository(Optional.empty(), connector, tableName);
    }

    @Override
    public boolean isInstalled() throws StatementCountsException {
        return connector.tableOperations().exists(tableName);
    }

    @Override
    public void install() throws AlreadyInstalledException, StatementCountsException {
        // Ensure the table doesn't already exist.
        if(isInstalled()) {
            throw new AlreadyInstalledException("An accumulo table named \"" + tableName + "\" already exists." +
                    "Ensure the Statement Counts Repository hasn't alreeady been installed.");
        }

        try {
            // Create the table.
            connector.tableOperations().create(tableName);

            // Add a combiner to it that does the summing.
            final IteratorSetting settings = new IteratorSetting(11, "StatementCountSummers", SummingCombiner.class);
            settings.addOption("type", LongCombiner.Type.FIXEDLEN.toString());
            settings.addOption("columns", COUNT_COLUMN.toString());

            connector.tableOperations().attachIterator(tableName, settings,
                    EnumSet.of(IteratorScope.majc, IteratorScope.minc, IteratorScope.scan));

        } catch (AccumuloException | AccumuloSecurityException | TableExistsException | TableNotFoundException e) {
            throw new StatementCountsException("Could not install the Statement Counts Repository because of a " +
                    "problem while installing it.", e);
        }
    }

    @Override
    public void updateCount(final List<Delta> deltas) throws NotInstalledException, StatementCountsException {
        requireNonNull(deltas);
        requireWrite();
        requireInstalled();

        final BatchWriter writer = bwSupplier.get();
        try {
            // Create a mutation for each delta that uses the context as the row key.
            for(final Delta delta : deltas) {
                final Text rowId = new Text( delta.getContext().stringValue() );
                final Value deltaVal = new Value(LONG_ENCODER.encode(delta.getDelta()));

                final Mutation mutation = new Mutation(rowId);
                mutation.put(COUNT_COLUMN, NO_COLUMN_QUALIFIER, deltaVal);
                writer.addMutation(mutation);
            }
        } catch (final MutationsRejectedException e) {
            throw new StatementCountsException("The mutations were rejected. This batch of deltas was not queued for writing.", e);
        }
    }

    @Override
    public Optional<Long> getCount(final Resource context) throws NotInstalledException, StatementCountsException {
        requireNonNull(context);
        requireInstalled();

        Scanner scan = null;
        try {
            // Configure the scanner to find a single row for the context and only fetch the count column family.
            scan = connector.createScanner(tableName, new Authorizations());
            scan.setRange(new Range(context.stringValue()));
            scan.fetchColumnFamily(COUNT_COLUMN);

            // Scan the Accumulo table using the provided row id.
            final Iterator<Entry<Key, Value>> it = scan.iterator();

            // If no rows matched the context, then there isn't a count.
            if(!it.hasNext()) {
                return Optional.empty();
            }

            // Otherwise, return the value that is stored.
            final Entry<Key, Value> entry = it.next();
            final Long count = LONG_ENCODER.decode( entry.getValue().get() );
            return Optional.of(count);

        } catch (final TableNotFoundException e) {
            throw new NotInstalledException("The accumulo table the statement counts are stored in appears to have been deleted.", e);
        } finally {
            scan.close();
        }
    }

    @Override
    public void deleteCount(final Resource context) throws NotInstalledException, StatementCountsException {
        requireNonNull(context);
        requireWrite();
        requireInstalled();

        final BatchWriter writer = bwSupplier.get();

        try {
            // Create a mutation that deletes the context from the table.
            final Text rowId = new Text( context.stringValue() );
            final Mutation mutation = new Mutation(rowId);
            mutation.putDelete(COUNT_COLUMN, NO_COLUMN_QUALIFIER);
            writer.addMutation(mutation);
        } catch (final MutationsRejectedException e) {
            throw new StatementCountsException("The mutation was rejected. The context \"" + context + "\" could not be deleted.", e);
        }
    }

    @Override
    public void deleteAllCounts() throws NotInstalledException, StatementCountsException {
        requireWrite();
        requireInstalled();

        final BatchWriter writer = bwSupplier.get();

        try {
            // Scan for all rows that are in the table with the COUNT column and add delete mutations for them.
            final Scanner scan = connector.createScanner(tableName, new Authorizations());
            scan.fetchColumnFamily(COUNT_COLUMN);

            for(final Entry<Key,Value> entry : scan) {
                final Text rowId = entry.getKey().getRow();
                final Mutation delete = new Mutation(rowId);
                delete.putDelete(COUNT_COLUMN, NO_COLUMN_QUALIFIER);
                writer.addMutation(delete);
            }
        } catch (final TableNotFoundException | MutationsRejectedException e) {
            throw new StatementCountsException("The mutation was rejected. Unable to delete all of the counts.", e);
        }
    }

    @Override
    public void uninstall() throws StatementCountsException {
        try {
            connector.tableOperations().delete(tableName);
        } catch(final TableNotFoundException e) {
            // This is okay, the table already does not exist.
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new StatementCountsException("Unable to delete the accumult table named \"" + tableName + "\".", e);
        }
    }

    private void requireInstalled() throws NotInstalledException, StatementCountsException {
        if(!isInstalled()) {
            throw new NotInstalledException("This repository has not been installed, so you may not use this function.");
        }
    }

    private void requireWrite() throws StatementCountsException {
        if(bwSupplier.get() == null) {
            throw new StatementCountsException("The invoked function requires a Batch Writer, but this is a Read Only" +
                    "instance of the repository, so you may not use this function.");
        }
    }
}