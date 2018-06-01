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
package org.apache.rya.accumulo.experimental;

import java.io.IOException;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.index.RyaSecondaryIndexer;

/**
 * Accumulo specific methods that may be used when implementing a secondary indexer for Rya.
 */
public interface AccumuloIndexer extends RyaSecondaryIndexer {

    /**
     * Provides this indexer with the {@link MultiTableBatchWriter} that is used by the
     * {@link AccumuloRyaDAO} that the indexer is integrating with. This method is invoked by
     * the DAO after {@link #setConf(org.apache.hadoop.conf.Configuration)} but before
     * {@link #init()}.
     * <p/>
     * In order for this indexer's batch writer to be flushed/closed/etc in unison with
     * the DAO's batch writers, then it must obtain its batch writer from {@code writer}.
     * <p/>
     * For example:
     * <pre>
     *  public class FooIndexer extends AccumuloIndexer {
     *      ...
     *      public void setMultiTableBatchWriter(MultiTableBatchWriter writer) throws IOException {
     *          try {
     *              this.batchWriter = writer.getBatchWriter("foo_index_table");
     *          } catch (final Exception e) {
     *              throw new IOException("This index could not obtain a BatchWriter.", e)
     *          }
     *      }
     *      ...
     *  }
     * </pre>
     *
     * @param writer - The {@link MultiTableBatchWriter} that is being used by Rya's DAO. (not null)
     * @throws IOException A problem was encountered while interacting with the {@code writer}.
     */
    public void setMultiTableBatchWriter(MultiTableBatchWriter writer) throws IOException;

    /**
     * Provides the indexer with the {@link Connector} that is used by the {@link AccumuloRyaDAO}
     * that the indexer is integrating with. This method is invoked by the DAO after
     * {@link #setConf(org.apache.hadoop.conf.Configuration)} but before {@link #init()}.
     * <p/>
     * The connector is required by the indexer so that it may perform table operations,
     * such as dropping tables.
     *
     * @param connector - The {@link Connector} used to communicate with the configured Accumulo instance. (not null)
     */
    public void setConnector(Connector connector);

    /**
     * Releases all resources that are being held by the indexer.
     * <p/>
     * This method is called after the DAO closes the writer that was provided by
     * {@link #setMultiTableBatchWriter(MultiTableBatchWriter)}, so implementations of this method must assume
     * their writers have already been flushed and closed when this is invoked.
     * <p/>
     * TODO This method could be deprecated in favor of the {@link #close()} method.
     */
    public void destroy();

    /**
     * Deletes all data from the index that is in the configured Rya instance. Any
     * tables that were created within Accumulo remain in tact. This index remains
     * initialized and may continue to be used to index data.
     * <p/>
     * TODO
     * This method should be revised to throw a {@link RyaDAOException}. Right
     * now a program that is invoking purge would have no way to know if the
     * operation failed other than by scraping the logs.
     *
     * @param configuration - Configures the purge operation. (not null)
     */
    public void purge(RdfCloudTripleStoreConfiguration configuration);

    /**
     * Drops all tables that were created by the index for the configured Rya instance.
     * This index will also be destroyed as if {@link #destroy()} was invoked. It will
     * need to be initialized again to be reused.
     * <p/>
     * TODO
     * This method should be revised to throw a {@link RyaDAOException}. Right
     * now a program that is invoking dropAndDestroy would have no way to know if the
     * operation failed other than by scraping the logs.
     */
    public void dropAndDestroy();
}