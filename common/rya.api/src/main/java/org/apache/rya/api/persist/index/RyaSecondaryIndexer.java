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
package org.apache.rya.api.persist.index;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.eclipse.rdf4j.model.IRI;

/**
 * Secondary Indexers are responsible for maintaining the state of a Secondary Index
 * when {@link RyaStatement}s are inserted or deleted from an instance of Rya.
 */
public interface RyaSecondaryIndexer extends Closeable, Flushable, Configurable {

    /**
     * Initializes the indexer using the configuration provided by {@link #setConf(org.apache.hadoop.conf.Configuration)}.
     */
    public void init();

    /**
     * Returns the table name if the implementation supports it.
     * Note that some indexers use multiple tables, this only returns one.
     * TODO recommend that we deprecate this method because it's a leaky interface.
     * @return table name as a string.
     */
    public String getTableName();

    /**
     * Index a collection of {@link RyaStatement}s.
     *
     * @param statements - The values to index. (not null)
     * @throws IOException The values could not be indexed.
     */
    public void storeStatements(Collection<RyaStatement> statements) throws IOException;

    /**
     * Index a {@link RyaStatement}.
     *
     * @param statement - The value to index. (not null)
     * @throws IOException The value could not be indexed.
     */
    public void storeStatement(RyaStatement statement) throws IOException;

    /**
     * Remove a {@link RyaStatement}'s indexing.
     *
     * @param stmt - The value to remove from the index. (not null)
     * @throws IOException The calue could not be removed from the index.
     */
    public void deleteStatement(RyaStatement stmt) throws IOException;

    /**
     * Remove all {@link RyaStatement} indexing for statements whose context values
     * are in the provided set of graphs.
     * <p/>
     * TODO
     * This method should be revised to throw a {@link RyaDAOException}. Right
     * now a program that is invoking dropGraph would have no way to know if the
     * operation failed other than by scraping the logs.
     *
     * @param graphs - The graphs that will be dropped from the index. (not null)
     */
    public void dropGraph(RyaIRI... graphs);

    /**
     * @return the set of predicates indexed by the indexer.
     */
    public Set<IRI> getIndexablePredicates();

    @Override
    public void flush() throws IOException;

    @Override
    public void close() throws IOException;
}