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
package org.apache.rya.api.persist;

import java.util.Iterator;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.query.RyaQueryEngine;

/**
 * Provides the access layer to the Rya triple store.
 *
 * Date: Feb 28, 2012
 * Time: 3:30:14 PM
 */
public interface RyaDAO<C extends RdfCloudTripleStoreConfiguration> extends RyaConfigured<C> {

    /**
     * Initialize the RyaDAO. Should only be called once, otherwise, if already initialized, it will
     * throw an exception.
     *
     * @throws RyaDAOException
     */
    public void init() throws RyaDAOException;

    /**
     *
     * @return true if the store is already initialized
     * @throws RyaDAOException
     */
    public boolean isInitialized() throws RyaDAOException;

    /**
     * Shutdown the store. To reinitialize, call the init() method.
     *
     * @throws RyaDAOException
     */
    public void destroy() throws RyaDAOException;

    /**
     * Add and commit a single RyaStatement
     *
     * @param statement
     * @throws RyaDAOException
     */
    public void add(RyaStatement statement) throws RyaDAOException;

    /**
     * Add and commit a collection of RyaStatements
     *
     * @param statementIter
     * @throws RyaDAOException
     */
    public void add(Iterator<RyaStatement> statementIter) throws RyaDAOException;

    /**
     * Delete a RyaStatement. The Configuration should provide the auths to perform the delete
     *
     * @param statement
     * @param conf
     * @throws RyaDAOException
     */
    public void delete(RyaStatement statement, C conf) throws RyaDAOException;

    /**
     * Drop a set of Graphs. The Configuration should provide the auths to perform the delete
     *
     * @param conf
     * @throws RyaDAOException
     */
    public void dropGraph(C conf, RyaIRI... graphs) throws RyaDAOException;

    /**
     * Delete a collection of RyaStatements.
     *
     * @param statements
     * @param conf
     * @throws RyaDAOException
     */
    public void delete(Iterator<RyaStatement> statements, C conf) throws RyaDAOException;

    /**
     * Get the version of the store.
     *
     * @return
     * @throws RyaDAOException
     */
    public String getVersion() throws RyaDAOException;

    /**
     * Get the Rya query engine
     * @return
     */
    public RyaQueryEngine<C> getQueryEngine();

    /**
     * Get the Rya Namespace Manager
     * @return
     */
    public RyaNamespaceManager<C> getNamespaceManager();

    /**
     * Deletes all data in the core SPO tables as well as the indexes over those
     * values from the configured Rya instance. Any structures that were created
     * within the database architecture used to host Rya remain in tact. This DAO
     * remains initialized and may continue to be used to insert data.
     * <p/>
     * For example, if you are using the Accumulo implementation of this interface,
     * this will delete all of the rows that are in the tables (the data), but it
     * will not delete the tables themselves. It just clears all the data out. You
     * could then add more data again without initializing the DAO again.
     * <p/>
     * TODO
     * This method should be revised to throw a {@link RyaDAOException}. Right
     * now a program that is invoking purge would have no way to know if the purge
     * operation failed other than by scraping the logs.
     *
     * @param configuration - Configures the purge operation. (not null)
     */
    public void purge(RdfCloudTripleStoreConfiguration configuration);

    /**
     * Deletes all data in the core SPO tables as well as the indexes over those
     * values for the configured Rya instance. All structures that were created
     * within the database architecture used to host Rya are also deleted. This
     * DAO will also be destroyed as if {@link #destroy()} was invoked.
     * <p/>
     * For example, if you are using the Accumulo implementation of this interface,
     * this will drop all of the tables and destroy the DAO. You would need to
     * initialize the DAO again if you wanted to recreate and use the Rya instance.
     *
     * @throws RyaDAOException The operation was unable to be completed.
     */
    public void dropAndDestroy() throws RyaDAOException;

    /**
     * Flushes any RyaStatements queued for insertion and writes them to the
     * datastore.
     * @throws RyaDAOException
     */
    public void flush() throws RyaDAOException;
}
