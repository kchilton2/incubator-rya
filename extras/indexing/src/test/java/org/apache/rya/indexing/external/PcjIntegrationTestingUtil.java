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
package org.apache.rya.indexing.external;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjSerializer;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjVarOrderFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.ShiftVarOrderFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.mongo.MongoPcjDocuments;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailException;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.mongodb.MongoClient;

public class PcjIntegrationTestingUtil {

    private static final AccumuloPcjSerializer converter = new AccumuloPcjSerializer();

    public static Set<QueryModelNode> getTupleSets(final TupleExpr te) {
        final ExternalTupleVisitor etv = new ExternalTupleVisitor();
        te.visit(etv);
        return etv.getExtTup();
    }

    public static void deleteCoreRyaTables(final Connector accCon, final String prefix)
            throws AccumuloException, AccumuloSecurityException,
            TableNotFoundException {
        final TableOperations ops = accCon.tableOperations();
        if (ops.exists(prefix + "spo")) {
            ops.delete(prefix + "spo");
        }
        if (ops.exists(prefix + "po")) {
            ops.delete(prefix + "po");
        }
        if (ops.exists(prefix + "osp")) {
            ops.delete(prefix + "osp");
        }
    }

    public static SailRepository getAccumuloPcjRepo(final String tablePrefix, final String instance)
            throws AccumuloException, AccumuloSecurityException,
            RyaDAOException, RepositoryException, InferenceEngineException,
            NumberFormatException, UnknownHostException, SailException {

        final AccumuloRdfConfiguration pcjConf = new AccumuloRdfConfiguration();
        pcjConf.set(ConfigUtils.USE_PCJ, "true");
        pcjConf.set(PrecomputedJoinIndexerConfig.PCJ_STORAGE_TYPE,PrecomputedJoinStorageType.ACCUMULO.name());
        populateAccumuloConfig(instance, tablePrefix, pcjConf);

        final Sail pcjSail = RyaSailFactory.getInstance(pcjConf);
        final SailRepository pcjRepo = new SailRepository(pcjSail);
        return pcjRepo;
    }

    public static SailRepository getAccumuloNonPcjRepo(final String tablePrefix,
            final String instance) throws AccumuloException,
    AccumuloSecurityException, RyaDAOException, RepositoryException, InferenceEngineException,
    NumberFormatException, UnknownHostException, SailException {

        final AccumuloRdfConfiguration nonPcjConf = new AccumuloRdfConfiguration();
        populateAccumuloConfig(instance, tablePrefix, nonPcjConf);
        final Sail nonPcjSail = RyaSailFactory.getInstance(nonPcjConf);
        final SailRepository nonPcjRepo = new SailRepository(nonPcjSail);
        return nonPcjRepo;
    }

    private static void populateAccumuloConfig(final String instance, final String tablePrefix, final AccumuloRdfConfiguration config) {
        config.set(ConfigUtils.USE_MOCK_INSTANCE, "true");
        config.set(ConfigUtils.CLOUDBASE_INSTANCE, instance);
        config.set(ConfigUtils.CLOUDBASE_USER, "test_user");
        config.set(ConfigUtils.CLOUDBASE_PASSWORD, "pswd");
        config.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, "localhost");
        config.setTablePrefix(tablePrefix);
    }

    public static void closeAndShutdown(final SailRepositoryConnection connection,
            final SailRepository repo) throws RepositoryException {
        connection.close();
        repo.shutDown();
    }

    public static void deleteIndexDocuments(final MongoClient client, final String instance) {
        client.getDatabase(instance).getCollection(MongoPcjDocuments.PCJ_COLLECTION_NAME).drop();
    }

    public static void deleteIndexTables(final Connector accCon, final int tableNum,
            final String prefix) throws AccumuloException, AccumuloSecurityException,
    TableNotFoundException {
        final TableOperations ops = accCon.tableOperations();
        final String tablename = prefix + "INDEX_";
        for (int i = 1; i < tableNum + 1; i++) {
            if (ops.exists(tablename + i)) {
                ops.delete(tablename + i);
            }
        }
    }

    public static class BindingSetAssignmentCollector extends
            AbstractQueryModelVisitor<RuntimeException> {

        private final Set<QueryModelNode> bindingSetList = Sets.newHashSet();

        public Set<QueryModelNode> getBindingSetAssignments() {
            return bindingSetList;
        }

        public boolean containsBSAs() {
            return bindingSetList.size() > 0;
        }

        @Override
        public void meet(final BindingSetAssignment node) {
            bindingSetList.add(node);
            super.meet(node);
        }

    }

    public static class ExternalTupleVisitor extends
            AbstractQueryModelVisitor<RuntimeException> {

        private final Set<QueryModelNode> eSet = new HashSet<>();

        @Override
        public void meetNode(final QueryModelNode node) throws RuntimeException {
            if (node instanceof ExternalTupleSet) {
                eSet.add(node);
            }
            super.meetNode(node);
        }

        public Set<QueryModelNode> getExtTup() {
            return eSet;
        }

    }






    //****************************Creation and Population of PcjTables Accumulo***************************





    /**
     * Creates a new PCJ Table in Accumulo and populates it by scanning an
     * instance of Rya for historic matches.
     * <p>
     * If any portion of this operation fails along the way, the partially
     * create PCJ table will be left in Accumulo.
     *
     * @param ryaConn - Connects to the Rya that will be scanned. (not null)
     * @param accumuloConn - Connects to the accumulo that hosts the PCJ results. (not null)
     * @param pcjTableName - The name of the PCJ table that will be created. (not null)
     * @param sparql - The SPARQL query whose results will be loaded into the table. (not null)
     * @param resultVariables - The variables that are included in the query's resulting binding sets. (not null)
     * @param pcjVarOrderFactory - An optional factory that indicates the various variable orders
     *   the results will be stored in. If one is not provided, then {@link ShiftVarOrderFactory}
     *   is used by default. (not null)
     * @throws PcjException The PCJ table could not be create or the values from
     *   Rya were not able to be loaded into it.
     */
    public static void createAndPopulatePcj(
            final RepositoryConnection ryaConn,
            final Connector accumuloConn,
            final String pcjTableName,
            final String sparql,
            final String[] resultVariables,
            final Optional<PcjVarOrderFactory> pcjVarOrderFactory) throws PcjException {
        checkNotNull(ryaConn);
        checkNotNull(accumuloConn);
        checkNotNull(pcjTableName);
        checkNotNull(sparql);
        checkNotNull(resultVariables);
        checkNotNull(pcjVarOrderFactory);

        final PcjTables pcj = new PcjTables();
        // Create the PCJ's variable orders.
        final PcjVarOrderFactory varOrderFactory = pcjVarOrderFactory.or(new ShiftVarOrderFactory());
        final Set<VariableOrder> varOrders = varOrderFactory.makeVarOrders( new VariableOrder(resultVariables) );

        // Create the PCJ table in Accumulo.
        pcj.createPcjTable(accumuloConn, pcjTableName, varOrders, sparql);

        // Load historic matches from Rya into the PCJ table.
        populatePcj(accumuloConn, pcjTableName, ryaConn);
    }


    /**
     * Scan Rya for results that solve the PCJ's query and store them in the PCJ
     * table.
     * <p>
     * This method assumes the PCJ table has already been created.
     *
     * @param accumuloConn
     *            - A connection to the Accumulo that hosts the PCJ table. (not
     *            null)
     * @param pcjTableName
     *            - The name of the PCJ table that will receive the results.
     *            (not null)
     * @param ryaConn
     *            - A connection to the Rya store that will be queried to find
     *            results. (not null)
     * @throws PcjException
     *             If results could not be written to the PCJ table, the PCJ
     *             table does not exist, or the query that is being execute was
     *             malformed.
     */
    public static void populatePcj(final Connector accumuloConn,
            final String pcjTableName, final RepositoryConnection ryaConn)
                    throws PcjException {
        checkNotNull(accumuloConn);
        checkNotNull(pcjTableName);
        checkNotNull(ryaConn);

        try {
            // Fetch the query that needs to be executed from the PCJ table.
            final PcjMetadata pcjMetadata = new PcjTables().getPcjMetadata(
                    accumuloConn, pcjTableName);
            final String sparql = pcjMetadata.getSparql();

            // Query Rya for results to the SPARQL query.
            final TupleQuery query = ryaConn.prepareTupleQuery(QueryLanguage.SPARQL,
                    sparql);
            final TupleQueryResult results = query.evaluate();

            // Load batches of 1000 of them at a time into the PCJ table
            final Set<BindingSet> batch = new HashSet<>(1000);
            while (results.hasNext()) {
                batch.add(results.next());

                if (batch.size() == 1000) {
                    addResults(accumuloConn, pcjTableName, batch);
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                addResults(accumuloConn, pcjTableName, batch);
            }

        } catch (RepositoryException | MalformedQueryException
                | QueryEvaluationException e) {
            throw new PcjException(
                    "Could not populate a PCJ table with Rya results for the table named: "
                            + pcjTableName, e);
        }
    }

    public static void addResults(final Connector accumuloConn,
            final String pcjTableName, final Collection<BindingSet> results)
                    throws PcjException {
        checkNotNull(accumuloConn);
        checkNotNull(pcjTableName);
        checkNotNull(results);

        // Write a result to each of the variable orders that are in the table.
        writeResults(accumuloConn, pcjTableName, results);
    }

    /**
     * Add a collection of results to a specific PCJ table.
     *
     * @param accumuloConn
     *            - A connection to the Accumulo that hosts the PCJ table. (not
     *            null)
     * @param pcjTableName
     *            - The name of the PCJ table that will receive the results.
     *            (not null)
     * @param results
     *            - Binding sets that will be written to the PCJ table. (not
     *            null)
     * @throws PcjException
     *             The provided PCJ table doesn't exist, is missing the PCJ
     *             metadata, or the result could not be written to it.
     */
    private static void writeResults(final Connector accumuloConn,
            final String pcjTableName, final Collection<BindingSet> results)
                    throws PcjException {
        checkNotNull(accumuloConn);
        checkNotNull(pcjTableName);
        checkNotNull(results);

        // Fetch the variable orders from the PCJ table.
        final PcjMetadata metadata = new PcjTables().getPcjMetadata(accumuloConn,
                pcjTableName);

        // Write each result formatted using each of the variable orders.
        BatchWriter writer = null;
        try {
            writer = accumuloConn.createBatchWriter(pcjTableName,
                    new BatchWriterConfig());
            for (final BindingSet result : results) {
                final Set<Mutation> addResultMutations = makeWriteResultMutations(
                        metadata.getVarOrders(), result);
                writer.addMutations(addResultMutations);
            }
        } catch (TableNotFoundException | MutationsRejectedException e) {
            throw new PcjException(
                    "Could not add results to the PCJ table named: "
                            + pcjTableName, e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (final MutationsRejectedException e) {
                    throw new PcjException(
                            "Could not add results to a PCJ table because some of the mutations were rejected.",
                            e);
                }
            }
        }
    }

    /**
     * Create the {@link Mutations} required to write a new {@link BindingSet}
     * to a PCJ table for each {@link VariableOrder} that is provided.
     *
     * @param varOrders
     *            - The variables orders the result will be written to. (not
     *            null)
     * @param result
     *            - A new PCJ result. (not null)
     * @return Mutation that will write the result to a PCJ table.
     * @throws PcjException
     *             The binding set could not be encoded.
     */
    private static Set<Mutation> makeWriteResultMutations(
            final Set<VariableOrder> varOrders, final BindingSet result)
                    throws PcjException {
        checkNotNull(varOrders);
        checkNotNull(result);

        final Set<Mutation> mutations = new HashSet<>();

        for (final VariableOrder varOrder : varOrders) {
            try {
                // Serialize the result to the variable order.
                final byte[] serializedResult = converter.convert(result, varOrder);

                // Row ID = binding set values, Column Family = variable order
                // of the binding set.
                final Mutation addResult = new Mutation(serializedResult);
                addResult.put(varOrder.toString(), "", "");
                mutations.add(addResult);
            } catch (final BindingSetConversionException e) {
                throw new PcjException("Could not serialize a result.", e);
            }
        }

        return mutations;
    }

    //****************************Creation and Population of PcjTables Mongo ***********************************

    public static void deleteCoreRyaTables(final MongoClient client, final String instance, final String collName) {
        final boolean bool = client.isLocked();
        client.getDatabase(instance).getCollection(collName).drop();
    }

    /**
     * Creates a new PCJ Table in Accumulo and populates it by scanning an
     * instance of Rya for historic matches.
     * <p>
     * If any portion of this operation fails along the way, the partially
     * create PCJ table will be left in Accumulo.
     *
     * @param ryaConn - Connects to the Rya that will be scanned. (not null)
     * @param mongoClient - Connects to the mongoDB that hosts the PCJ results. (not null)
     * @param pcjName - The name of the PCJ table that will be created. (not null)
     * @param sparql - The SPARQL query whose results will be loaded into the table. (not null)
     * @throws PcjException The PCJ table could not be create or the values from Rya were
     *         not able to be loaded into it.
     */
    public static void createAndPopulatePcj(final RepositoryConnection ryaConn, final MongoClient mongoClient, final String pcjName, final String instanceName, final String sparql) throws PcjException {
        checkNotNull(ryaConn);
        checkNotNull(mongoClient);
        checkNotNull(pcjName);
        checkNotNull(instanceName);
        checkNotNull(sparql);

        final MongoPcjDocuments pcj = new MongoPcjDocuments(mongoClient, instanceName);

        pcj.createPcj(pcjName, sparql);

        // Load historic matches from Rya into the PCJ table.
        populatePcj(pcj, pcjName, ryaConn);
    }


    /**
     * Scan Rya for results that solve the PCJ's query and store them in the PCJ
     * table.
     * <p>
     * This method assumes the PCJ table has already been created.
     *
     * @param mongoClient - A connection to the mongoDB that hosts the PCJ table. (not null)
     * @param pcjTableName - The name of the PCJ table that will receive the results. (not null)
     * @param ryaConn - A connection to the Rya store that will be queried to find results. (not null)
     * @throws PcjException
     *             If results could not be written to the PCJ table, the PCJ
     *             table does not exist, or the query that is being execute was
     *             malformed.
     */
    public static void populatePcj(final MongoPcjDocuments pcj, final String pcjTableName, final RepositoryConnection ryaConn) throws PcjException {
        checkNotNull(pcj);
        checkNotNull(pcjTableName);
        checkNotNull(ryaConn);

        try {
            // Fetch the query that needs to be executed from the PCJ table.
            final PcjMetadata pcjMetadata = pcj.getPcjMetadata(pcjTableName);
            final String sparql = pcjMetadata.getSparql();

            // Query Rya for results to the SPARQL query.
            final TupleQuery query = ryaConn.prepareTupleQuery(QueryLanguage.SPARQL, sparql);
            final TupleQueryResult results = query.evaluate();

            // Load batches of 1000 of them at a time into the PCJ table
            final Set<BindingSet> batch = new HashSet<>(1000);
            while (results.hasNext()) {
                batch.add(results.next());

                if (batch.size() == 1000) {
                    writeResults(pcj, pcjTableName, batch);
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                writeResults(pcj, pcjTableName, batch);
            }

        } catch (RepositoryException | MalformedQueryException | QueryEvaluationException e) {
            throw new PcjException("Could not populate a PCJ table with Rya results for the table named: " + pcjTableName, e);
        }
    }

    /**
     * Add a collection of results to a specific PCJ table.
     *
     * @param accumuloConn - A connection to the Accumulo that hosts the PCJ table. (not null)
     * @param pcjTableName - The name of the PCJ table that will receive the results. (not null)
     * @param results - Binding sets that will be written to the PCJ table. (not null)
     * @throws PcjException The provided PCJ table doesn't exist, is missing the
     *   PCJ metadata, or the result could not be written to it.
     */
    private static void writeResults(final MongoPcjDocuments pcj,
            final String pcjTableName, final Collection<BindingSet> results)
                    throws PcjException {
        checkNotNull(pcj);
        checkNotNull(pcjTableName);
        checkNotNull(results);

        final Collection<VisibilityBindingSet> visRes = new ArrayList<>();
        results.forEach(bindingSet -> {
            visRes.add(new VisibilityBindingSet(bindingSet));
        });
        pcj.addResults(pcjTableName, visRes);
    }
}
