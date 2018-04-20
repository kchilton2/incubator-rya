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
package org.apache.rya.rdftriplestore.inference;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.eclipse.rdf4j.common.iteration.Iterations;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Test;

import junit.framework.TestCase;

public class SameAsTest extends TestCase {
    private String user = "user";
    private String pwd = "pwd";
    private String instance = "myinstance";
    private String tablePrefix = "t_";
    private Authorizations auths = Constants.NO_AUTHS;
    private Connector connector;
    private AccumuloRyaDAO ryaDAO;
    private static final ValueFactory VF = SimpleValueFactory.getInstance();
    private String namespace = "urn:test#";
    private AccumuloRdfConfiguration conf;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        connector = new MockInstance(instance).getConnector(user, pwd.getBytes());
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
        SecurityOperations secOps = connector.securityOperations();
        secOps.createUser(user, pwd.getBytes(), auths);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX, TablePermission.READ);

        conf = new AccumuloRdfConfiguration();
        ryaDAO = new AccumuloRyaDAO();
        ryaDAO.setConnector(connector);
        conf.setTablePrefix(tablePrefix);
        ryaDAO.setConf(conf);
        ryaDAO.init();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
    }

    @Test
    //This isn't a good test.  It's simply a cut-and-paste from a test that was failing in a different package in the SameAsVisitor.
    public void testGraphConfiguration() throws Exception {
        IRI a = VF.createIRI(namespace, "a");
        Statement statement = VF.createStatement(a, VF.createIRI(namespace, "p"), VF.createLiteral("l"));
        Statement statement2 = VF.createStatement(a, VF.createIRI(namespace, "p2"), VF.createLiteral("l"));
        ryaDAO.add(RdfToRyaConversions.convertStatement(statement));
        ryaDAO.add(RdfToRyaConversions.convertStatement(statement2));
        ryaDAO.add(RdfToRyaConversions.convertStatement(VF.createStatement(VF.createIRI(namespace, "b"), VF.createIRI(namespace, "p"), VF.createLiteral("l"))));
        ryaDAO.add(RdfToRyaConversions.convertStatement(VF.createStatement(VF.createIRI(namespace, "c"), VF.createIRI(namespace, "n"), VF.createLiteral("l"))));

        // build a connection
        RdfCloudTripleStore store = new RdfCloudTripleStore();
        store.setConf(conf);
        store.setRyaDAO(ryaDAO);

        InferenceEngine inferenceEngine = new InferenceEngine();
        inferenceEngine.setRyaDAO(ryaDAO);
        store.setInferenceEngine(inferenceEngine);
        
        store.initialize();

        System.out.println(Iterations.asList(store.getConnection().getStatements(a, VF.createIRI(namespace, "p"), VF.createLiteral("l"), false, new Resource[0])).size());
    }
}
