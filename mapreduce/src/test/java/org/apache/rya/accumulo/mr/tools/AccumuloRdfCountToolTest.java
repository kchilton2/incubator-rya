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
package org.apache.rya.accumulo.mr.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.io.Text;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/24/12
 * Time: 5:05 PM
 * To change this template use File | Settings | File Templates.
 */
//@Ignore
public class AccumuloRdfCountToolTest {

    private String user = "user";
    private String pwd = "pwd";
    private String instance = AccumuloRdfCountToolTest.class.getSimpleName() + ".myinstance";
    private String tablePrefix = "t_";
    private Authorizations auths = Constants.NO_AUTHS;
    private Connector connector;

    private AccumuloRyaDAO dao;
    private static final ValueFactory VF = SimpleValueFactory.getInstance();
    private AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
    static String litdupsNS = "urn:test:litdups#";

    @Before
    public void setUp() throws Exception {
        connector = new MockInstance(instance).getConnector(user, pwd.getBytes());
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX);
        SecurityOperations secOps = connector.securityOperations();
        secOps.createUser(user, pwd.getBytes(), auths);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX, TablePermission.WRITE);

        dao = new AccumuloRyaDAO();
        dao.setConnector(connector);
        conf.setTablePrefix(tablePrefix);
        dao.setConf(conf);
        dao.init();
    }

    @After
    public void tearDown() throws Exception {
        dao.destroy();
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX);
    }

    @Test
    public void testMR() throws Exception {
        RyaIRI test1 = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "test1"));
        RyaIRI pred1 = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "pred1"));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(0))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(1))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(2))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(3))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(4))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(5))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(6))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(7))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(8))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(9))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(10))));

        AccumuloRdfCountTool.main(new String[]{
                "-Dac.mock=true",
                "-Dac.instance=" + instance,
                "-Dac.username=" + user,
                "-Dac.pwd=" + pwd,
                "-Drdf.tablePrefix=" + tablePrefix,
        });

        Map<String, Key> expectedValues = new HashMap<String, Key>();
        String row = test1.getData();
        expectedValues.put(row,
                new Key(new Text(row),
                        RdfCloudTripleStoreConstants.SUBJECT_CF_TXT,
                        RdfCloudTripleStoreConstants.EMPTY_TEXT));
        row = pred1.getData();
        expectedValues.put(row,
                new Key(new Text(row),
                        RdfCloudTripleStoreConstants.PRED_CF_TXT,
                        RdfCloudTripleStoreConstants.EMPTY_TEXT));
        Scanner scanner = connector.createScanner(tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX, auths);
        scanner.setRange(new Range());
        int count = 0;
        for (Map.Entry<Key, Value> entry : scanner) {
            assertTrue(expectedValues.get(entry.getKey().getRow().toString()).equals(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL));
            assertEquals(11, Long.parseLong(entry.getValue().toString()));
            count++;
        }
        assertEquals(2, count);
    }

//    public void testMRObject() throws Exception {
//        IRI pred1 = VF.createIRI(litdupsNS, "pred1");
//        Literal literal = VF.createLiteral(0);
//        dao.add(VF.createStatement(VF.createIRI(litdupsNS, "test0"), pred1, literal));
//        dao.add(VF.createStatement(VF.createIRI(litdupsNS, "test1"), pred1, literal));
//        dao.add(VF.createStatement(VF.createIRI(litdupsNS, "test2"), pred1, literal));
//        dao.add(VF.createStatement(VF.createIRI(litdupsNS, "test3"), pred1, literal));
//        dao.add(VF.createStatement(VF.createIRI(litdupsNS, "test4"), pred1, literal));
//        dao.add(VF.createStatement(VF.createIRI(litdupsNS, "test5"), pred1, literal));
//        dao.add(VF.createStatement(VF.createIRI(litdupsNS, "test6"), pred1, literal));
//        dao.add(VF.createStatement(VF.createIRI(litdupsNS, "test7"), pred1, literal));
//        dao.add(VF.createStatement(VF.createIRI(litdupsNS, "test8"), pred1, literal));
//        dao.add(VF.createStatement(VF.createIRI(litdupsNS, "test9"), pred1, literal));
//        dao.add(VF.createStatement(VF.createIRI(litdupsNS, "test10"), pred1, literal));
//        dao.commit();
//
//        AccumuloRdfCountTool.main(new String[]{
//                "-Dac.mock=true",
//                "-Dac.instance=" + instance,
//                "-Dac.username=" + user,
//                "-Dac.pwd=" + pwd,
//                "-Drdf.tablePrefix=" + tablePrefix,
//        });
//
//        Map<String, Key> expectedValues = new HashMap<String, Key>();
//        byte[] row_bytes = RdfCloudTripleStoreUtils.writeValue(literal);
//        expectedValues.put(new String(row_bytes),
//                new Key(new Text(row_bytes),
//                        RdfCloudTripleStoreConstants.OBJ_CF_TXT,
//                        RdfCloudTripleStoreConstants.INFO_TXT));
//        row_bytes = RdfCloudTripleStoreUtils.writeValue(pred1);
//        expectedValues.put(new String(row_bytes),
//                new Key(new Text(row_bytes),
//                        RdfCloudTripleStoreConstants.PRED_CF_TXT,
//                        RdfCloudTripleStoreConstants.INFO_TXT));
//        Scanner scanner = connector.createScanner(tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX, auths);
//        scanner.setRange(new Range());
//        int count = 0;
//        for (Map.Entry<Key, Value> entry : scanner) {
//            assertTrue(expectedValues.get(entry.getKey().getRow().toString()).equals(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL));
//            assertEquals(11, Long.parseLong(entry.getValue().toString()));
//            count++;
//        }
//        assertEquals(2, count);
//    }

    @Test
    public void testTTL() throws Exception {
        RyaIRI test1 = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "test1"));
        RyaIRI pred1 = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "pred1"));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(0))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(1))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(2))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(3))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(4))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(5))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(6))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(7))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(8))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(9))));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(10))));

        AccumuloRdfCountTool.main(new String[]{
                "-Dac.mock=true",
                "-Dac.instance=" + instance,
                "-Dac.username=" + user,
                "-Dac.pwd=" + pwd,
                "-Dac.ttl=0",
                "-Drdf.tablePrefix=" + tablePrefix,
        });

        Scanner scanner = connector.createScanner(tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX, auths);
        scanner.setRange(new Range());
        int count = 0;
        for (Map.Entry<Key, Value> entry : scanner) {
            count++;
        }
        assertEquals(0, count);
    }

    @Test
    public void testContext() throws Exception {
        RyaIRI test1 = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "test1"));
        RyaIRI pred1 = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "pred1"));
        RyaIRI cntxt = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "cntxt"));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(0)), cntxt));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(1)), cntxt));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(2)), cntxt));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(3)), cntxt));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(4)), cntxt));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(5)), cntxt));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(6)), cntxt));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(7)), cntxt));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(8)), cntxt));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(9)), cntxt));
        dao.add(new RyaStatement(test1, pred1, RdfToRyaConversions.convertLiteral(VF.createLiteral(10)), cntxt));

        AccumuloRdfCountTool.main(new String[]{
                "-Dac.mock=true",
                "-Dac.instance=" + instance,
                "-Dac.username=" + user,
                "-Dac.pwd=" + pwd,
                "-Drdf.tablePrefix=" + tablePrefix,
        });

        Map<String, Key> expectedValues = new HashMap<String, Key>();
        String row = test1.getData();
        expectedValues.put(row,
                new Key(new Text(row),
                        RdfCloudTripleStoreConstants.SUBJECT_CF_TXT,
                        new Text(cntxt.getData())));
        row = pred1.getData();
        expectedValues.put(row,
                new Key(new Text(row),
                        RdfCloudTripleStoreConstants.PRED_CF_TXT,
                        new Text(cntxt.getData())));
        Scanner scanner = connector.createScanner(tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX, auths);
        scanner.setRange(new Range());
        int count = 0;
        for (Map.Entry<Key, Value> entry : scanner) {
            assertTrue(expectedValues.get(entry.getKey().getRow().toString()).equals(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL));
            assertEquals(11, Long.parseLong(entry.getValue().toString()));
            count++;
        }
        assertEquals(2, count);
    }
}
