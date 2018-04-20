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
package org.apache.rya.indexing.accumulo.freetext;

import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;


public class AccumuloFreeTextIndexerTest {
    private static final StatementConstraints EMPTY_CONSTRAINTS = new StatementConstraints();

    private AccumuloRdfConfiguration conf;

    @Before
    public void before() throws Exception {
        conf = new AccumuloRdfConfiguration();
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
        conf.set(ConfigUtils.CLOUDBASE_USER, "USERNAME");
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "PASS");
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, "U");
        conf.setClass(ConfigUtils.TOKENIZER_CLASS, SimpleTokenizer.class, Tokenizer.class);
        conf.setTablePrefix("triplestore_");
        
        // If a table exists from last time, delete it.
        List<String> tableNames = AccumuloFreeTextIndexer.getTableNames(conf);
        for (String name : tableNames) {
            destroyTable(conf, name);
        }
        // Tables are created in each test with setConf(conf)
    }

    private static void destroyTable(Configuration conf, String tablename) throws AccumuloException, AccumuloSecurityException,
            TableNotFoundException, TableExistsException {
        TableOperations tableOps = ConfigUtils.getConnector(conf).tableOperations();
        if (tableOps.exists(tablename)) {
            tableOps.delete(tablename);
        }
    }

    @Test
    public void testSearch() throws Exception {
        try (AccumuloFreeTextIndexer f = new AccumuloFreeTextIndexer()) {
            f.setConf(conf);
            f.setMultiTableBatchWriter(ConfigUtils.createMultitableBatchWriter(conf));
            f.init();

            ValueFactory vf = SimpleValueFactory.getInstance();

            IRI subject = vf.createIRI("foo:subj");
            IRI predicate = RDFS.LABEL;
            Value object = vf.createLiteral("this is a new hat");

            IRI context = vf.createIRI("foo:context");

            Statement statement = vf.createStatement(subject, predicate, object, context);
            f.storeStatement(RdfToRyaConversions.convertStatement(statement));
            f.flush();

            printTables(conf);

            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryText("asdf", EMPTY_CONSTRAINTS)));

            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryText("this & !is", EMPTY_CONSTRAINTS)));

            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryText("this", EMPTY_CONSTRAINTS)));
            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryText("is", EMPTY_CONSTRAINTS)));
            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryText("a", EMPTY_CONSTRAINTS)));
            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryText("new", EMPTY_CONSTRAINTS)));
            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryText("hat", EMPTY_CONSTRAINTS)));

            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryText("ha*", EMPTY_CONSTRAINTS)));
            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryText("*at", EMPTY_CONSTRAINTS)));

            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryText("hat & new", EMPTY_CONSTRAINTS)));

            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryText("this & hat & new", EMPTY_CONSTRAINTS)));

            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryText("bat", EMPTY_CONSTRAINTS)));
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryText("this & bat", EMPTY_CONSTRAINTS)));
        }
    }

    @Test
    public void testDelete() throws Exception {
        try (AccumuloFreeTextIndexer f = new AccumuloFreeTextIndexer()) {
            f.setConf(conf);
            f.setMultiTableBatchWriter(ConfigUtils.createMultitableBatchWriter(conf));
            f.init();

            ValueFactory vf = SimpleValueFactory.getInstance();

            IRI subject1 = vf.createIRI("foo:subj");
            IRI predicate1 = RDFS.LABEL;
            Value object1 = vf.createLiteral("this is a new hat");

            IRI context1 = vf.createIRI("foo:context");

            Statement statement1 = vf.createStatement(subject1, predicate1, object1, context1);
            f.storeStatement(RdfToRyaConversions.convertStatement(statement1));

            IRI subject2 = vf.createIRI("foo:subject");
            IRI predicate2 = RDFS.LABEL;
            Value object2 = vf.createLiteral("Do you like my new hat?");

            IRI context2 = vf.createIRI("foo:context");

            Statement statement2 = vf.createStatement(subject2, predicate2, object2, context2);
            f.storeStatement(RdfToRyaConversions.convertStatement(statement2));

            f.flush();


            System.out.println("testDelete: BEFORE DELETE");
            printTables(conf);

            f.deleteStatement(RdfToRyaConversions.convertStatement(statement1));
            System.out.println("testDelete: AFTER FIRST DELETION");
            printTables(conf);
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryText("this is a new hat", EMPTY_CONSTRAINTS)));
            Assert.assertEquals(Sets.newHashSet(statement2), getSet(f.queryText("Do you like my new hat?", EMPTY_CONSTRAINTS)));

            // Check that "new" didn't get deleted from the term table after "this is a new hat"
            // was deleted since "new" is still in "Do you like my new hat?"
            Assert.assertEquals(Sets.newHashSet(statement2), getSet(f.queryText("new", EMPTY_CONSTRAINTS)));

            f.deleteStatement(RdfToRyaConversions.convertStatement(statement2));
            System.out.println("testDelete: AFTER LAST DELETION");
            printTables(conf);

            System.out.println("testDelete: DONE");
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryText("this is a new hat", EMPTY_CONSTRAINTS)));
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryText("Do you like my new hat?", EMPTY_CONSTRAINTS)));
        }
    }

    @Test
    public void testRestrictPredicatesSearch() throws Exception {
        conf.setStrings(ConfigUtils.FREETEXT_PREDICATES_LIST, "pred:1,pred:2");

        try (AccumuloFreeTextIndexer f = new AccumuloFreeTextIndexer()) {
            f.setConf(conf);
            f.setMultiTableBatchWriter(ConfigUtils.createMultitableBatchWriter(conf));
            f.init();

            // These should not be stored because they are not in the predicate list
            f.storeStatement(new RyaStatement(new RyaIRI("foo:subj1"), new RyaIRI(RDFS.LABEL.toString()), new RyaType("invalid")));
            f.storeStatement(new RyaStatement(new RyaIRI("foo:subj2"), new RyaIRI(RDFS.COMMENT.toString()), new RyaType("invalid")));

            RyaIRI pred1 = new RyaIRI("pred:1");
            RyaIRI pred2 = new RyaIRI("pred:2");

            // These should be stored because they are in the predicate list
            RyaStatement s3 = new RyaStatement(new RyaIRI("foo:subj3"), pred1, new RyaType("valid"));
            RyaStatement s4 = new RyaStatement(new RyaIRI("foo:subj4"), pred2, new RyaType("valid"));
            f.storeStatement(s3);
            f.storeStatement(s4);

            // This should not be stored because the object is not a literal
            f.storeStatement(new RyaStatement(new RyaIRI("foo:subj5"), pred1, new RyaIRI("in:valid")));

            f.flush();

            printTables(conf);

            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryText("invalid", EMPTY_CONSTRAINTS)));
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryText("in:valid", EMPTY_CONSTRAINTS)));

            Set<Statement> actual = getSet(f.queryText("valid", EMPTY_CONSTRAINTS));
            Assert.assertEquals(2, actual.size());
            Assert.assertTrue(actual.contains(RyaToRdfConversions.convertStatement(s3)));
            Assert.assertTrue(actual.contains(RyaToRdfConversions.convertStatement(s4)));
        }
    }

    @Test
    public void testContextSearch() throws Exception {
        try (AccumuloFreeTextIndexer f = new AccumuloFreeTextIndexer()) {
            f.setConf(conf);
            f.setMultiTableBatchWriter(ConfigUtils.createMultitableBatchWriter(conf));
            f.init();

            ValueFactory vf = SimpleValueFactory.getInstance();
            IRI subject = vf.createIRI("foo:subj");
            IRI predicate = vf.createIRI(RDFS.COMMENT.toString());
            Value object = vf.createLiteral("this is a new hat");
            IRI context = vf.createIRI("foo:context");

            Statement statement = vf.createStatement(subject, predicate, object, context);
            f.storeStatement(RdfToRyaConversions.convertStatement(statement));
            f.flush();

            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryText("hat", EMPTY_CONSTRAINTS)));
            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryText("hat", new StatementConstraints().setContext(context))));
            Assert.assertEquals(Sets.newHashSet(),
                    getSet(f.queryText("hat", new StatementConstraints().setContext(vf.createIRI("foo:context2")))));
        }
    }

    public static void printTables(Configuration conf) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        TableOperations tops = ConfigUtils.getConnector(conf).tableOperations();

        // print tables
        String FORMAT = "%-20s  %-20s  %-40s  %-40s\n";
        for (String table : tops.list()) {
            System.out.println("Reading : " + table);
            System.out.format(FORMAT, "--Row--", "--ColumnFamily--", "--ColumnQualifier--", "--Value--");
            Scanner s = ConfigUtils.getConnector(conf).createScanner(table, Authorizations.EMPTY);
            for (Entry<Key, org.apache.accumulo.core.data.Value> entry : s) {
                Key k = entry.getKey();
                System.out.format(FORMAT, k.getRow(), k.getColumnFamily(), k.getColumnQualifier(), entry.getValue());
            }
            System.out.println();
        }

    }

    private static <X> Set<X> getSet(CloseableIteration<X, ?> iter) throws Exception {
        Set<X> set = new HashSet<X>();
        while (iter.hasNext()) {
            set.add(iter.next());
        }
        return set;
    }
}
