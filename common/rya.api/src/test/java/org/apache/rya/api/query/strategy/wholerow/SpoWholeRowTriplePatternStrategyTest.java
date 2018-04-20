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

//package org.apache.rya.api.query.strategy.wholerow;

//
//import junit.framework.TestCase;
//import org.apache.rya.api.RdfCloudTripleStoreConstants;
//import org.apache.rya.api.domain.*;
//import org.apache.rya.api.resolver.RyaContext;
//import org.apache.rya.api.resolver.triple.TripleRow;
//import org.apache.accumulo.core.data.Key;
//import org.apache.accumulo.core.data.Range;
//import org.eclipse.rdf4j.model.ValueFactory;
//import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
//import org.apache.hadoop.io.Text;
//
//import java.util.Map;
//
///**
// * Date: 7/14/12
// * Time: 7:47 AM
// */
//public class SpoWholeRowTriplePatternStrategyTest extends TestCase {
//    private static final ValueFactory VF = SimpleValueFactory.getInstance();
//
//    RyaIRI uri = new RyaIRI("urn:test#1234");
//    RyaIRI uri2 = new RyaIRI("urn:test#1235");
//    RyaIRIRange rangeIRI = new RyaIRIRange(uri, uri2);
//    RyaIRIRange rangeIRI2 = new RyaIRIRange(new RyaIRI("urn:test#1235"), new RyaIRI("urn:test#1236"));
//    SpoWholeRowTriplePatternStrategy strategy = new SpoWholeRowTriplePatternStrategy();
//    RyaContext ryaContext = RyaContext.getInstance();
//
//    RyaType customType1 = new RyaType(VF.createIRI("urn:custom#type"), "1234");
//    RyaType customType2 = new RyaType(VF.createIRI("urn:custom#type"), "1235");
//    RyaType customType3 = new RyaType(VF.createIRI("urn:custom#type"), "1236");
//    RyaTypeRange customTypeRange1 = new RyaTypeRange(customType1, customType2);
//    RyaTypeRange customTypeRange2 = new RyaTypeRange(customType2, customType3);
//
//    public void testSpo() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, uri, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(uri, uri, uri, null, null);
//        assertTrue(entry.getValue().contains(key));
//
//        entry = strategy.defineRange(uri, uri, uri2, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testSpoCustomType() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, customType1, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(uri, uri, customType1, null, null);
//        assertTrue(entry.getValue().contains(key));
//
//        entry = strategy.defineRange(uri, uri, customType2, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testSpoRange() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, uri, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(uri, uri, rangeIRI, null, null);
//        assertTrue(entry.getValue().contains(key));
//
//        entry = strategy.defineRange(uri, uri, rangeIRI2, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testSpoRangeCustomType() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, customType1, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(uri, uri, customTypeRange1, null, null);
//        assertTrue(entry.getValue().contains(key));
//
//        entry = strategy.defineRange(uri, uri, customTypeRange2, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testSp() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, uri, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(uri, uri, null, null, null);
//        assertTrue(entry.getValue().contains(key));
//        entry = strategy.defineRange(uri, uri2, null, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testSpRange() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, uri, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(uri, rangeIRI, null, null, null);
//        assertTrue(entry.getValue().contains(key));
//        entry = strategy.defineRange(uri, rangeIRI2, null, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testS() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, uri, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(uri, null, null, null, null);
//        assertTrue(entry.getValue().contains(key));
//
//        entry = strategy.defineRange(uri2, null, null, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testSRange() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, uri, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(rangeIRI, null, null, null, null);
//        assertTrue(entry.getValue().contains(key));
//
//        entry = strategy.defineRange(rangeIRI2, null, null, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testHandles() throws Exception {
//        //spo(ng)
//        assertTrue(strategy.handles(uri, uri, uri, null));
//        assertTrue(strategy.handles(uri, uri, uri, uri));
//        //sp(ng)
//        assertTrue(strategy.handles(uri, uri, null, null));
//        assertTrue(strategy.handles(uri, uri, null, uri));
//        //s(ng)
//        assertTrue(strategy.handles(uri, null, null, null));
//        assertTrue(strategy.handles(uri, null, null, uri));
//        //sp_r(o)(ng)
//        assertTrue(strategy.handles(uri, uri, rangeIRI, null));
//        assertTrue(strategy.handles(uri, uri, rangeIRI, uri));
//        //s_r(p)(ng)
//        assertTrue(strategy.handles(uri, rangeIRI, null, null));
//        assertTrue(strategy.handles(uri, rangeIRI, null, uri));
//        //r(s)
//        assertTrue(strategy.handles(rangeIRI, null, null, null));
//
//        //fail
//        //s_r(p)_r(o)
//        assertFalse(strategy.handles(uri, rangeIRI, rangeIRI, null));
//
//        //s==null
//        assertFalse(strategy.handles(null, uri, uri, null));
//
//        //s_r(o)
//        assertFalse(strategy.handles(uri, null, rangeIRI, null));
//    }
//}
