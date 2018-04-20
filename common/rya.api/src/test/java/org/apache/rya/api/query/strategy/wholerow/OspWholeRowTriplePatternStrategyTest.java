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
// * Time: 11:46 AM
// */
//public class OspWholeRowTriplePatternStrategyTest extends TestCase {
//    private static final ValueFactory VF = SimpleValueFactory.getInstance();
//
//    RyaIRI uri = new RyaIRI("urn:test#1234");
//    RyaIRI uri2 = new RyaIRI("urn:test#1235");
//    RyaIRIRange rangeIRI = new RyaIRIRange(uri, uri2);
//    RyaIRIRange rangeIRI2 = new RyaIRIRange(new RyaIRI("urn:test#1235"), new RyaIRI("urn:test#1236"));
//
//    RyaType customType1 = new RyaType(VF.createIRI("urn:custom#type"), "1234");
//    RyaType customType2 = new RyaType(VF.createIRI("urn:custom#type"), "1235");
//    RyaType customType3 = new RyaType(VF.createIRI("urn:custom#type"), "1236");
//    RyaTypeRange customTypeRange1 = new RyaTypeRange(customType1, customType2);
//    RyaTypeRange customTypeRange2 = new RyaTypeRange(customType2, customType3);
//
//    OspWholeRowTriplePatternStrategy strategy = new OspWholeRowTriplePatternStrategy();
//    RyaContext ryaContext = RyaContext.getInstance();
//
//    public void testO() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, uri, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP);
//        Key key = new Key(new Text(tripleRow.getRow()));
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(null, null, uri, null, null);
//        assertTrue(entry.getValue().contains(key));
//
//        entry = strategy.defineRange(null, null, uri2, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testORange() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, uri, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(null, null, rangeIRI, null, null);
//        assertTrue(entry.getValue().contains(key));
//
//        entry = strategy.defineRange(null, null, rangeIRI2, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testOs() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, uri, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(uri, null, uri, null, null);
//        assertTrue(entry.getValue().contains(key));
//
//        entry = strategy.defineRange(uri2, null, uri, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testOsRange() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, uri, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(rangeIRI, null, uri, null, null);
//        assertTrue(entry.getValue().contains(key));
//
//        entry = strategy.defineRange(rangeIRI2, null, uri, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testOsRangeCustomType() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, customType1, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(rangeIRI, null, customType1, null, null);
//        assertTrue(entry.getValue().contains(key));
//
//        entry = strategy.defineRange(rangeIRI2, null, customType2, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testHandles() throws Exception {
//        //os(ng)
//        assertTrue(strategy.handles(uri, null, uri, null));
//        assertTrue(strategy.handles(uri, null, uri, uri));
//        //o_r(s)(ng)
//        assertTrue(strategy.handles(rangeIRI, null, uri, null));
//        assertTrue(strategy.handles(rangeIRI, null, uri, uri));
//        //o(ng)
//        assertTrue(strategy.handles(null, null, uri, null));
//        assertTrue(strategy.handles(null, null, uri, uri));
//        //r(o)
//        assertTrue(strategy.handles(null, null, rangeIRI, null));
//        assertTrue(strategy.handles(null, null, rangeIRI, uri));
//
//        //false
//        assertFalse(strategy.handles(uri, null, rangeIRI, null));
//    }
//}
