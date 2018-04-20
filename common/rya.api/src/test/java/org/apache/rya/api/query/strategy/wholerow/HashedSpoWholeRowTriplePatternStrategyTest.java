package org.apache.rya.api.query.strategy.wholerow;

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

import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaTypeRange;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaIRIRange;
import org.apache.rya.api.query.strategy.ByteRange;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Before;

import junit.framework.TestCase;

/**
 * Date: 7/14/12
 * Time: 7:47 AM
 */
public class HashedSpoWholeRowTriplePatternStrategyTest extends TestCase {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    RyaIRI uri = new RyaIRI("urn:test#1234");
    RyaIRI uri2 = new RyaIRI("urn:test#1235");
    RyaIRIRange rangeIRI = new RyaIRIRange(uri, uri2);
    RyaIRIRange rangeIRI2 = new RyaIRIRange(new RyaIRI("urn:test#1235"), new RyaIRI("urn:test#1236"));
    HashedSpoWholeRowTriplePatternStrategy strategy = new HashedSpoWholeRowTriplePatternStrategy();
    RyaContext ryaContext = RyaContext.getInstance();
    RyaTripleContext ryaTripleContext;

    RyaType customType1 = new RyaType(VF.createIRI("urn:custom#type"), "1234");
    RyaType customType2 = new RyaType(VF.createIRI("urn:custom#type"), "1235");
    RyaType customType3 = new RyaType(VF.createIRI("urn:custom#type"), "1236");
    RyaTypeRange customTypeRange1 = new RyaTypeRange(customType1, customType2);
    RyaTypeRange customTypeRange2 = new RyaTypeRange(customType2, customType3);

    @Before
    public void setUp() {
    	MockRdfConfiguration config = new MockRdfConfiguration();
    	config.set(MockRdfConfiguration.CONF_PREFIX_ROW_WITH_HASH, Boolean.TRUE.toString());
    	ryaTripleContext = RyaTripleContext.getInstance(config);
    }
    
    public void testSpo() throws Exception {
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaTripleContext.serializeTriple(
                new RyaStatement(uri, uri, uri, null));
        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);

        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry = strategy.defineRange(uri, uri, uri, null, null);
         assertContains(entry.getValue(), tripleRow.getRow());
        

        entry = strategy.defineRange(uri, uri, uri2, null, null);
        assertContainsFalse(entry.getValue(), tripleRow.getRow());
    }

	private void assertContains(ByteRange value, byte[] row) {
	       Text rowText = new Text(row);
	        Text startText = new Text(value.getStart());
	        Text endText = new Text(value.getEnd());
	        assertTrue((startText.compareTo(rowText) <= 0) &&(endText.compareTo(rowText) >= 0)) ;
	}

	private void assertContainsFalse(ByteRange value, byte[] row) {
	       Text rowText = new Text(row);
	        Text startText = new Text(value.getStart());
	        Text endText = new Text(value.getEnd());
	        assertFalse((startText.compareTo(rowText) <= 0) &&(endText.compareTo(rowText) >= 0)) ;
	}

    public void testSpoCustomType() throws Exception {
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaTripleContext.serializeTriple(
                new RyaStatement(uri, uri, customType1, null));
        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);

        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry = strategy.defineRange(uri, uri, customType1, null, null);
        assertContains(entry.getValue(), tripleRow.getRow());

        entry = strategy.defineRange(uri, uri, customType2, null, null);
        assertContainsFalse(entry.getValue(), tripleRow.getRow());
    }

    public void testSpoRange() throws Exception {
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaTripleContext.serializeTriple(
                new RyaStatement(uri, uri, uri, null));
        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);

        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry = strategy.defineRange(uri, uri, rangeIRI, null, null);
        assertContains(entry.getValue(), tripleRow.getRow());

        entry = strategy.defineRange(uri, uri, rangeIRI2, null, null);
        assertContainsFalse(entry.getValue(), tripleRow.getRow());
    }

    public void testSpoRangeCustomType() throws Exception {
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaTripleContext.serializeTriple(
                new RyaStatement(uri, uri, customType1, null));
        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);

        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry = strategy.defineRange(uri, uri, customTypeRange1, null, null);
        assertContains(entry.getValue(), tripleRow.getRow());

        entry = strategy.defineRange(uri, uri, customTypeRange2, null, null);
        assertContainsFalse(entry.getValue(), tripleRow.getRow());
    }

    public void testSp() throws Exception {
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaTripleContext.serializeTriple(
                new RyaStatement(uri, uri, uri, null));
        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);

        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry = strategy.defineRange(uri, uri, null, null, null);
        assertContains(entry.getValue(), tripleRow.getRow());
        entry = strategy.defineRange(uri, uri2, null, null, null);
        assertContainsFalse(entry.getValue(), tripleRow.getRow());
    }

    public void testSpRange() throws Exception {
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaTripleContext.serializeTriple(
                new RyaStatement(uri, uri, uri, null));
        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);

        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry = strategy.defineRange(uri, rangeIRI, null, null, null);
        assertContains(entry.getValue(), tripleRow.getRow());
        entry = strategy.defineRange(uri, rangeIRI2, null, null, null);
        assertContainsFalse(entry.getValue(), tripleRow.getRow());
    }

    public void testS() throws Exception {
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaTripleContext.serializeTriple(
                new RyaStatement(uri, uri, uri, null));
        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);
 
        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry = strategy.defineRange(uri, null, null, null, null);
        assertContains(entry.getValue(), tripleRow.getRow());

        entry = strategy.defineRange(uri2, null, null, null, null);
        assertContainsFalse(entry.getValue(), tripleRow.getRow());
    }

    public void testSRange() throws Exception {
 
        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry = strategy.defineRange(rangeIRI, null, null, null, null);
        assertNull(entry);
    }

    public void testHandles() throws Exception {
        //spo(ng)
        assertTrue(strategy.handles(uri, uri, uri, null));
        assertTrue(strategy.handles(uri, uri, uri, uri));
        //sp(ng)
        assertTrue(strategy.handles(uri, uri, null, null));
        assertTrue(strategy.handles(uri, uri, null, uri));
        //s(ng)
        assertTrue(strategy.handles(uri, null, null, null));
        assertTrue(strategy.handles(uri, null, null, uri));
        //sp_r(o)(ng)
        assertTrue(strategy.handles(uri, uri, rangeIRI, null));
        assertTrue(strategy.handles(uri, uri, rangeIRI, uri));
        //s_r(p)(ng)
        assertTrue(strategy.handles(uri, rangeIRI, null, null));
        assertTrue(strategy.handles(uri, rangeIRI, null, uri));

        //fail
        //s_r(p)_r(o)
        assertFalse(strategy.handles(uri, rangeIRI, rangeIRI, null));

        //s==null
        assertFalse(strategy.handles(null, uri, uri, null));

        //s_r(o)
        assertFalse(strategy.handles(uri, null, rangeIRI, null));
    }
}
