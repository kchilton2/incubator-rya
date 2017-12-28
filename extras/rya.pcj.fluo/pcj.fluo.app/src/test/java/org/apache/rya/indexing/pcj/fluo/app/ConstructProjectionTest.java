package org.apache.rya.indexing.pcj.fluo.app;
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
import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.junit.Test;
import org.openrdf.model.BNode;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class ConstructProjectionTest {

    private static final ValueFactory vf = new ValueFactoryImpl();
    
    @Test
    public void testConstructProjectionProjectSubj() throws MalformedQueryException, UnsupportedEncodingException {
        String query = "select ?x where { ?x <uri:talksTo> <uri:Bob> }";
        
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());
        ConstructProjection projection = new ConstructProjection(patterns.get(0));
        
        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("x", vf.createURI("uri:Joe"));
        VisibilityBindingSet vBs = new VisibilityBindingSet(bs, "FOUO");
        RyaStatement statement = projection.projectBindingSet(vBs, new HashMap<>());
        
        RyaStatement expected = new RyaStatement(new RyaURI("uri:Joe"), new RyaURI("uri:talksTo"), new RyaURI("uri:Bob"));
        expected.setColumnVisibility("FOUO".getBytes("UTF-8"));
        expected.setTimestamp(statement.getTimestamp());
        
        assertEquals(expected, statement);
    }
    
    @Test
    public void testConstructProjectionProjPred() throws MalformedQueryException {
        String query = "select ?p where { <uri:Joe> ?p <uri:Bob> }";
        
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());
        ConstructProjection projection = new ConstructProjection(patterns.get(0));
        
        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("p", vf.createURI("uri:worksWith"));
        VisibilityBindingSet vBs = new VisibilityBindingSet(bs);
        RyaStatement statement = projection.projectBindingSet(vBs, new HashMap<>());
        
        RyaStatement expected = new RyaStatement(new RyaURI("uri:Joe"), new RyaURI("uri:worksWith"), new RyaURI("uri:Bob"));
        expected.setTimestamp(statement.getTimestamp());
        expected.setColumnVisibility(new byte[0]);
        
        assertEquals(expected, statement);
    }
    
    @Test
    public void testConstructProjectionBNodes() throws MalformedQueryException {
        String query = "select ?o where { _:b <uri:talksTo> ?o }";
        
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());
        ConstructProjection projection = new ConstructProjection(patterns.get(0));
        
        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("o", vf.createURI("uri:Bob"));
        VisibilityBindingSet vBs = new VisibilityBindingSet(bs);
        BNode bNode = vf.createBNode();
        Map<String, BNode> bNodeMap = new HashMap<>();
        bNodeMap.put("-anon-1", bNode);
        RyaStatement statement = projection.projectBindingSet(vBs,bNodeMap);
        
        RyaStatement expected = new RyaStatement(RdfToRyaConversions.convertResource(bNode), new RyaURI("uri:talksTo"), new RyaURI("uri:Bob"));
        expected.setTimestamp(statement.getTimestamp());
        expected.setColumnVisibility(new byte[0]);
        
        assertEquals(expected, statement);
    }
    
}
