/*
l * Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.rya.indexing.geotemporal.mongo;

import static org.apache.rya.indexing.geotemporal.GeoTemporalTestUtils.assertEqualMongo;
import static org.apache.rya.indexing.geotemporal.GeoTemporalTestUtils.getFilters;
import static org.apache.rya.indexing.geotemporal.GeoTemporalTestUtils.getSps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.IndexingExpr;
import org.apache.rya.indexing.IndexingFunctionRegistry;
import org.apache.rya.indexing.IndexingFunctionRegistry.FUNCTION_TYPE;
import org.apache.rya.indexing.geotemporal.GeoTemporalIndexer.GeoPolicy;
import org.apache.rya.indexing.geotemporal.GeoTemporalIndexer.TemporalPolicy;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Tests The {@link GeoTemporalMongoDBStorageStrategy}, which turns the filters
 * into mongo {@link DBObject}s used to query.
 *
 * This tests also ensures all possible filter functions are accounted for in the test.
 * @see TemporalPolicy Temporal Filter Functions
 * @see GeoPolicy Geo Filter Functions
 */
public class GeoTemporalMongoDBStorageStrategyTest {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private GeoTemporalMongoDBStorageStrategy adapter;
    @Before
    public void setup() {
        adapter = new GeoTemporalMongoDBStorageStrategy();
    }

    @Test
    public void emptyFilters_test() throws Exception {
        final List<IndexingExpr> geoFilters = new ArrayList<>();
        final List<IndexingExpr> temporalFilters = new ArrayList<>();
        final DBObject actual = adapter.getFilterQuery(geoFilters, temporalFilters);
        final String expectedString =
                "{ }";
        final DBObject expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);
    }

    @Test
    public void equalsInstantAfterInterval_onlyOneGeo() throws Exception {
        final String query =
          "PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
        + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>"
        + "SELECT ?point ?wkt "
        + "WHERE { "
          + "  ?point geo:asWKT ?wkt . "
          + "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-3 -2, -3 2, 1 2, 1 -2, -3 -2))\"^^geo:wktLiteral)) "
        + "}";
        final List<IndexingExpr> geoFilters = new ArrayList<>();
        final List<StatementPattern> sps = getSps(query);
        final List<FunctionCall> filters = getFilters(query);
        for(final FunctionCall filter : filters) {
            //should only be one.
            final Var objVar = IndexingFunctionRegistry.getResultVarFromFunctionCall(VF.createIRI(filter.getURI()), filter.getArgs());
            final Value[] arguments = extractArguments(objVar.getName(), filter);
            final IndexingExpr expr = new IndexingExpr(VF.createIRI(filter.getURI()), sps.get(0), Arrays.stream(arguments).toArray());
            geoFilters.add(expr);
        }
        final List<IndexingExpr> temporalFilters = new ArrayList<>();
        final DBObject actual = adapter.getFilterQuery(geoFilters, temporalFilters);
        final String expectedString =
            "{ "
            + "\"location\" : { "
              + "\"$geoWithin\" : { "
                + "\"$geometry\" : { "
                  + "\"coordinates\" : [ [ [ -3.0 , -2.0] , [ -3.0 , 2.0] , [ 1.0 , 2.0] , [ 1.0 , -2.0] , [ -3.0 , -2.0]]] , "
                  + "\"type\" : \"Polygon\""
                + "}"
              + "}"
            + "}"
          + "}";
        final DBObject expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);
    }

    @Test
    public void equalsInstantAfterInterval_onlyGeos() throws Exception {

        /*
         * TODO: change filter functions for coverage
         */


        final String query =
                "PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
              + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>"
              + "SELECT ?point ?wkt "
              + "WHERE { "
                + "  ?point geo:asWKT ?wkt . "
                + "  FILTER(geof:sfIntersects(?wkt, \"POLYGON((-3 -2, -3 2, 1 2, 1 -2, -3 -2))\"^^geo:wktLiteral)) "
                + "  FILTER(geof:sfEquals(?wkt, \"POLYGON((-4 -3, -4 3, 2 3, 2 -3, -4 -3))\"^^geo:wktLiteral)) "
              + "}";
              final List<IndexingExpr> geoFilters = new ArrayList<>();
              final List<StatementPattern> sps = getSps(query);
              final List<FunctionCall> filters = getFilters(query);
              for(final FunctionCall filter : filters) {
                  final Var objVar = IndexingFunctionRegistry.getResultVarFromFunctionCall(VF.createIRI(filter.getURI()), filter.getArgs());
                  final Value[] arguments = extractArguments(objVar.getName(), filter);
                  final IndexingExpr expr = new IndexingExpr(VF.createIRI(filter.getURI()), sps.get(0), Arrays.stream(arguments).toArray());
                  geoFilters.add(expr);
              }
              final List<IndexingExpr> temporalFilters = new ArrayList<>();
              final DBObject actual = adapter.getFilterQuery(geoFilters, temporalFilters);

              final String expectedString =
                  "{ "
                  + "\"$and\" : [ { "
                    + "\"location\" : {"
                      + " \"coordinates\" : [ [ [ -4.0 , -3.0] , [ -4.0 , 3.0] , [ 2.0 , 3.0] , [ 2.0 , -3.0] , [ -4.0 , -3.0]]] ,"
                      + " \"type\" : \"Polygon\""
                    + "}"
                  + "} , { "
                  + "\"location\" : { "
                    + "\"$geoIntersects\" : {"
                      + " \"$geometry\" : {"
                        + " \"coordinates\" : [ [ [ -3.0 , -2.0] , [ -3.0 , 2.0] , [ 1.0 , 2.0] , [ 1.0 , -2.0] , [ -3.0 , -2.0]]] ,"
                        + " \"type\" : \"Polygon\""
                      + "}"
                    + "}"
                  + "}"
                + "}]}";
              final DBObject expected = (DBObject) JSON.parse(expectedString);
              assertEqualMongo(expected, actual);
    }

    @Test
    public void equalsInstantAfterInterval_onlyOneTemporal() throws Exception {
        final String query =
          "PREFIX time: <http://www.w3.org/2006/time#> \n"
        + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"
        + "SELECT ?event ?time "
        + "WHERE { "
          + "  ?event time:atTime ?time . "
          + "  FILTER(tempo:equals(?time, \"2015-12-30T12:00:00Z\")) . "
        + "}";
        final List<IndexingExpr> geoFilters = new ArrayList<>();
        final List<IndexingExpr> temporalFilters = new ArrayList<>();
        final List<StatementPattern> sps = getSps(query);
        final List<FunctionCall> filters = getFilters(query);
        for(final FunctionCall filter : filters) {
            //should only be one.
            final Var objVar = IndexingFunctionRegistry.getResultVarFromFunctionCall(VF.createIRI(filter.getURI()), filter.getArgs());
            final Value[] arguments = extractArguments(objVar.getName(), filter);
            final IndexingExpr expr = new IndexingExpr(VF.createIRI(filter.getURI()), sps.get(0), Arrays.stream(arguments).toArray());
            temporalFilters.add(expr);
        }
        final DBObject actual = adapter.getFilterQuery(geoFilters, temporalFilters);
        final String expectedString =
        "{ "
        + "\"instant\" : {"
          + "\"$date\" : \"2015-12-30T12:00:00.000Z\""
        + "}"
      + "}";
        final DBObject expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);
    }

    @Test
    public void equalsInstantAfterInterval_onlyTemporal() throws Exception {
        final String query =
                "PREFIX time: <http://www.w3.org/2006/time#> \n"
              + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"
              + "SELECT ?event ?time "
              + "WHERE { "
                + "  ?event time:atTime ?time . "
                + "  FILTER(tempo:before(?time, \"2015-12-30T12:00:00Z\")) . "
                + "  FILTER(tempo:insideInterval(?time, \"[1969-12-31T19:00:00-05:00,1969-12-31T19:00:01-05:00]\")) . "
              + "}";
              final List<IndexingExpr> geoFilters = new ArrayList<>();
              final List<IndexingExpr> temporalFilters = new ArrayList<>();
              final List<StatementPattern> sps = getSps(query);
              final List<FunctionCall> filters = getFilters(query);
              for(final FunctionCall filter : filters) {
                  final Var objVar = IndexingFunctionRegistry.getResultVarFromFunctionCall(VF.createIRI(filter.getURI()), filter.getArgs());
                  final Value[] arguments = extractArguments(objVar.getName(), filter);
                  final IndexingExpr expr = new IndexingExpr(VF.createIRI(filter.getURI()), sps.get(0), Arrays.stream(arguments).toArray());
                  temporalFilters.add(expr);
              }
              final DBObject actual = adapter.getFilterQuery(geoFilters, temporalFilters);
              final String expectedString =
              "{ "
              + "\"$and\" : [{"
                + "\"instant\" : {"
                  + "\"$gt\" : {"
                    + "\"$date\" : \"1970-01-01T00:00:00.000Z\""
                  + "},"
                  + "\"$lt\" : {"
                    + "\"$date\" : \"1970-01-01T00:00:01.000Z\""
                  + "},"
                + "}}, {"
                + "\"instant\" : {"
                  + "\"$lt\" : {"
                    + "\"$date\" : \"2015-12-30T12:00:00.000Z\""
                  + "}"
                + "}"
              + "}]"
            + "}";
              final DBObject expected = (DBObject) JSON.parse(expectedString);
              assertEqualMongo(expected, actual);
    }

    @Test
    public void equalsInstantAfterInterval_GeoTemporalOneEach() throws Exception {
        final String query =
                "PREFIX time: <http://www.w3.org/2006/time#> \n"
              + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"
              + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
              + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>"
              + "SELECT ?event ?time ?point ?wkt "
              + "WHERE { "
                + "  ?event time:atTime ?time . "
                + "  ?point geo:asWKT ?wkt . "
                + "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-3 -2, -3 2, 1 2, 1 -2, -3 -2))\"^^geo:wktLiteral)) "
                + "  FILTER(tempo:after(?time, \"2015-12-30T12:00:00Z\")) "
              + "}";
              final List<IndexingExpr> geoFilters = new ArrayList<>();
              final List<IndexingExpr> temporalFilters = new ArrayList<>();
              final List<StatementPattern> sps = getSps(query);
              final List<FunctionCall> filters = getFilters(query);
              for(final FunctionCall filter : filters) {
                  final IRI filterURI = VF.createIRI(filter.getURI());
                  final Var objVar = IndexingFunctionRegistry.getResultVarFromFunctionCall(filterURI, filter.getArgs());
                  final Value[] arguments = extractArguments(objVar.getName(), filter);
                  final IndexingExpr expr = new IndexingExpr(filterURI, sps.get(0), Arrays.stream(arguments).toArray());
                  if(IndexingFunctionRegistry.getFunctionType(filterURI) == FUNCTION_TYPE.GEO) {
                      geoFilters.add(expr);
                  } else {
                      temporalFilters.add(expr);
                  }
              }
              final DBObject actual = adapter.getFilterQuery(geoFilters, temporalFilters);
              final String expectedString =
              "{ "
              + "\"$and\" : [ { "
                + "\"location\" : { "
                  + "\"$geoWithin\" : { "
                    + "\"$geometry\" : { "
                      + "\"coordinates\" : [ [ [ -3.0 , -2.0] , [ -3.0 , 2.0] , [ 1.0 , 2.0] , [ 1.0 , -2.0] , [ -3.0 , -2.0]]] , "
                      + "\"type\" : \"Polygon\""
                    + "}"
                  + "}"
                + "}"
              + "} , { "
                + "\"instant\" : { "
                  + "\"$gt\" : { "
                    + "\"$date\" : \"2015-12-30T12:00:00.000Z\""
                  + "}"
                + "}"
              + "}]"
            + "}";
              final DBObject expected = (DBObject) JSON.parse(expectedString);
              assertEqualMongo(expected, actual);
    }

    @Test
    public void equalsInstantAfterInterval_GeoTemporalTwoEach() throws Exception {
        final String query =
                "PREFIX time: <http://www.w3.org/2006/time#> \n"
              + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"
              + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
              + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>"
              + "SELECT ?event ?time ?point ?wkt "
              + "WHERE { "
                + "  ?event time:atTime ?time . "
                + "  ?point geo:asWKT ?wkt . "
                + "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-3 -2, -3 2, 1 2, 1 -2, -3 -2))\"^^geo:wktLiteral)) "
                + "  FILTER(geof:sfEquals(?wkt, \"POLYGON((-4 -3, -4 3, 2 3, 2 -3, -4 -3))\"^^geo:wktLiteral)) "
                + "  FILTER(tempo:hasEndInterval(?time, \"[1969-12-31T19:00:00-05:00,1969-12-31T19:00:01-05:00]\")) . "
                + "  FILTER(tempo:beforeInterval(?time, \"[1969-12-31T19:00:00-05:00,1969-12-31T19:00:01-05:00]\")) . "
              + "}";
              final List<IndexingExpr> geoFilters = new ArrayList<>();
              final List<IndexingExpr> temporalFilters = new ArrayList<>();
              final List<StatementPattern> sps = getSps(query);
              final List<FunctionCall> filters = getFilters(query);
              for(final FunctionCall filter : filters) {
                  final IRI filterIRI = VF.createIRI(filter.getURI());
                  final Var objVar = IndexingFunctionRegistry.getResultVarFromFunctionCall(filterIRI, filter.getArgs());
                  final Value[] arguments = extractArguments(objVar.getName(), filter);
                  final IndexingExpr expr = new IndexingExpr(filterIRI, sps.get(0), Arrays.stream(arguments).toArray());
                  if(IndexingFunctionRegistry.getFunctionType(filterIRI) == FUNCTION_TYPE.GEO) {
                      geoFilters.add(expr);
                  } else {
                      temporalFilters.add(expr);
                  }
              }
              final DBObject actual = adapter.getFilterQuery(geoFilters, temporalFilters);
              final String expectedString =
                  "{ "
                  + "\"$and\" : [ { "
                    + "\"$and\" : [ { "
                      + "\"location\" : { "
                        + "\"coordinates\" : [ [ [ -4.0 , -3.0] , [ -4.0 , 3.0] , [ 2.0 , 3.0] , [ 2.0 , -3.0] , [ -4.0 , -3.0]]] , "
                        + "\"type\" : \"Polygon\""
                      + "}"
                    + "} , { "
                      + "\"location\" : { "
                        + "\"$geoWithin\" : { "
                          + "\"$geometry\" : { "
                            + "\"coordinates\" : [ [ [ -3.0 , -2.0] , [ -3.0 , 2.0] , [ 1.0 , 2.0] , [ 1.0 , -2.0] , [ -3.0 , -2.0]]] , "
                            + "\"type\" : \"Polygon\""
                         + "}"
                       + "}"
                     + "}"
                   + "}]"
                 + "} , { "
                   + "\"$and\" : [ { "
                     + "\"instant\" : { "
                       + "\"$lt\" : { "
                         + "\"$date\" : \"1970-01-01T00:00:00.000Z\""
                       + "}"
                     + "}"
                   + "} , { "
                     + "\"instant\" : { "
                       + "\"$date\" : \"1970-01-01T00:00:01.000Z\""
                     + "}"
                   + "}]"
                 + "}]"
               + "}";
              final DBObject expected = (DBObject) JSON.parse(expectedString);
              assertEqualMongo(expected, actual);
    }

    @Test
    public void equalsInstantAfterInterval_GeoTemporalSingleGeoTwoTemporal() throws Exception {
        final String query =
                "PREFIX time: <http://www.w3.org/2006/time#> \n"
              + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"
              + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
              + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>"
              + "SELECT ?event ?time ?point ?wkt "
              + "WHERE { "
                + "  ?event time:atTime ?time . "
                + "  ?point geo:asWKT ?wkt . "
                + "  FILTER(geof:sfEquals(?wkt, \"POLYGON((-4 -3, -4 3, 2 3, 2 -3, -4 -3))\"^^geo:wktLiteral)) ."
                + "  FILTER(tempo:hasBeginningInterval(?time, \"[1969-12-31T19:00:00-05:00,1969-12-31T19:00:01-05:00]\")) . "
                + "  FILTER(tempo:afterInterval(?time, \"[1969-12-31T19:00:00-05:00,1969-12-31T19:00:01-05:00]\"))"
              + "}";
        final List<IndexingExpr> geoFilters = new ArrayList<>();
        final List<IndexingExpr> temporalFilters = new ArrayList<>();
        final List<StatementPattern> sps = getSps(query);
        final List<FunctionCall> filters = getFilters(query);
        for(final FunctionCall filter : filters) {
            final IRI filterURI = VF.createIRI(filter.getURI());
            final Var objVar = IndexingFunctionRegistry.getResultVarFromFunctionCall(filterURI, filter.getArgs());
            final Value[] arguments = extractArguments(objVar.getName(), filter);
            final IndexingExpr expr = new IndexingExpr(filterURI, sps.get(0), Arrays.stream(arguments).toArray());
            if(IndexingFunctionRegistry.getFunctionType(filterURI) == FUNCTION_TYPE.GEO) {
                geoFilters.add(expr);
             } else {
                temporalFilters.add(expr);
             }
        }
        final DBObject actual = adapter.getFilterQuery(geoFilters, temporalFilters);
        final String expectedString =
            "{ "
            + "\"$and\" : [ { "
              + "\"location\" : { "
                + "\"coordinates\" : [ [ [ -4.0 , -3.0] , [ -4.0 , 3.0] , [ 2.0 , 3.0] , [ 2.0 , -3.0] , [ -4.0 , -3.0]]] , "
                + "\"type\" : \"Polygon\""
              + "}"
            + "} , { "
              + "\"$and\" : [ { "
                + "\"instant\" : { "
                  + "\"$gt\" : { "
                    + "\"$date\" : \"1970-01-01T00:00:01.000Z\""
                  + "}"
                + "}"
              + "} , { "
                + "\"instant\" : { "
                  + "\"$date\" : \"1970-01-01T00:00:00.000Z\""
                + "}"
              + "}]"
            + "}]"
          + "}";
        final DBObject expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);
    }

    @Test
    public void serializeTest() {
        final Resource subject = VF.createIRI("foo:subj");
        final Resource context = VF.createIRI("foo:context");

        //GEO
        IRI predicate = GeoConstants.GEO_AS_WKT;
        Value object = VF.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);

        Statement statement = VF.createStatement(subject, predicate, object, context);
        DBObject actual = adapter.serialize(RdfToRyaConversions.convertStatement(statement));
        String expectedString =
            "{ "
            + "\"_id\" : -852305321 , "
            + "\"location\" : { "
              + "\"coordinates\" : [ -77.03524 , 38.889468] , "
              + "\"type\" : \"Point\""
            + "}"
          + "}";
        DBObject expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);

        //TIME INSTANT
        predicate = VF.createIRI("Property:event:time");
        object = VF.createLiteral("2015-12-30T12:00:00Z");
        statement = VF.createStatement(subject, predicate, object, context);
        actual = adapter.serialize(RdfToRyaConversions.convertStatement(statement));
        expectedString =
                "{"
                  +"_id : -852305321, "
                  +"time: {"
                    + "instant : {"
                      +"\"$date\" : \"2015-12-30T12:00:00.000Z\""
                    + "}"
                + "}"
              + "}";
        expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);

        //TIME INTERVAL
        predicate = VF.createIRI("Property:circa");
        object = VF.createLiteral("[1969-12-31T19:00:00-05:00,1969-12-31T19:00:01-05:00]");
        statement = VF.createStatement(subject, predicate, object, context);
        actual = adapter.serialize(RdfToRyaConversions.convertStatement(statement));
        expectedString =
                "{"
                +"_id : -852305321, "
                +"time: {"
                  + "start : {"
                    +"\"$date\" : \"1970-01-01T00:00:00.000Z\""
                  + "},"
                  + "end : {"
                    +"\"$date\" : \"1970-01-01T00:00:01.000Z\""
                  + "}"
              + "}"
            + "}";
        expected = (DBObject) JSON.parse(expectedString);
        assertEqualMongo(expected, actual);
    }

    private static Value[] extractArguments(final String matchName, final FunctionCall call) {
        final Value args[] = new Value[call.getArgs().size() - 1];
        int argI = 0;
        for (int i = 0; i != call.getArgs().size(); ++i) {
            final ValueExpr arg = call.getArgs().get(i);
            if (argI == i && arg instanceof Var && matchName.equals(((Var)arg).getName())) {
                continue;
            }
            if (arg instanceof ValueConstant) {
                args[argI] = ((ValueConstant)arg).getValue();
            } else if (arg instanceof Var && ((Var)arg).hasValue()) {
                args[argI] = ((Var)arg).getValue();
            } else {
                throw new IllegalArgumentException("Query error: Found " + arg + ", expected a Literal, BNode or URI");
            }
            ++argI;
        }
        return args;
    }
}
