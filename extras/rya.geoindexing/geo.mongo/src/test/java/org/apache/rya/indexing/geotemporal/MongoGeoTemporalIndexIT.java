/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.geotemporal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.GeoRyaSailFactory;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.accumulo.geo.OptionalConfigUtils;
import org.apache.rya.indexing.geotemporal.model.Event;
import org.apache.rya.indexing.geotemporal.mongo.MongoGeoTemporalIndexer;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoITBase;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.Test;

public class MongoGeoTemporalIndexIT extends MongoITBase {
    private static final String URI_PROPERTY_AT_TIME = "Property:atTime";

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    @Override
    public void updateConfiguration(final MongoDBRdfConfiguration conf) {
        conf.setBoolean(OptionalConfigUtils.USE_GEOTEMPORAL, true);
    }

    @Test
    public void ensureInEventStore_Test() throws Exception {
        final Sail sail = GeoRyaSailFactory.getInstance(conf);
        final SailRepository repo = new SailRepository(sail);
        try(final MongoGeoTemporalIndexer indexer = new MongoGeoTemporalIndexer()) {
            indexer.setConf(conf);
            indexer.init();

            addStatements(repo.getConnection());
            final EventStorage events = indexer.getEventStorage();
            final RyaIRI subject = new RyaIRI("urn:event1");
            final Optional<Event> event = events.get(subject);
            assertTrue(event.isPresent());
        } finally {
            sail.shutDown();
        }
    }

    @Test
    public void constantSubjQuery_Test() throws Exception {
        final Sail sail = GeoRyaSailFactory.getInstance(conf);
        final SailRepositoryConnection conn = new SailRepository(sail).getConnection();

        try {
            addStatements(conn);

            final String query =
                    "PREFIX time: <http://www.w3.org/2006/time#> \n"
                            + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"
                            + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
                            + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>"
                            + "SELECT * "
                            + "WHERE { "
                            + "  <urn:event1> time:atTime ?time . "
                            + "  <urn:event1> geo:asWKT ?point . "
                            + "  FILTER(geof:sfWithin(?point, \"POLYGON((-3 -2, -3 2, 1 2, 1 -2, -3 -2))\"^^geo:wktLiteral)) "
                            + "  FILTER(tempo:equals(?time, \"2015-12-30T12:00:00Z\")) "
                            + "}";

            final TupleQueryResult rez = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
            final Set<BindingSet> results = new HashSet<>();
            while(rez.hasNext()) {
                final BindingSet bs = rez.next();
                results.add(bs);
            }
            final MapBindingSet expected = new MapBindingSet();
            expected.addBinding("point", VF.createLiteral("POINT (0 0)"));
            expected.addBinding("time", VF.createLiteral("2015-12-30T12:00:00Z"));

            assertEquals(1, results.size());
            assertEquals(expected, results.iterator().next());
        } finally {
            conn.close();
            sail.shutDown();
        }
    }

    @Test
    public void variableSubjQuery_Test() throws Exception {
        final Sail sail = GeoRyaSailFactory.getInstance(conf);
        final SailRepositoryConnection conn = new SailRepository(sail).getConnection();

        try {
            addStatements(conn);

            final String query =
                    "PREFIX time: <http://www.w3.org/2006/time#> \n"
                            + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"
                            + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
                            + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>"
                            + "SELECT * "
                            + "WHERE { "
                            + "  ?subj time:atTime ?time . "
                            + "  ?subj geo:asWKT ?point . "
                            + "  FILTER(geof:sfWithin(?point, \"POLYGON((-3 -2, -3 2, 1 2, 1 -2, -3 -2))\"^^geo:wktLiteral)) "
                            + "  FILTER(tempo:equals(?time, \"2015-12-30T12:00:00Z\")) "
                            + "}";

            final TupleQueryResult rez = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
            final List<BindingSet> results = new ArrayList<>();
            while(rez.hasNext()) {
                final BindingSet bs = rez.next();
                results.add(bs);
            }
            final MapBindingSet expected1 = new MapBindingSet();
            expected1.addBinding("point", VF.createLiteral("POINT (0 0)"));
            expected1.addBinding("time", VF.createLiteral("2015-12-30T12:00:00Z"));

            final MapBindingSet expected2 = new MapBindingSet();
            expected2.addBinding("point", VF.createLiteral("POINT (1 1)"));
            expected2.addBinding("time", VF.createLiteral("2015-12-30T12:00:00Z"));

            assertEquals(2, results.size());
            assertEquals(expected1, results.get(0));
            assertEquals(expected2, results.get(1));
        } finally {
            conn.close();
            sail.shutDown();
        }
    }

    private void addStatements(final SailRepositoryConnection conn) throws Exception {
        IRI subject = VF.createIRI("urn:event1");
        final IRI predicate = VF.createIRI(URI_PROPERTY_AT_TIME);
        Value object = VF.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString());
        conn.add(VF.createStatement(subject, predicate, object));

        object = VF.createLiteral("Point(0 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
        conn.add(VF.createStatement(subject, GeoConstants.GEO_AS_WKT, object));

        subject = VF.createIRI("urn:event2");
        object = VF.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString());
        conn.add(VF.createStatement(subject, predicate, object));

        object = VF.createLiteral("Point(1 1)", GeoConstants.XMLSCHEMA_OGC_WKT);
        conn.add(VF.createStatement(subject, GeoConstants.GEO_AS_WKT, object));
        conn.commit();
    }
}
