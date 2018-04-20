package org.apache.rya.indexing.mongodb.geo;
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

import static org.apache.rya.indexing.mongodb.geo.GeoMongoDBStorageStrategy.GeoQueryType.EQUALS;
import static org.apache.rya.indexing.mongodb.geo.GeoMongoDBStorageStrategy.GeoQueryType.INTERSECTS;
import static org.apache.rya.indexing.mongodb.geo.GeoMongoDBStorageStrategy.GeoQueryType.NEAR;
import static org.apache.rya.indexing.mongodb.geo.GeoMongoDBStorageStrategy.GeoQueryType.WITHIN;

import org.apache.log4j.Logger;
import org.apache.rya.indexing.GeoIndexer;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.geo.GeoTupleSet.GeoSearchFunctionFactory.NearQuery;
import org.apache.rya.indexing.mongodb.AbstractMongoIndexer;
import org.apache.rya.indexing.mongodb.geo.GeoMongoDBStorageStrategy.GeoQuery;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;

import com.mongodb.DBObject;
import com.vividsolutions.jts.geom.Geometry;

public class MongoGeoIndexer extends AbstractMongoIndexer<GeoMongoDBStorageStrategy> implements GeoIndexer {
    private static final String COLLECTION_SUFFIX = "geo";
    private static final Logger logger = Logger.getLogger(MongoGeoIndexer.class);

    @Override
	public void init() {
        initCore();
        predicates = ConfigUtils.getGeoPredicates(conf);
        if(predicates.size() == 0) {
            logger.debug("No predicates specified for geo indexing.  During insertion, all statements will be attempted to be indexed into the geo indexer.");
        }
        storageStrategy = new GeoMongoDBStorageStrategy(Double.valueOf(conf.get(MongoDBRdfConfiguration.MONGO_GEO_MAXDISTANCE, "1e-10")));
        storageStrategy.createIndices(collection);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryEquals(
            final Geometry query, final StatementConstraints constraints) {
        try {
            final DBObject queryObj = storageStrategy.getQuery(new GeoQuery(EQUALS, query));
            return withConstraints(constraints, queryObj);
        } catch (final MalformedQueryException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryDisjoint(
            final Geometry query, final StatementConstraints constraints) {
        throw new UnsupportedOperationException(
                "Disjoint queries are not supported in Mongo DB.");
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryIntersects(
            final Geometry query, final StatementConstraints constraints) {
        try {
            final DBObject queryObj = storageStrategy.getQuery(new GeoQuery(INTERSECTS, query));
            return withConstraints(constraints, queryObj);
        } catch (final MalformedQueryException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryTouches(
            final Geometry query, final StatementConstraints constraints) {
        throw new UnsupportedOperationException(
                "Touches queries are not supported in Mongo DB.");
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryCrosses(
            final Geometry query, final StatementConstraints constraints) {
        throw new UnsupportedOperationException(
                "Crosses queries are not supported in Mongo DB.");
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryWithin(
            final Geometry query, final StatementConstraints constraints) {
        try {
            final DBObject queryObj = storageStrategy.getQuery(new GeoQuery(WITHIN, query));
            return withConstraints(constraints, queryObj);
        } catch (final MalformedQueryException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryNear(final NearQuery query, final StatementConstraints constraints) {
        double maxDistance = 0;
        double minDistance = 0;
        if (query.getMaxDistance().isPresent()) {
            maxDistance = query.getMaxDistance().get();
        }

        if (query.getMinDistance().isPresent()) {
            minDistance = query.getMinDistance().get();
        }

        try {
            final DBObject queryObj = storageStrategy.getQuery(new GeoQuery(NEAR, query.getGeometry(), maxDistance, minDistance));
            return withConstraints(constraints, queryObj);
        } catch (final MalformedQueryException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryContains(
            final Geometry query, final StatementConstraints constraints) {
        throw new UnsupportedOperationException(
                "Contains queries are not supported in Mongo DB.");
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryOverlaps(
            final Geometry query, final StatementConstraints constraints) {
        throw new UnsupportedOperationException(
                "Overlaps queries are not supported in Mongo DB.");
    }

    @Override
    public String getCollectionName() {
        return ConfigUtils.getTablePrefix(conf)  + COLLECTION_SUFFIX;
    }
}