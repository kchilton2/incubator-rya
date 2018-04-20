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
package org.apache.rya.mongodb.dao;

import static org.eclipse.rdf4j.model.vocabulary.XMLSchema.ANYURI;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.api.persist.query.RyaQuery;
import org.apache.rya.mongodb.document.visibility.DocumentVisibility;
import org.apache.rya.mongodb.document.visibility.DocumentVisibilityAdapter;
import org.apache.rya.mongodb.document.visibility.DocumentVisibilityAdapter.MalformedDocumentVisibilityException;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Defines how {@link RyaStatement}s are stored in MongoDB.
 */
public class SimpleMongoDBStorageStrategy implements MongoDBStorageStrategy<RyaStatement> {
    private static final Logger LOG = Logger.getLogger(SimpleMongoDBStorageStrategy.class);

    public static final String ID = "_id";
    public static final String OBJECT_TYPE = "objectType";
    public static final String OBJECT_TYPE_VALUE = XMLSchema.ANYURI.stringValue();
    public static final String CONTEXT = "context";
    public static final String PREDICATE = "predicate";
    public static final String PREDICATE_HASH = "predicate_hash";
    public static final String OBJECT = "object";
    public static final String OBJECT_HASH = "object_hash";
    public static final String SUBJECT = "subject";
    public static final String SUBJECT_HASH = "subject_hash";
    public static final String TIMESTAMP = "insertTimestamp";
    public static final String STATEMENT_METADATA = "statementMetadata";
    public static final String DOCUMENT_VISIBILITY = "documentVisibility";

    /**
     * Generate the hash that will be used to index and retrieve a given value.
     * @param value  A value to be stored or accessed (e.g. a IRI or literal).
     * @return the hash associated with that value in MongoDB.
     */
    public static String hash(String value) {
        return DigestUtils.sha256Hex(value);
    }

    protected SimpleValueFactory factory = SimpleValueFactory.getInstance();

    @Override
    public void createIndices(final DBCollection coll){
        BasicDBObject doc = new BasicDBObject();
        doc.put(SUBJECT_HASH, 1);
        doc.put(PREDICATE_HASH, 1);
        doc.put(OBJECT_HASH, 1);
        doc.put(OBJECT_TYPE, 1);
        coll.createIndex(doc);
        doc = new BasicDBObject(PREDICATE_HASH, 1);
        doc.put(OBJECT_HASH, 1);
        doc.put(OBJECT_TYPE, 1);
        coll.createIndex(doc);
        doc = new BasicDBObject(OBJECT_HASH, 1);
        doc.put(OBJECT_TYPE, 1);
        doc.put(SUBJECT_HASH, 1);
        coll.createIndex(doc);
    }

    @Override
    public DBObject getQuery(final RyaStatement stmt) {
        final RyaIRI subject = stmt.getSubject();
        final RyaIRI predicate = stmt.getPredicate();
        final RyaType object = stmt.getObject();
        final RyaIRI context = stmt.getContext();
        final BasicDBObject query = new BasicDBObject();
        if (subject != null){
            query.append(SUBJECT_HASH, hash(subject.getData()));
        }
        if (object != null){
            query.append(OBJECT_HASH, hash(object.getData()));
            query.append(OBJECT_TYPE, object.getDataType().toString());
        }
        if (predicate != null){
            query.append(PREDICATE_HASH, hash(predicate.getData()));
        }
        if (context != null){
            query.append(CONTEXT, context.getData());
        }
        return query;
    }

    @Override
    public RyaStatement deserializeDBObject(final DBObject queryResult) {
        final Map<?, ?> result = queryResult.toMap();
        final String subject = (String) result.get(SUBJECT);
        final String object = (String) result.get(OBJECT);
        final String objectType = (String) result.get(OBJECT_TYPE);
        final String predicate = (String) result.get(PREDICATE);
        final String context = (String) result.get(CONTEXT);
        DocumentVisibility documentVisibility = null;
        try {
            documentVisibility = DocumentVisibilityAdapter.toDocumentVisibility(queryResult);
        } catch (final MalformedDocumentVisibilityException e) {
            throw new RuntimeException("Unable to convert document visibility", e);
        }
        final Long timestamp = (Long) result.get(TIMESTAMP);
        final String statementMetadata = (String) result.get(STATEMENT_METADATA);
        RyaType objectRya = null;
        if (objectType.equalsIgnoreCase(ANYURI.stringValue())){
            objectRya = new RyaIRI(object);
        }
        else {
            objectRya = new RyaType(factory.createIRI(objectType), object);
        }

        final RyaStatement statement;
        if (!context.isEmpty()){
            statement = new RyaStatement(new RyaIRI(subject), new RyaIRI(predicate), objectRya,
                    new RyaIRI(context));
        } else {
            statement = new RyaStatement(new RyaIRI(subject), new RyaIRI(predicate), objectRya);
        }

        statement.setColumnVisibility(documentVisibility.flatten());
        if(timestamp != null) {
            statement.setTimestamp(timestamp);
        }
        if(statementMetadata != null) {
            try {
                final StatementMetadata metadata = new StatementMetadata(statementMetadata);
                statement.setStatementMetadata(metadata);
            }
            catch (final Exception ex){
                LOG.debug("Error deserializing metadata for statement", ex);
            }
        }
        return statement;
    }

    @Override
    public DBObject serialize(final RyaStatement statement){
        return serializeInternal(statement);
    }

    public BasicDBObject serializeInternal(final RyaStatement statement){
        String context = "";
        if (statement.getContext() != null){
            context = statement.getContext().getData();
        }
        final String id = statement.getSubject().getData() + " " +
                statement.getPredicate().getData() + " " +  statement.getObject().getData() + " " + context;
        byte[] bytes = id.getBytes(StandardCharsets.UTF_8);
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-1");
            bytes = digest.digest(bytes);
        } catch (final NoSuchAlgorithmException e) {
            LOG.error("Unable to perform SHA-1 on the ID, defaulting to raw bytes.", e);
        }
        if (statement.getMetadata() == null){
            statement.setStatementMetadata(StatementMetadata.EMPTY_METADATA);
        }
        final BasicDBObject dvObject = DocumentVisibilityAdapter.toDBObject(statement.getColumnVisibility());
        final BasicDBObject doc = new BasicDBObject(ID, new String(Hex.encodeHex(bytes)))
        .append(SUBJECT, statement.getSubject().getData())
        .append(SUBJECT_HASH, hash(statement.getSubject().getData()))
        .append(PREDICATE, statement.getPredicate().getData())
        .append(PREDICATE_HASH, hash(statement.getPredicate().getData()))
        .append(OBJECT, statement.getObject().getData())
        .append(OBJECT_HASH, hash(statement.getObject().getData()))
        .append(OBJECT_TYPE, statement.getObject().getDataType().toString())
        .append(CONTEXT, context)
        .append(STATEMENT_METADATA, statement.getMetadata().toString())
        .append(DOCUMENT_VISIBILITY, dvObject.get(DOCUMENT_VISIBILITY))
        .append(TIMESTAMP, statement.getTimestamp());
        return doc;
    }

    @Override
    public DBObject getQuery(final RyaQuery ryaQuery) {
        return getQuery(ryaQuery.getQuery());
    }
}
