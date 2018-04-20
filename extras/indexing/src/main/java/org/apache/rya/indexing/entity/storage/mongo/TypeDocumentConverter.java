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
package org.apache.rya.indexing.entity.storage.mongo;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.indexing.entity.model.Type;
import org.bson.Document;

import com.google.common.collect.ImmutableSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Converts between {@link Type} and {@link Document}.
 */
@DefaultAnnotation(NonNull.class)
public class TypeDocumentConverter implements DocumentConverter<Type> {

    public static final String ID = "_id";
    public static final String PROPERTY_NAMES = "propertyNames";

    @Override
    public Document toDocument(final Type type) {
        requireNonNull(type);

        final Document doc = new Document();
        doc.append(ID, type.getId().getData());

        final List<String> propertyNames = new ArrayList<>();
        type.getPropertyNames().forEach(field -> propertyNames.add(field.getData()));
        doc.append(PROPERTY_NAMES, propertyNames);

        return doc;
    }

    @Override
    public Type fromDocument(final Document document) throws DocumentConverterException {
        requireNonNull(document);

        if(!document.containsKey(ID)) {
            throw new DocumentConverterException("Could not convert document '" + document +
                    "' because its '" + ID + "' field is missing.");
        }

        if(!document.containsKey(PROPERTY_NAMES)) {
            throw new DocumentConverterException("Could not convert document '" + document +
                    "' because its '" + PROPERTY_NAMES + "' field is missing.");
        }

        final RyaIRI typeId = new RyaIRI( document.getString(ID) );

        final ImmutableSet.Builder<RyaIRI> propertyNames = ImmutableSet.builder();
        ((List<String>) document.get(PROPERTY_NAMES))
            .forEach(propertyName -> propertyNames.add(new RyaIRI(propertyName)));

        return new Type(typeId, propertyNames.build());
    }
}