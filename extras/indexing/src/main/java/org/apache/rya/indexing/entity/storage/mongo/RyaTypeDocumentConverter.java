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

import org.apache.rya.api.domain.RyaType;
import org.bson.Document;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Converts between {@link RyaType} and {@link Document}.
 */
@DefaultAnnotation(NonNull.class)
public class RyaTypeDocumentConverter implements DocumentConverter<RyaType> {

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    public static final String DATA_TYPE = "dataType";
    public static final String VALUE = "value";

    @Override
    public Document toDocument(final RyaType ryaType) {
        requireNonNull(ryaType);

        return new Document()
                .append(DATA_TYPE, ryaType.getDataType().toString())
                .append(VALUE, ryaType.getData());
    }

    @Override
    public RyaType fromDocument(final Document document) throws DocumentConverterException {
        requireNonNull(document);

        if(!document.containsKey(DATA_TYPE)) {
            throw new DocumentConverterException("Could not convert document '" + document +
                    "' because its '" + DATA_TYPE + "' field is missing.");
        }

        if(!document.containsKey(VALUE)) {
            throw new DocumentConverterException("Could not convert document '" + document +
                    "' because its '" + VALUE + "' field is missing.");
        }

        return new RyaType(
                VF.createIRI( document.getString(DATA_TYPE) ),
                document.getString(VALUE));
    }
}