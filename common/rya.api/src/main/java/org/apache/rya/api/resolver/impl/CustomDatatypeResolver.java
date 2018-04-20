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
package org.apache.rya.api.resolver.impl;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.TYPE_DELIM_BYTE;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.TYPE_DELIM_BYTES;

import java.nio.charset.StandardCharsets;

import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.resolver.RyaTypeResolverException;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import com.google.common.primitives.Bytes;

/**
 * Date: 7/16/12
 * Time: 1:12 PM
 */
public class CustomDatatypeResolver extends RyaTypeResolverImpl {
    public static final int DT_LITERAL_MARKER = 8;

    public CustomDatatypeResolver() {
        super((byte) DT_LITERAL_MARKER, null);
    }

    @Override
    public byte[][] serializeType(final RyaType ryaType) throws RyaTypeResolverException {
        final byte[] bytes = serializeData(ryaType.getData()).getBytes(StandardCharsets.UTF_8);
        return new byte[][]{bytes, Bytes.concat(TYPE_DELIM_BYTES, ryaType.getDataType().stringValue().getBytes(StandardCharsets.UTF_8), TYPE_DELIM_BYTES, markerBytes)};
    }

    @Override
    public byte[] serialize(final RyaType ryaType) throws RyaTypeResolverException {
        final byte[][] bytes = serializeType(ryaType);
        return Bytes.concat(bytes[0], bytes[1]);
    }

    @Override
    public RyaType deserialize(final byte[] bytes) throws RyaTypeResolverException {
        if (!deserializable(bytes)) {
            throw new RyaTypeResolverException("Bytes not deserializable");
        }
        final RyaType rt = newInstance();
        final int length = bytes.length;
        final int indexOfType = Bytes.indexOf(bytes, TYPE_DELIM_BYTE);
        if (indexOfType < 1) {
            throw new RyaTypeResolverException("Not a datatype literal");
        }
        final String label = deserializeData(new String(bytes, 0, indexOfType, StandardCharsets.UTF_8));
        rt.setDataType(SimpleValueFactory.getInstance().createIRI(new String(bytes, indexOfType + 1, (length - indexOfType) - 3, StandardCharsets.UTF_8)));
        rt.setData(label);
        return rt;
    }
}
