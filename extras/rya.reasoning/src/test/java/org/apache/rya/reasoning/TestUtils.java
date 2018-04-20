package org.apache.rya.reasoning;

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

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaIRI;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

public class TestUtils {
    private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();
    public static final String TEST_PREFIX = "http://test.test";
    public static final IRI NODE = uri("http://thisnode.test", "x");

    public static IRI uri(String prefix, String u) {
        if (prefix.length() > 0) {
            u = prefix + "#" + u;
        }
        return VALUE_FACTORY.createIRI(u);
    }

    public static IRI uri(String u) {
        return uri(TEST_PREFIX, u);
    }

    public static Fact fact(Resource s, IRI p, Value o) {
        return new Fact(s, p, o);
    }

    public static Statement statement(Resource s, IRI p, Value o) {
        return VALUE_FACTORY.createStatement(s, p, o);
    }

    public static Literal intLiteral(String s) {
        return VALUE_FACTORY.createLiteral(s, XMLSchema.INT);
    }

    public static Literal stringLiteral(String s) {
        return VALUE_FACTORY.createLiteral(s, XMLSchema.STRING);
    }

    public static Literal stringLiteral(String s, String lang) {
        return VALUE_FACTORY.createLiteral(s, lang);
    }

    public static BNode bnode(String id) {
        return VALUE_FACTORY.createBNode(id);
    }

    public static RyaStatement ryaStatement(String s, String p, String o) {
        return new RyaStatement(new RyaIRI(TEST_PREFIX + "#" + s),
            new RyaIRI(TEST_PREFIX + "#" + p), new RyaIRI(TEST_PREFIX + "#" + o));
    }
}
