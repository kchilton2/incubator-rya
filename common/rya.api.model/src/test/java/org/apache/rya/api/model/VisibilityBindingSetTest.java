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
package org.apache.rya.api.model;

import static org.junit.Assert.assertNotEquals;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Test;

/**
 * Unit tests the methods of {@link VisibilityBindingSet}.
 */
public class VisibilityBindingSetTest {

    @Test
    public void hashcode() {
        // Create a BindingSet, decorate it, and grab its hash code.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final MapBindingSet bSet = new MapBindingSet();
        bSet.addBinding("name", vf.createLiteral("alice"));

        final VisibilityBindingSet visSet = new VisibilityBindingSet(bSet);
        final int origHash = visSet.hashCode();

        // Add another binding to the binding set and grab the new hash code.
        bSet.addBinding("age", vf.createLiteral(37));
        final int updatedHash = visSet.hashCode();

        // Show those hashes are different.
        assertNotEquals(origHash, updatedHash);
    }
}
