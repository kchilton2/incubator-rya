package org.apache.rya.rdftriplestore.inference;

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

import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

/**
 * Class DoNotExpandSP
 * Date: Mar 15, 2012
 * Time: 9:39:45 AM
 */
public class DoNotExpandSP extends StatementPattern{
    public DoNotExpandSP() {
    }

    public DoNotExpandSP(Var subject, Var predicate, Var object) {
        super(subject, predicate, object);
    }

    public DoNotExpandSP(Scope scope, Var subject, Var predicate, Var object) {
        super(scope, subject, predicate, object);
    }

    public DoNotExpandSP(Var subject, Var predicate, Var object, Var context) {
        super(subject, predicate, object, context);
    }

    public DoNotExpandSP(Scope scope, Var subjVar, Var predVar, Var objVar, Var conVar) {
        super(scope, subjVar, predVar, objVar, conVar);
    }
}
