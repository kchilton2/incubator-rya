/**
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
package org.apache.rya.api.client;

import org.eclipse.rdf4j.model.Resource;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * If being maintained, fetch the number of statements that have been stored for a specific context within a Rya instance.
 */
@DefaultAnnotation(NonNull.class)
public interface GetStatementCount {

    /**
     * Checks a Rya instance to see if it is maintaining statement counts by context.
     *
     * @param ryaInstanceName - The name of the Rya instance to check. (not null)
     * @return {@code true} if the Rya instance is maintaining statement counts by context; otherwise {@code false}.
     * @throws InstanceDoesNotExistException No instance of Rya exists for the provided name.
     * @throws RyaClientException Something caused the command to fail.
     */
    public boolean isEnabled(String ryaInstanceName) throws InstanceDoesNotExistException, RyaClientException;

    /**
     * Get the number of statements that have been stored for a specific context within a Rya instance.
     *
     * @param ryaInstanceName - The name of the Rya instance to check. (not null)
     * @param context - The context within the Rya instance to check.
     * @return The number of statements that have been stored for the context.
     * @throws InstanceDoesNotExistException No instance of Rya exists for the provided name.
     * @throws RyaClientException Something caused the command to fail. If the feature is not enabled, this will be thrown.
     */
    public long getStatementCount(String ryaInstanceName, Resource context) throws InstanceDoesNotExistException, RyaClientException;
}