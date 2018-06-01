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
package org.apache.rya.api.client.accumulo;

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.client.Connector;
import org.apache.rya.accumulo.statistics.AccumuloStatementCountsRepository;
import org.apache.rya.api.client.GetInstanceDetails;
import org.apache.rya.api.client.GetStatementCount;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.statistics.StatementCountsRepository;
import org.apache.rya.api.statistics.StatementCountsRepository.StatementCountsException;
import org.apache.rya.indexing.accumulo.statistics.AccumuloStatementCountIndexer;
import org.eclipse.rdf4j.model.Resource;

import com.google.common.base.Optional;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An Accumulo implementation of {@link GetStatementCount}.
 */
@DefaultAnnotation(NonNull.class)
public class AccumuloGetStatementCount implements GetStatementCount {

    private final InstanceExists instanceExists;
    private final GetInstanceDetails getInstanceDetails;
    private final Connector connector;

    /**
     * Constructs an instance of {@link AccumuloGetStatementCount}.
     *
     * @param instanceExists - The interactor used to verify Rya instances exist. (not null)
     * @param getInstanceDetails - The interactor used to fetch the Rya instance's details. (not null)
     * @param connector - Provides programatic access to the instance of Accumulo that hosts Rya instance. (not null)
     */
    public AccumuloGetStatementCount(
            final InstanceExists instanceExists,
            final GetInstanceDetails getInstanceDetails,
            final Connector connector) {
        this.instanceExists = requireNonNull(instanceExists);
        this.getInstanceDetails = requireNonNull(getInstanceDetails);
        this.connector = requireNonNull(connector);
    }

    @Override
    public boolean isEnabled(final String ryaInstanceName) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);

        // Verify the Rya Instance exists.
        if(!instanceExists.exists(ryaInstanceName)) {
            throw new InstanceDoesNotExistException("There is no Rya instance named '" + ryaInstanceName + "' in this storage.");
        }

        // This feature only works for instances of Rya that use Rya Details to track configuration and instance state.
        final Optional<RyaDetails> details = getInstanceDetails.getDetails(ryaInstanceName);
        if(!details.isPresent()) {
            return false;
        }

        // Return whatever the details say the instance is configured to do.
        return details.get().isMaintainStatementCounts();
    }

    @Override
    public long getStatementCount(final String ryaInstanceName, final Resource context) throws InstanceDoesNotExistException, RyaClientException {
        requireNonNull(ryaInstanceName);
        requireNonNull(context);

        // Verify the Rya Instance exists.
        if(!instanceExists.exists(ryaInstanceName)) {
            throw new InstanceDoesNotExistException("There is no Rya instance named '" + ryaInstanceName + "' in this storage.");
        }

        final String tableName = AccumuloStatementCountIndexer.makeTableName(ryaInstanceName);
        final StatementCountsRepository countsRepo = AccumuloStatementCountsRepository.makeReadOnly(connector, tableName);

        try {
            // The feature's storage must be installed.
            if(!countsRepo.isInstalled()) {
                throw new RyaClientException("Unable to fetch Statement Counts for the Rya instance '" + ryaInstanceName +
                        "' because the StatementCountsRepository has not been installed.");
            }

            return countsRepo.getCount(context).orElse(0L);

        } catch (final StatementCountsException e) {
            throw new RyaClientException("Unable to fetch the Statements Count for the context " + context +
                    " in Rya instance " + ryaInstanceName + ".", e);
        }
    }
}