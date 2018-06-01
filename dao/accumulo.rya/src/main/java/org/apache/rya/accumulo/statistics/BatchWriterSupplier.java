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
package org.apache.rya.accumulo.statistics;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.TableNotFoundException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Abstracts away how the repository receives the {@link BatchWriter} that it will use to write deltas.
 * This makes it possible for other parts of the application to provide a batch writer that is tied to
 * a {@link MultiTableBatchWriter}. Delta mutations will be flushed when the statemetns those deltas
 * represent are flushed.
 */
@DefaultAnnotation(NonNull.class)
public interface BatchWriterSupplier {

    /**
     * Make a {@link BatchWriter} that writes to the specified table.
     *
     * @param tableName - The name of the table the writer will be connected to. (not null)
     * @return A {@link BatchWriter} to the specified table.
     * @throws TableNotFoundException The {@link BatchWriter} could not be created because the
     *   table does not exist yet. Ensure the repository has been installed.
     * @throws AccumuloException The {@link BatchWriter} could not be made for some other reason.
     * @throws AccumuloSecurityException The {@link BatchWriter} could not be made for some other reason.
     */
    public BatchWriter makeBatchWriter(String tableName) throws TableNotFoundException, AccumuloException, AccumuloSecurityException;
}