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
package org.apache.rya.api.statistics;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.eclipse.rdf4j.model.Resource;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A repository that may be used to maintain the number of statements that have been stored for each context.
 */
@DefaultAnnotation(NonNull.class)
public interface StatementCountsRepository {

    /**
     * Check to see if the repository has been installed.
     *
     * @return {@code true} if the repository has been enitionalized; otherwise {@code false}.
     * @throws StatementCountsException The check was unable to be performed.
     */
    public boolean isInstalled() throws StatementCountsException;

    /**
     * Installs the repository so that it is able to be used to maintain statement counts.
     *
     * @throws AlreadyInstalledException The repository is already installed.
     * @throws StatementCountsException The repository was unable to be installed.
     */
    public void install() throws AlreadyInstalledException, StatementCountsException;

    /**
     * Update the number of statements that have been written for specific contexts.
     *
     * @param deltas - A list of changes that need to be applied to the various contexts that are being tracked. (not null)
     * @throws NotInstalledException The repository needs to be installed before calling this function.
     * @throws StatementCountsException The count was unable to be updated.
     */
    public void updateCount(List<Delta> deltas) throws NotInstalledException, StatementCountsException;

    /**
     * Get the number of statements that have been written for a specific context.
     *
     * @param context - The context whose could will be fetched.
     * @return The number of statements that have been stored for {@code context}.
     * @throws NotInstalledException The repository needs to be installed before calling this function.
     * @throws StatementCountsException The count was unable to be fetched.
     */
    public Optional<Long> getCount(Resource context) throws NotInstalledException, StatementCountsException;

    /**
     * Completely remove the count for a specific context.
     *
     * @param context - The context whose count will be deleted.
     * @throws NotInstalledException The repository needs to be installed before calling this function.
     * @throws StatementCountsException The count was unable to be deleted.
     */
    public void deleteCount(Resource context) throws NotInstalledException, StatementCountsException;

    /**
     * Completely remove the count for all contexts that are being tracked.
     *
     * @throws NotInstalledException The repository needs to be installed before calling this function.
     * @throws StatementCountsException The counts were unable to be deleted.
     */
    public void deleteAllCounts() throws NotInstalledException, StatementCountsException;

    /**
     * Uninstalls the repository.
     *
     * @throws StatementCountsException The repository wsa unable to be uninstalled.
     */
    public void uninstall() throws StatementCountsException;

    /**
     * The amount of change that needs to be applied to a context's statement count.
     */
    @DefaultAnnotation(NonNull.class)
    public static final class Delta {

        private final Resource context;
        private final long delta;

        /**
         * Constructs an instance of {@link Delta}.
         *
         * @param context - The context the delta will be applied to.
         * @param delta - The amount of change that will be applied to the statement count.
         */
        public Delta(final Resource context, final long delta) {
            this.context = requireNonNull(context);
            this.delta = delta;
        }

        /**
         * @return The context the delta will be applied to.
         */
        public Resource getContext() {
            return context;
        }

        /**
         * @return The amount of change that will be applied to the statement count.
         */
        public long getDelta() {
            return delta;
        }

        @Override
        public int hashCode() {
            return Objects.hash(context, delta);
        }

        @Override
        public boolean equals(final Object o) {
            if(this == o) {
                return true;
            }
            if(o instanceof Delta) {
                final Delta other = (Delta) o;
                return Objects.equals(context, other.context) &&
                        delta == other.delta;
            }
            return false;
        }

        @Override
        public String toString() {
            return "Delta { context: " + context + ", delta: " + delta + " }";
        }
    }

    /**
     * A function of {@link StatementCountsRepository} could not be executed.
     */
    public static class StatementCountsException extends Exception {
        private static final long serialVersionUID = 1L;

        public StatementCountsException(final String message) {
            super(message);
        }

        public StatementCountsException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * You could not install a new {@link StatementCountsException} because it already has been installed.
     */
    public static class AlreadyInstalledException extends StatementCountsException {
        private static final long serialVersionUID = 1L;

        public AlreadyInstalledException(final String message) {
            super(message);
        }

        public AlreadyInstalledException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * The function could not be executed because the repository has not been installed.
     */
    public static class NotInstalledException extends StatementCountsException {
        private static final long serialVersionUID = 1L;

        public NotInstalledException(final String message) {
            super(message);
        }

        public NotInstalledException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}