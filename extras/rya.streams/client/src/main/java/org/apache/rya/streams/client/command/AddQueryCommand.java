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
package org.apache.rya.streams.client.command;

import static java.util.Objects.requireNonNull;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.exception.RyaStreamsException;
import org.apache.rya.streams.api.interactor.AddQuery;
import org.apache.rya.streams.api.interactor.defaults.DefaultAddQuery;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.client.RyaStreamsCommand;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.queries.KafkaQueryChangeLogFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Strings;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A command that adds a new query into Rya Streams.
 */
@DefaultAnnotation(NonNull.class)
public class AddQueryCommand implements RyaStreamsCommand {

    /**
     * Command line parameters that are used by this command to configure itself.
     */
    private class AddParameters extends RyaStreamsCommand.KafkaParameters {
        @Parameter(names = { "--query", "-q" }, required = true, description = "The SPARQL query to add to Rya Streams.")
        private String query;

        @Parameter(names = {"--isActive", "-a"}, required = false, description = "True if the added query will be started.")
        private String isActive;

        @Override
        public String toString() {
            final StringBuilder parameters = new StringBuilder();
            parameters.append(super.toString());

            if (!Strings.isNullOrEmpty(query)) {
                parameters.append("\tQuery: " + query + "\n");
            }
            parameters.append("\tIs Active: " + isActive + "\n");
            return parameters.toString();
        }
    }

    @Override
    public String getCommand() {
        return "add-query";
    }

    @Override
    public String getDescription() {
        return "Add a new query to Rya Streams.";
    }

    @Override
    public String getUsage() {
        final JCommander parser = new JCommander(new AddParameters());

        final StringBuilder usage = new StringBuilder();
        parser.usage(usage);
        return usage.toString();
    }

    @Override
    public boolean validArguments(final String[] args) {
        boolean valid = true;
        try {
            new JCommander(new AddParameters(), args);
        } catch(final ParameterException e) {
            valid = false;
        }
        return valid;
    }

    @Override
    public void execute(final String[] args) throws ArgumentsException, ExecutionException {
        requireNonNull(args);

        // Parse the command line arguments.
        final AddParameters params = new AddParameters();
        try {
            new JCommander(params, args);
        } catch(final ParameterException e) {
            throw new ArgumentsException("Could not add a new query because of invalid command line parameters.", e);
        }

        // Create the Kafka backed QueryChangeLog.
        final String bootstrapServers = params.kafkaIP + ":" + params.kafkaPort;
        final String topic = KafkaTopics.queryChangeLogTopic(params.ryaInstance);
        final QueryChangeLog queryChangeLog = KafkaQueryChangeLogFactory.make(bootstrapServers, topic);

        // Execute the add query command.
        try(QueryRepository queryRepo = new InMemoryQueryRepository(queryChangeLog)) {
            final AddQuery addQuery = new DefaultAddQuery(queryRepo);
            try {
                final StreamsQuery query = addQuery.addQuery(params.query, Boolean.parseBoolean(params.isActive));
                System.out.println("Added query: " + query.getSparql());
                System.out.println("Query ID: " + query.getQueryId());
            } catch (final RyaStreamsException e) {
                System.err.println("Unable to parse query: " + params.query);
                e.printStackTrace();
                System.exit(1);
            }
        } catch (final Exception e) {
            System.err.println("Problem encountered while closing the QueryRepository.");
            e.printStackTrace();
            System.exit(1);
        }
    }
}