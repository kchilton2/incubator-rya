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
package org.apache.rya.streams.kafka.connect;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * TODO impl, test, doc
 *
 * What is this responsible for doing?
 *
 * Needs to be told the format of the data that it is writing back to Rya.
 * PCJs produce binding sets and need to be written back to the PCJ Storage.
 * Statements needs to be written back to the Rya statements collection.
 *
 * Does any part of that happen in here?
 */
public class RyaSinkConnector extends SinkConnector {

    @Override
    public String version() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void start(Map<String, String> props) {
        // TODO Auto-generated method stub

    }

    @Override
    public Class<? extends Task> taskClass() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub

    }

    @Override
    public ConfigDef config() {
        // TODO Auto-generated method stub
        return null;
    }
}