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
package org.apache.rya.kafka.connect.api.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkConnector;

import com.jcabi.manifests.Manifests;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * TODO doc
 */
@DefaultAnnotation(NonNull.class)
public abstract class RyaSinkConnector extends SinkConnector {

    /**
     * TODO doc
     *
     * Only called after start has been invoked
     *
     * @return
     * @throws IllegalStateException
     */
    protected abstract AbstractConfig getConfig() throws IllegalStateException;

    @Override
    public String version() {
        return Manifests.exists("Build-Version") ? Manifests.read("Build-Version") : "UNKNOWN";
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for(int i = 0; i < maxTasks; i++) {
            configs.add( getConfig().originalsStrings() );
        }
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since the RyaSinkconnector has no background monitoring.
    }
}