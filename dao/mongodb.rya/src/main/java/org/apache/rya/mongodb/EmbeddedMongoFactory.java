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
package org.apache.rya.mongodb;

import static de.flapdoodle.embed.process.io.Processors.console;
import static de.flapdoodle.embed.process.io.Processors.namedConsole;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.join;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

import de.flapdoodle.embed.mongo.AbstractMongoProcess;
import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongoShellStarter;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongoCmdOptions;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongoCmdOptionsBuilder;
import de.flapdoodle.embed.mongo.config.MongoShellConfigBuilder;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.distribution.IFeatureAwareVersion;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.runtime.Mongod;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.distribution.Distribution;
import de.flapdoodle.embed.process.extract.IExtractedFileSet;
import de.flapdoodle.embed.process.io.IStreamProcessor;
import de.flapdoodle.embed.process.io.LogWatchStreamProcessor;
import de.flapdoodle.embed.process.io.NamedOutputStreamProcessor;
import de.flapdoodle.embed.process.io.file.Files;
import de.flapdoodle.embed.process.runtime.Executable;

public class EmbeddedMongoFactory {
    private static Logger logger = LoggerFactory.getLogger(EmbeddedMongoFactory.class.getName());

    public static final String DEFAULT_ADMIN_USER = "test_admin";
    public static final char[] DEFAULT_ADMIN_PSWD = "pswd".toCharArray();

    private IMongodConfig mongoConfig;

    public static EmbeddedMongoFactory newFactory(final boolean authEnabled) throws Exception {
        return EmbeddedMongoFactory.with(Version.Main.PRODUCTION, authEnabled);
    }

    public static EmbeddedMongoFactory with(final IFeatureAwareVersion version, final boolean authEnabled) throws Exception {
        return new EmbeddedMongoFactory(version, authEnabled);
    }

    private Executable<IMongodConfig, MongodProcess> mongodExecutable;
    private AbstractMongoProcess<IMongodConfig, MongodExecutable, MongodProcess> mongodProcess;

    /**
     * Create the testing utility using the specified version of MongoDB.
     *
     * @param version - version of MongoDB.
     * @param authEnabled - Whether or not to use authentication
     * @throws Exception
     */
    private EmbeddedMongoFactory(final IFeatureAwareVersion version, final boolean authEnabled) throws Exception {
        final Net net = new Net(findRandomOpenPortOnAllLocalInterfaces(), false);
        mongoConfig = new MongodConfigBuilder()
                .version(version)
                .net(net)
                .build();

        final IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder().defaultsWithLogger(Command.MongoD, logger).build();
        final MongodStarter runtime = MongodStarter.getInstance(runtimeConfig);
        mongodExecutable = runtime.prepare(mongoConfig);
        mongodProcess = mongodExecutable.start();

        if(authEnabled) {
            //add admin user
            System.out.print("Adding default admin user.....");
            addAdmin(DEFAULT_ADMIN_USER, DEFAULT_ADMIN_PSWD);
            System.out.println("Done.");

            //bring un-authed mongo down
            //mongodProcess.stop();

            //update configs to enable auths
            final IMongoCmdOptions cmdBuilder = new MongoCmdOptionsBuilder()
                    .enableAuth(authEnabled)
                    .build();
            mongoConfig = new MongodConfigBuilder()
                    .version(version)
                    .cmdOptions(cmdBuilder)
                    .net(new Net(findRandomOpenPortOnAllLocalInterfaces(), false))
                    .build();

            final IExtractedFileSet dbFile = mongodExecutable.getFile();
            mongodExecutable = new EmbeddedMongodExecutable(Distribution.detectFor(version), mongoConfig, runtimeConfig, dbFile);
            mongodProcess = mongodExecutable.start();
        }

    }

    private int findRandomOpenPortOnAllLocalInterfaces() throws IOException {
        try (ServerSocket socket = new ServerSocket(0);) {
            return socket.getLocalPort();
        }
    }

    public void addAdmin(final String username, final char[] password) throws Exception {
        final String scriptText = join(
                format("db.createUser(" +
                        "{\"user\":\"%s\",\"pwd\":\"%s\"," +
                        "\"roles\":[" +
                        "\"root\"" +
                        "]});\n",
                        username, password));
        runScriptAndWait(scriptText, "Successfully added user", new String[]{"couldn't add user", "failed to load", "login failed"}, "admin", null, null);
    }

    public void addDBUser(final String username, final char[] password, final String mongoDBName, final String adminUsername, final char[] adminPassword) throws IOException {
        final String[] roles = {"\"readWrite\"", "{\"db\":\"local\",\"role\":\"read\"}", "{\"db\":\"admin\",\"role\":\"read\"}"};
        final String scriptText = join(
                format("db = db.getSiblingDB('%s'); " +
                        "db.createUser({\"user\":\"%s\",\"pwd\":\"%s\",\"roles\":[%s]});\n" +
                        "db.getUser('%s');",
                        mongoDBName, username, password, StringUtils.join(roles, ","), username), "");
        runScriptAndWait(scriptText, "Successfully added user", new String[]{"already exists", "failed to load", "login failed"}, mongoDBName, adminUsername, adminPassword);
    }

    public void removeUser(final String username) throws Exception {
        final String scriptText = "db.dropUser(" + username + ")";
        runScriptAndWait(scriptText, "Successfully removed user.", new String[]{"couldn't remove user", "failed to load", "login failed"}, "admin", null, null);
    }

    public void removeAllUsers(final String adminUsername, final char[] adminPassword) throws Exception {
        final String scriptText = "db.dropAllUsers()";
        runScriptAndWait(scriptText, "Successfully removed all users.", new String[]{"couldn't remove user", "failed to load", "login failed"}, "admin", adminUsername, adminPassword);
    }

    /**
     * Creates a new Mongo connection.
     *
     * @throws MongoException
     * @throws UnknownHostException
     */
    public MongoClient newMongoClient() throws UnknownHostException, MongoException {
        return new MongoClient(new ServerAddress(mongodProcess.getConfig().net().getServerAddress(), mongodProcess.getConfig().net().getPort()));
    }

    /**
     * Creates a new Mongo connection.
     *
     * @throws MongoException
     * @throws UnknownHostException
     */
    public MongoClient newMongoClient(final String username, final char[] password) throws UnknownHostException, MongoException {
        final ServerAddress addr = new ServerAddress(mongodProcess.getConfig().net().getServerAddress(), mongodProcess.getConfig().net().getPort());
        final MongoCredential creds = MongoCredential.createCredential(username, "admin", password);
        return new MongoClient(addr, Lists.newArrayList(creds));
    }

    /**
     * Gives access to the process configuration.
     *
     */
    public IMongodConfig getMongoServerDetails() {
        return mongodProcess.getConfig();
    }

    /**
     * Cleans up the resources created by the utility.
     */
    public void shutdown() {
        mongodProcess.stop();
        mongodExecutable.stop();
    }


    private void runScriptAndWait(final String scriptText, final String token, final String[] failures, final String dbName, final String username, final char[] password) throws IOException {
        IStreamProcessor mongoOutput;
        if (!isEmpty(token)) {
            mongoOutput = new LogWatchStreamProcessor(
                    format(token),
                    (failures != null) ? new HashSet<>(asList(failures)) : Collections.<String>emptySet(),
                            namedConsole("[mongo shell output]"));
        } else {
            mongoOutput = new NamedOutputStreamProcessor("[mongo shell output]", console());
        }
        final IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
                .defaults(Command.Mongo)
                .processOutput(new ProcessOutput(
                        mongoOutput,
                        namedConsole("[mongo shell error]"),
                        console()))
                .build();
        final MongoShellStarter starter = MongoShellStarter.getInstance(runtimeConfig);
        final File scriptFile = writeTmpScriptFile(scriptText);
        final MongoShellConfigBuilder builder = new MongoShellConfigBuilder();
        if (!isEmpty(dbName)) {
            builder.dbName(dbName);
        }
        if (!isEmpty(username)) {
            builder.username(username);
        }
        if (password != null && password.length != 0) {
            builder.password(new String(password));
        }
        starter.prepare(builder
                .scriptName(scriptFile.getPath())
                .version(mongoConfig.version())
                .net(mongoConfig.net())
                .build()).start();
        if (mongoOutput instanceof LogWatchStreamProcessor) {
            ((LogWatchStreamProcessor) mongoOutput).waitForResult(10000);
        }
    }

    private File writeTmpScriptFile(final String scriptText) throws IOException {
        final File scriptFile = File.createTempFile("tempfile", ".js");
        scriptFile.deleteOnExit();
        final BufferedWriter bw = new BufferedWriter(new FileWriter(scriptFile));
        bw.write(scriptText);
        bw.close();
        return scriptFile;
    }

    public class EmbeddedMongodExecutable extends Executable<IMongodConfig, MongodProcess> {

        public EmbeddedMongodExecutable(final Distribution distribution, final IMongodConfig mongodConfig, final IRuntimeConfig runtimeConfig,
                final IExtractedFileSet files) {
            super(distribution, mongodConfig, runtimeConfig, files);
        }

        @Override
        protected MongodProcess start(final Distribution distribution, final IMongodConfig config, final IRuntimeConfig runtime)
                throws IOException {
            return new EmbeddedMongodProcess(distribution, config, runtime, this);
        }

    }

    private static class EmbeddedMongodProcess extends MongodProcess {

        private static final Logger logger = LoggerFactory.getLogger(EmbeddedMongodProcess.class);

        private File dbDir;
        boolean dbDirIsTemp;

        private final MongodExecutable mongodExecutable;

        public EmbeddedMongodProcess(final Distribution distribution, final IMongodConfig config, final IRuntimeConfig runtimeConfig,
                final EmbeddedMongodExecutable embeddedMongodExecutable) throws IOException {
            super(distribution, config, runtimeConfig, embeddedMongodExecutable);
            mongodExecutable = embeddedMongodExecutable;
        }

        @Override
        protected void onBeforeProcess(final IRuntimeConfig runtimeConfig) throws IOException {
            super.onBeforeProcess(runtimeConfig);

            dbDir = mongodExecutable.getFile().executable();
        }

        @Override
        protected void onBeforeProcessStart(final ProcessBuilder processBuilder, final IMongodConfig config, final IRuntimeConfig runtimeConfig) {
            config.processListener().onBeforeProcessStart(dbDir,dbDirIsTemp);
            super.onBeforeProcessStart(processBuilder, config, runtimeConfig);
        }

        @Override
        protected void onAfterProcessStop(final IMongodConfig config, final IRuntimeConfig runtimeConfig) {
            super.onAfterProcessStop(config, runtimeConfig);
            config.processListener().onAfterProcessStop(dbDir,dbDirIsTemp);
        }


        @Override
        protected List<String> getCommandLine(final Distribution distribution, final IMongodConfig config, final IExtractedFileSet files) throws IOException {
            return Mongod.enhanceCommandLinePlattformSpecific(distribution, Mongod.getCommandLine(getConfig(), files, dbDir));
        }

        @Override
        protected void deleteTempFiles() {
            super.deleteTempFiles();

            if ((dbDir != null) && (dbDirIsTemp) && (!Files.forceDelete(dbDir))) {
                logger.warn("Could not delete temp db dir: {}", dbDir);
            }

        }
    }
}
