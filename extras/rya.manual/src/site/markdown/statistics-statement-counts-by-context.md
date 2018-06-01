<!--
[comment]: # Licensed to the Apache Software Foundation (ASF) under one
[comment]: # or more contributor license agreements.  See the NOTICE file
[comment]: # distributed with this work for additional information
[comment]: # regarding copyright ownership.  The ASF licenses this file
[comment]: # to you under the Apache License, Version 2.0 (the
[comment]: # "License"); you may not use this file except in compliance
[comment]: # with the License.  You may obtain a copy of the License at
[comment]: # 
[comment]: #   http://www.apache.org/licenses/LICENSE-2.0
[comment]: # 
[comment]: # Unless required by applicable law or agreed to in writing,
[comment]: # software distributed under the License is distributed on an
[comment]: # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
[comment]: # KIND, either express or implied.  See the License for the
[comment]: # specific language governing permissions and limitations
[comment]: # under the License.
-->

# Statistics - Statement Counts by Context #

Introduced in 4.0.0

# Table of Contents #
- [Introduction](#introduction)
- [Quick Start](#quick-start)
- [Future Work](#future-work)

<div id='introduction'/>

## Introduction ##
For some instances of Rya, it is convenient to quickly look up how many 
Statements have been stored for a context. When this feature is enabled,
Rya clients will index that information as statements are added and removed.

This feature may only be turned on at install time. Once it is turned on for
an instance, it can not be turned off.

<div id='quick-start'/>

## Quick Start ##
This tutorial demonstrates how you may install an instance of Rya with this
feature enabled, as well as how to access the counts from the command line.
If you need programatic access to the count, then you may use the Java 
RyaClient class.

### Step 1: Download the application ###
You can fetch the artifacts you need to follow this Quick Start from our
[downloads page](http://rya.apache.org/download/). Click on the release of
interest and follow the "Central repository for Maven and other dependency
managers" URL.

Fetch the following artifact:

Artifact Id | Type 
--- | ---
rya.shell | shaded jar

### Step 2: Install an instance of Rya with the feature enabled ###
The Rya instance need to have the feature turned on when you do the install.
This is an option that is presented to you when performing the install. You
need to tell it "true" when prompted with "Maintain Statement counts for each 
Context [default: false]". Here's roughly what an installation session should 
look like.

```
java -jar rya.shell-4.0.0-incubating-shaded.jar

 _____                _____ _          _ _
|  __ \              / ____| |        | | |
| |__) |   _  __ _  | (___ | |__   ___| | |
|  _  / | | |/ _` |  \___ \| '_ \ / _ \ | |
| | \ \ |_| | (_| |  ____) | | | |  __/ | |
|_|  \_\__, |\__,_| |_____/|_| |_|\___|_|_|
        __/ |
       |___/
4.0.0-incubating

Welcome to the Rya Shell.

Execute one of the connect commands to start interacting with an instance of Rya.
You may press tab at any time to see which of the commands are available.

rya>connect-accumulo --zookeepers localhost --instanceName quickstart_instance --username quickstart
Password: *******

rya/quickstart_instance> install
Rya Instance Name [default: rya_]: quickstart
Use Shard Balancing (improves streamed input write speeds) [default: false]: f
Maintain Statement counts for each Context [default: false]: true
Use Entity Centric Indexing [default: true]: f
Use Free Text Indexing [default: true]: f
Use Temporal Indexing [default: true]: f
Use Precomputed Join Indexing [default: true]: f

A Rya instance will be installed using the following values:
   Instance Name: quickstart
   Use Shard Balancing: false
   Maintain Statement counts for each Context: true
   Use Entity Centric Indexing: false
   Use Free Text Indexing: false
   Use Temporal Indexing: false
   Use Precomputed Join Indexing: false

Continue with the install? (y/n) y
The Rya instance named 'quickstart' has been installed.

```

### Step 3: Load data into the Rya instance ###
Next we need to create a file that contains the statements we will load into the topic.
Name the file "quickstart-statements.nt" and use a text editor to write the following lines to it:

```
<urn:Alice> <urn:talksTo> <urn:Bob> .
<urn:Bob> <urn:talksTo> <urn:Alice> .
<urn:Bob> <urn:talksTo> <urn:Charlie> .
<urn:Charlie> <urn:talksTo> <urn:Alice> .
<urn:David> <urn:talksTo> <urn:Eve> .
<urn:Eve> <urn:listensTo> <urn:Bob> .
```
Now within the shell, you may load that file using a specific context.

```
rya/quickstart_instance> connect-rya --instance quickstart
rya/quickstart_instance:quickstart> load-data --file ./quickstart-statements.nt --contexts urn:contextA
Detected RDF Format: N-Triples (mimeTypes=application/n-triples, text/plain; ext=nt)
Loaded the file: './quickstart-statements.nt' successfully in 0.451 seconds.
```

### Step 4: Query the Rya instance for a count ###
Finally, we would like to see how many statements were loaded for "urn:contextA".

```
rya/quickstart_instance:quickstart> get-statement-count --context urn:contextA
Count: 6
```

If you ask for the count of a context nothing has been loaded to, it reports 0.

```
rya/quickstart_instance:quickstart> get-statement-count --context urn:contextA
Count: 0
```

<div id='future-work'/>

## Future Work ##

### Mongo DB Support ###
This feature is only implemented for the Accumulo implementation of Rya.
In the future this support could be expanded to the Mongo DB implementaiton
as well.