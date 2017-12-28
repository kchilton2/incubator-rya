package org.apache.rya.indexing.pcj.fluo.app.batch.serializer;
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
import java.lang.reflect.Type;

import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;
import org.apache.rya.api.function.join.LazyJoiningIterator.Side;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation.Task;
import org.apache.rya.indexing.pcj.fluo.app.batch.JoinBatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata.JoinType;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetStringConverter;

import com.google.common.base.Joiner;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * JsonSerializer/JsonDeserializer to serialize/deserialize {@link JoinBatchInformation} objects.
 *
 */
public class JoinBatchInformationTypeAdapter implements JsonSerializer<JoinBatchInformation>, JsonDeserializer<JoinBatchInformation> {

    private static final VisibilityBindingSetStringConverter converter = new VisibilityBindingSetStringConverter();

    @Override
    public JsonElement serialize(final JoinBatchInformation batch, final Type typeOfSrc, final JsonSerializationContext context) {
        final JsonObject result = new JsonObject();
        result.add("class", new JsonPrimitive(batch.getClass().getName()));
        result.add("batchSize", new JsonPrimitive(batch.getBatchSize()));
        result.add("task", new JsonPrimitive(batch.getTask().name()));
        final Column column = batch.getColumn();
        result.add("column", new JsonPrimitive(column.getsFamily() + "\u0000" + column.getsQualifier()));
        final Span span = batch.getSpan();
        result.add("span", new JsonPrimitive(span.getStart().getsRow() + "\u0000" + span.getEnd().getsRow()));
        result.add("startInc", new JsonPrimitive(span.isStartInclusive()));
        result.add("endInc", new JsonPrimitive(span.isEndInclusive()));
        result.add("side", new JsonPrimitive(batch.getSide().name()));
        result.add("joinType", new JsonPrimitive(batch.getJoinType().name()));
        final String updateVarOrderString = Joiner.on(";").join(batch.getBs().getBindingNames());
        final VariableOrder updateVarOrder = new VariableOrder(updateVarOrderString);
        result.add("bindingSet", new JsonPrimitive(converter.convert(batch.getBs(), updateVarOrder)));
        result.add("updateVarOrder", new JsonPrimitive(updateVarOrderString));
        return result;
    }

    @Override
    public JoinBatchInformation deserialize(final JsonElement element, final Type typeOfT, final JsonDeserializationContext context)
            throws JsonParseException {
        final JsonObject json = element.getAsJsonObject();
        final int batchSize = json.get("batchSize").getAsInt();
        final Task task = Task.valueOf(json.get("task").getAsString());
        final String[] colArray = json.get("column").getAsString().split("\u0000");
        final Column column = new Column(colArray[0], colArray[1]);
        final String[] rows = json.get("span").getAsString().split("\u0000");
        final boolean startInc = json.get("startInc").getAsBoolean();
        final boolean endInc = json.get("endInc").getAsBoolean();
        final Span span = new Span(new RowColumn(rows[0]), startInc, new RowColumn(rows[1]), endInc);
        final VariableOrder updateVarOrder = new VariableOrder(json.get("updateVarOrder").getAsString());
        final VisibilityBindingSet bs = converter.convert(json.get("bindingSet").getAsString(), updateVarOrder);
        final Side side = Side.valueOf(json.get("side").getAsString());
        final JoinType join = JoinType.valueOf(json.get("joinType").getAsString());
        return JoinBatchInformation.builder().setBatchSize(batchSize).setTask(task).setSpan(span).setColumn(column).setBs(bs)
               .setSide(side).setJoinType(join).build();
    }

}
