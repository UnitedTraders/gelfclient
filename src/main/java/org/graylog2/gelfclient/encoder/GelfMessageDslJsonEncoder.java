/*
 * Copyright 2014 TORCH GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.graylog2.gelfclient.encoder;

import com.dslplatform.json.*;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.graylog2.gelfclient.GelfMessage;
import org.graylog2.gelfclient.GelfMessageLevel;
import org.graylog2.gelfclient.GelfMessageVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 * A Netty channel handler encoding {@link GelfMessage} into valid JSON according to the
 * <a href="http://graylog2.org/gelf#specs">GELF specification</a>.
 */
@ChannelHandler.Sharable
public class GelfMessageDslJsonEncoder extends MessageToMessageEncoder<GelfMessage> {
    private static final Logger LOG = LoggerFactory.getLogger(GelfMessageJsonEncoder.class);
    private final DslJson<Object> jsonProcessor;
    private boolean serializationTrackingEnabled;


    /**
     * Creates a new instance of this channel handler with the default {@link com.dslplatform.json.DslJson}.
     */
    public GelfMessageDslJsonEncoder(boolean serializationTrackingEnbaled) {
        this(new DslJson<>());
        this.serializationTrackingEnabled = serializationTrackingEnbaled;
    }

    /**
     * Creates a new instance of this channel handler with the given {@link com.dslplatform.json.DslJson}.
     *
     * @param jsonProcessor is a {@link com.dslplatform.json.DslJson} to use for constructing a GELF message payload
     */
    private GelfMessageDslJsonEncoder(final DslJson<Object> jsonProcessor) {
        this.jsonProcessor = jsonProcessor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        LOG.error("JSON encoding error", cause);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, GelfMessage message, List<Object> out) throws Exception {
        out.add(Unpooled.wrappedBuffer(toJson(message)));
    }

    private byte[] toJson(final GelfMessage message) throws Exception {

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (serializationTrackingEnabled) {
            message.addAdditionalField("_serializationRate", "logs_count=" + SerializationTimeTracker.logsCount.get() + ";time_spent=" + SerializationTimeTracker.timeSpent.get());

            long t1 = System.currentTimeMillis();
            jsonProcessor.serialize(message, out);
            long t2 = System.currentTimeMillis();
            SerializationTimeTracker.logsCount.incrementAndGet();
            SerializationTimeTracker.timeSpent.addAndGet((t2 - t1));
        } else {
            jsonProcessor.serialize(message, out);
        }

        return out.toByteArray();
    }

    public static class MessageVersionConverter {

        public static final JsonWriter.WriteObject<GelfMessageVersion> JSON_WRITER = new JsonWriter.WriteObject<GelfMessageVersion>() {
            @Override
            public void write(JsonWriter writer, GelfMessageVersion version) {
                if (version == null) {
                    writer.writeNull();
                } else {
                    writer.writeByte((byte) '"');
                    writer.writeAscii(version.toString());
                    writer.writeByte((byte) '"');
                }
            }
        };

        public static final JsonReader.ReadObject<GelfMessageVersion> JSON_READER = new JsonReader.ReadObject<GelfMessageVersion>() {
            public GelfMessageVersion read(JsonReader reader) throws IOException {
                return reader.wasNull() ? null : GelfMessageVersion.valueOf(reader.readString());
            }
        };

    }

    public static class MessageLevelConverter {
        public static final JsonWriter.WriteObject<GelfMessageLevel> JSON_WRITER = new JsonWriter.WriteObject<GelfMessageLevel>() {
            @Override
            public void write(JsonWriter writer, GelfMessageLevel level) {
                if (level == null) {
                    writer.writeNull();
                } else {
                    writer.writeByte((byte) '"');
                    NumberConverter.serialize(level.getNumericLevel(), writer);
                    writer.writeByte((byte) '"');
                }
            }
        };

        public static final JsonReader.ReadObject<GelfMessageLevel> JSON_READER = new JsonReader.ReadObject<GelfMessageLevel>() {
            public GelfMessageLevel read(final JsonReader reader) throws IOException {
                return null; // we only serialize log levels
            }
        };

    }

    public static class AdditionalFieldsConverter {

        public static final JsonWriter.WriteObject<Map<String, Object>> JSON_WRITER = new JsonWriter.WriteObject<Map<String, Object>>() {

            public void write(JsonWriter writer, Map<String, Object> fields) {

                writer.writeByte(JsonWriter.QUOTE);
                writer.writeByte(JsonWriter.QUOTE);

                if (fields == null) {
                    writer.writeNull();
                    return;
                }

                String realKey;
                for (Map.Entry<String, Object> field : fields.entrySet()) {
                    realKey = field.getKey().startsWith("_") ? field.getKey() : ("_" + field.getKey());

                    writer.writeByte(JsonWriter.COMMA);
                    writer.writeString(realKey);
                    writer.writeByte(JsonWriter.SEMI);
                    writer.serializeObject(field.getValue());
                }

            }
        };

        public static final JsonReader.ReadObject<Map<String, Object>> JSON_READER = new JsonReader.ReadObject<Map<String, Object>>() {
            public Map<String, Object> read(JsonReader reader) throws IOException {
                return null; // we only serialize log additional fields
            }
        };
    }
}


