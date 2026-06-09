/*
 * Copyright 2026 FlowWarden
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.flowwarden.amqp.dlq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.flowwarden.stream.spi.FailedEvent;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Serializes {@link FailedEvent} into a UTF-8 JSON body suitable for AMQP
 * publish.
 *
 * <p>Conversion of MongoDB-specific BSON types (handled manually rather than
 * via a Jackson module to keep the dependency footprint small):</p>
 * <ul>
 *   <li>{@code BsonValue documentKey} &mdash; if {@code BsonDocument},
 *       serialized via {@link BsonDocument#toJson()}; otherwise wrapped as
 *       {@code {"_id": value}}. Null fields are omitted.</li>
 *   <li>{@code BsonDocument resumeToken} &mdash; serialized via
 *       {@link BsonDocument#toJson()} (MongoDB extended JSON).</li>
 *   <li>{@code Document fullDocument} &mdash; serialized via
 *       {@link Document#toJson()}.</li>
 *   <li>{@code Instant} timestamps &mdash; ISO-8601 via Jackson
 *       {@code JavaTimeModule} with {@code WRITE_DATES_AS_TIMESTAMPS=false}.</li>
 *   <li>{@code Map<String, Object> metadata} &mdash; standard Jackson
 *       serialization.</li>
 * </ul>
 *
 * <p>This is the wire format for the satellite's AMQP messages. Its version
 * is tracked via the {@code flowwarden-schema-version} AMQP header; any
 * breaking change to {@link FailedEvent} bumps the schema version.</p>
 */
public final class AmqpDlqMessageBuilder {

    /**
     * Wire schema version published in the {@code flowwarden-schema-version}
     * AMQP header. Bump on breaking changes to {@link FailedEvent}.
     */
    public static final int SCHEMA_VERSION = 1;

    private final ObjectMapper objectMapper;

    public AmqpDlqMessageBuilder() {
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .findAndRegisterModules();
        this.objectMapper.configure(
                com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    /**
     * Serializes a {@link FailedEvent} into a JSON string.
     */
    public String toJson(FailedEvent event) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("id", event.id());
        root.put("streamName", event.streamName());
        root.put("operationType", event.operationType());

        BsonValue documentKey = event.documentKey();
        if (documentKey != null) {
            root.put("documentKey", parseJsonOrLiteral(documentKeyToJson(documentKey)));
        }

        Document fullDocument = event.fullDocument();
        if (fullDocument != null) {
            root.put("fullDocument", parseJsonOrLiteral(fullDocument.toJson()));
        }

        BsonDocument resumeToken = event.resumeToken();
        if (resumeToken != null) {
            root.put("resumeToken", parseJsonOrLiteral(resumeToken.toJson()));
        }

        FailedEvent.ErrorInfo error = event.error();
        if (error != null) {
            Map<String, Object> errorMap = new LinkedHashMap<>();
            errorMap.put("type", error.type());
            errorMap.put("message", error.message());
            if (error.stackTrace() != null) {
                errorMap.put("stackTrace", error.stackTrace());
            }
            root.put("error", errorMap);
        }

        root.put("attempts", event.attempts());
        root.put("status", event.status());
        root.put("firstAttemptAt", event.firstAttemptAt());
        root.put("lastAttemptAt", event.lastAttemptAt());
        root.put("createdAt", event.createdAt());
        if (event.expiresAt() != null) {
            root.put("expiresAt", event.expiresAt());
        }
        if (event.metadata() != null && !event.metadata().isEmpty()) {
            root.put("metadata", event.metadata());
        }

        try {
            return objectMapper.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    "Failed to serialize FailedEvent[id=" + event.id() + "] to JSON", e);
        }
    }

    private String documentKeyToJson(BsonValue documentKey) {
        if (documentKey.isDocument()) {
            return documentKey.asDocument().toJson();
        }
        return new BsonDocument("_id", documentKey).toJson();
    }

    /**
     * Re-parses a JSON-shaped string into a Jackson tree so it ends up nested
     * in the final JSON output (instead of being escaped as a string literal).
     */
    private Object parseJsonOrLiteral(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (JsonProcessingException e) {
            // Defensive fallback: should not happen with valid BSON .toJson() outputs
            return json;
        }
    }
}
