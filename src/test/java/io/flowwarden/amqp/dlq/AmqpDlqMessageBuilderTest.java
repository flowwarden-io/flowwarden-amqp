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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.flowwarden.stream.spi.FailedEvent;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AmqpDlqMessageBuilderTest {

    private final AmqpDlqMessageBuilder builder = new AmqpDlqMessageBuilder();
    private final ObjectMapper reader = new ObjectMapper();

    @Test
    void toJson_fullEvent_roundTripsAllFields() throws Exception {
        BsonDocument documentKey = BsonDocument.parse("{\"_id\": {\"$oid\": \"507f1f77bcf86cd799439011\"}}");
        Document fullDocument = Document.parse("{\"_id\": \"abc\", \"amount\": 42}");
        BsonDocument resumeToken = BsonDocument.parse("{\"_data\": \"82500\"}");
        Instant now = Instant.parse("2026-06-09T12:00:00Z");
        FailedEvent event = new FailedEvent(
                "evt-123",
                "orders-stream",
                "INSERT",
                documentKey,
                fullDocument,
                resumeToken,
                new FailedEvent.ErrorInfo("java.lang.RuntimeException", "boom", "java.lang.RuntimeException: boom\n\tat foo"),
                3,
                FailedEvent.STATUS_PENDING,
                now.minusSeconds(60),
                now,
                now,
                now.plusSeconds(86400),
                Map.of("tenant", "acme", "priority", 1));

        String json = builder.toJson(event);
        JsonNode tree = reader.readTree(json);

        assertThat(tree.get("id").asText()).isEqualTo("evt-123");
        assertThat(tree.get("streamName").asText()).isEqualTo("orders-stream");
        assertThat(tree.get("operationType").asText()).isEqualTo("INSERT");
        assertThat(tree.get("documentKey").isObject()).isTrue();
        assertThat(tree.get("fullDocument").get("amount").asInt()).isEqualTo(42);
        assertThat(tree.get("resumeToken").get("_data").asText()).isEqualTo("82500");
        assertThat(tree.get("error").get("type").asText()).isEqualTo("java.lang.RuntimeException");
        assertThat(tree.get("error").get("message").asText()).isEqualTo("boom");
        assertThat(tree.get("error").get("stackTrace").asText()).contains("at foo");
        assertThat(tree.get("attempts").asInt()).isEqualTo(3);
        assertThat(tree.get("status").asText()).isEqualTo("PENDING");
        assertThat(tree.get("firstAttemptAt").asText()).isEqualTo("2026-06-09T11:59:00Z");
        assertThat(tree.get("metadata").get("tenant").asText()).isEqualTo("acme");
    }

    @Test
    void toJson_nullableFields_omittedFromOutput() throws Exception {
        Instant now = Instant.parse("2026-06-09T12:00:00Z");
        FailedEvent event = new FailedEvent(
                "evt-456",
                "stream-x",
                "UPDATE",
                null,  // documentKey
                null,  // fullDocument
                null,  // resumeToken
                new FailedEvent.ErrorInfo("Err", "msg", null),  // null stackTrace
                1,
                FailedEvent.STATUS_PENDING,
                now,
                now,
                now,
                null,  // null expiresAt (permanent retention)
                Map.of());

        String json = builder.toJson(event);
        JsonNode tree = reader.readTree(json);

        assertThat(tree.has("documentKey")).isFalse();
        assertThat(tree.has("fullDocument")).isFalse();
        assertThat(tree.has("resumeToken")).isFalse();
        assertThat(tree.has("expiresAt")).isFalse();
        assertThat(tree.has("metadata")).isFalse();
        assertThat(tree.get("error").has("stackTrace")).isFalse();
    }

    @Test
    void toJson_nonDocumentDocumentKey_wrappedInIdEnvelope() throws Exception {
        FailedEvent event = new FailedEvent(
                "evt-789",
                "stream-y",
                "DELETE",
                new BsonString("simple-string-id"),  // not a BsonDocument
                null,
                null,
                new FailedEvent.ErrorInfo("E", "m", null),
                1,
                FailedEvent.STATUS_PENDING,
                Instant.now(),
                Instant.now(),
                Instant.now(),
                null,
                Map.of());

        String json = builder.toJson(event);
        JsonNode tree = reader.readTree(json);

        // Non-document BsonValue is wrapped as {"_id": value}
        assertThat(tree.get("documentKey").has("_id")).isTrue();
        assertThat(tree.get("documentKey").get("_id").asText()).isEqualTo("simple-string-id");
    }
}
