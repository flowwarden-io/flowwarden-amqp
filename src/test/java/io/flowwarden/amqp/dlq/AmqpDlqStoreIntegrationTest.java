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
import io.flowwarden.amqp.annotation.AmqpDlqOptions;
import io.flowwarden.amqp.autoconfigure.AmqpDlqProperties;
import io.flowwarden.amqp.test.SharedAmqpContainer;
import io.flowwarden.stream.spi.DlqPolicy;
import io.flowwarden.stream.spi.FailedEvent;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.lang.annotation.Annotation;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AmqpDlqStoreIntegrationTest {

    private static final String DEFAULT_EXCHANGE = "flowwarden.dlq";
    private static final String TEST_QUEUE = "test-dlq-listener";

    private static CachingConnectionFactory connectionFactory;
    private static RabbitTemplate template;
    private static RabbitAdmin admin;

    @BeforeAll
    static void setUpInfra() {
        connectionFactory = new CachingConnectionFactory(
                SharedAmqpContainer.RABBIT.getHost(),
                SharedAmqpContainer.RABBIT.getAmqpPort());
        connectionFactory.setUsername(SharedAmqpContainer.RABBIT.getAdminUsername());
        connectionFactory.setPassword(SharedAmqpContainer.RABBIT.getAdminPassword());
        connectionFactory.setPublisherConfirmType(
                CachingConnectionFactory.ConfirmType.CORRELATED);

        template = new RabbitTemplate(connectionFactory);
        template.setReceiveTimeout(5_000L);
        admin = new RabbitAdmin(template);

        TopicExchange exchange = new TopicExchange(DEFAULT_EXCHANGE, true, false);
        admin.declareExchange(exchange);
        // non-durable, non-exclusive, non-auto-delete — queue survives between tests until @AfterAll deletes it
        Queue queue = new Queue(TEST_QUEUE, false, false, false);
        admin.declareQueue(queue);
        // catch-all binding ("#" matches any routing key) so per-stream override tests work too
        Binding binding = BindingBuilder.bind(queue).to(exchange).with("#");
        admin.declareBinding(binding);
    }

    @AfterAll
    static void tearDownInfra() {
        if (admin != null) {
            admin.deleteQueue(TEST_QUEUE);
            admin.deleteExchange(DEFAULT_EXCHANGE);
        }
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @BeforeEach
    void purgeQueue() {
        admin.purgeQueue(TEST_QUEUE, false);
    }

    private AmqpDlqStore newStore(boolean confirmModeEffective) {
        return new AmqpDlqStore(template, defaultProperties(),
                new AmqpDlqMessageBuilder(), confirmModeEffective);
    }

    private AmqpDlqProperties defaultProperties() {
        AmqpDlqProperties props = new AmqpDlqProperties();
        props.setExchange(DEFAULT_EXCHANGE);
        props.setRoutingKeyPrefix("dlq.");
        props.setConfirmMode(true);
        return props;
    }

    private FailedEvent simpleEvent(String streamName, String id) {
        Instant now = Instant.now();
        return new FailedEvent(
                id, streamName, "INSERT",
                BsonDocument.parse("{\"_id\": \"abc\"}"),
                new Document("amount", 42),
                BsonDocument.parse("{\"_data\": \"token\"}"),
                new FailedEvent.ErrorInfo("RuntimeException", "boom", null),
                3, FailedEvent.STATUS_PENDING,
                now.minusSeconds(60), now, now,
                now.plusSeconds(86400),
                Map.of("k", "v"));
    }

    private DlqPolicy defaultPolicy() {
        return new DlqPolicy(30, true, true);
    }

    @Test
    void save_publishesToExchangeWithDefaultRoutingKey() {
        AmqpDlqStore store = newStore(true);

        store.save(simpleEvent("orders-stream", "evt-1"), defaultPolicy());

        Message received = template.receive(TEST_QUEUE, 5_000L);
        assertThat(received).isNotNull();
        assertThat(received.getMessageProperties().getReceivedRoutingKey())
                .isEqualTo("dlq.orders-stream");
    }

    @Test
    void save_setsAmqpHeaders() {
        AmqpDlqStore store = newStore(true);

        store.save(simpleEvent("payments-stream", "evt-h1"), defaultPolicy());

        Message received = template.receive(TEST_QUEUE, 5_000L);
        Map<String, Object> headers = received.getMessageProperties().getHeaders();
        assertThat(headers).containsEntry("flowwarden-event-id", "evt-h1");
        assertThat(headers).containsEntry("flowwarden-stream-name", "payments-stream");
        assertThat(headers).containsEntry("flowwarden-operation-type", "INSERT");
        assertThat(headers).containsEntry("flowwarden-attempts", 3);
        assertThat(headers).containsEntry("flowwarden-status", "PENDING");
        assertThat(headers).containsEntry("flowwarden-schema-version",
                AmqpDlqMessageBuilder.SCHEMA_VERSION);
        assertThat(headers.get("flowwarden-first-attempt-at")).isInstanceOf(Long.class);
    }

    @Test
    void save_jsonBodyContainsExpectedFields() throws Exception {
        AmqpDlqStore store = newStore(true);

        store.save(simpleEvent("orders-stream", "evt-json"), defaultPolicy());

        Message received = template.receive(TEST_QUEUE, 5_000L);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode tree = mapper.readTree(received.getBody());
        assertThat(tree.get("id").asText()).isEqualTo("evt-json");
        assertThat(tree.get("streamName").asText()).isEqualTo("orders-stream");
        assertThat(tree.get("error").get("type").asText()).isEqualTo("RuntimeException");
        assertThat(received.getMessageProperties().getContentType()).isEqualTo("application/json");
    }

    @Test
    void save_appliesPolicyRetention() {
        AmqpDlqStore store = newStore(true);

        store.save(simpleEvent("orders-stream", "evt-ttl"),
                new DlqPolicy(7, true, true));

        Message received = template.receive(TEST_QUEUE, 5_000L);
        String expiration = received.getMessageProperties().getExpiration();
        assertThat(expiration).isNotNull();
        long ttlMs = Long.parseLong(expiration);
        assertThat(ttlMs).isEqualTo(7L * 86_400_000L);
    }

    @Test
    void save_zeroRetentionDoesNotSetExpiration() {
        AmqpDlqStore store = newStore(true);

        store.save(simpleEvent("orders-stream", "evt-no-ttl"),
                new DlqPolicy(0, true, true));

        Message received = template.receive(TEST_QUEUE, 5_000L);
        assertThat(received.getMessageProperties().getExpiration()).isNull();
    }

    @Test
    void save_handlesNullableFields() {
        AmqpDlqStore store = newStore(true);
        Instant now = Instant.now();
        FailedEvent nullable = new FailedEvent(
                "evt-null", "orders-stream", "DELETE",
                null, null, null,
                new FailedEvent.ErrorInfo("E", "m", null),
                1, FailedEvent.STATUS_PENDING,
                now, now, now, null, Map.of());

        store.save(nullable, new DlqPolicy(0, true, true));

        Message received = template.receive(TEST_QUEUE, 5_000L);
        assertThat(received).isNotNull();
    }

    @Test
    void save_withAmqpDlqOptionsRoutingKeyOverride_usesCustomKey() {
        AmqpDlqStore store = newStore(true);
        store.registerStream("custom-stream", amqpDlqOptions("", "alert.payment.failed", false));

        store.save(simpleEvent("custom-stream", "evt-custom"), defaultPolicy());

        Message received = template.receive(TEST_QUEUE, 5_000L);
        assertThat(received).isNotNull();
        assertThat(received.getMessageProperties().getReceivedRoutingKey())
                .isEqualTo("alert.payment.failed");
    }

    @Test
    void findById_returnsEmptyAndDoesNotThrow() {
        AmqpDlqStore store = newStore(true);
        assertThat(store.findById("any")).isEmpty();
    }

    @Test
    void findByStreamName_returnsEmptyListAndDoesNotThrow() {
        AmqpDlqStore store = newStore(true);
        assertThat(store.findByStreamName("any")).isEmpty();
    }

    @Test
    void save_withConfirmModeFalse_doesNotBlockOnAck() {
        AmqpDlqStore store = newStore(false);

        // Just verify it completes without throwing
        store.save(simpleEvent("orders-stream", "evt-no-confirm"), defaultPolicy());

        Message received = template.receive(TEST_QUEUE, 5_000L);
        assertThat(received).isNotNull();
    }

    /**
     * Builds an {@link AmqpDlqOptions} instance via a hand-rolled annotation proxy.
     * Cleaner than reflection or AnnotationUtils.synthesizeAnnotation for unit setup.
     */
    private static AmqpDlqOptions amqpDlqOptions(String exchange, String routingKey, boolean mandatory) {
        return new AmqpDlqOptions() {
            @Override public Class<? extends Annotation> annotationType() { return AmqpDlqOptions.class; }
            @Override public String exchange() { return exchange; }
            @Override public String routingKey() { return routingKey; }
            @Override public boolean mandatory() { return mandatory; }
        };
    }
}
