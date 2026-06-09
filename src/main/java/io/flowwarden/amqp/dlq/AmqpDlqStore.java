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

import io.flowwarden.amqp.annotation.AmqpDlqOptions;
import io.flowwarden.amqp.autoconfigure.AmqpDlqProperties;
import io.flowwarden.stream.spi.DlqPolicy;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.FailedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AMQP 0.9.1 publish-only {@link DlqStore} implementation.
 *
 * <p>Publishes failed events to a topic exchange via Spring AMQP
 * {@link RabbitTemplate}. Read operations ({@code findById},
 * {@code findByStreamName}) return empty values and log a one-shot warning
 * pointing users to the AMQP consumer pattern.</p>
 *
 * <p>Behavior is documented in detail in the satellite's README and ADR. Key
 * points:</p>
 * <ul>
 *   <li>Topic exchange + routing key {@code prefix + streamName}, configurable
 *       globally via {@link AmqpDlqProperties} and per-stream via
 *       {@link AmqpDlqOptions}.</li>
 *   <li>JSON body via {@link AmqpDlqMessageBuilder}.</li>
 *   <li>AMQP headers: {@code flowwarden-event-id}, {@code flowwarden-stream-name},
 *       {@code flowwarden-operation-type}, {@code flowwarden-attempts},
 *       {@code flowwarden-status}, {@code flowwarden-first-attempt-at},
 *       {@code flowwarden-schema-version}.</li>
 *   <li>Publisher confirms (blocking ACK wait) when
 *       {@code flowwarden.amqp.confirm-mode=true}.</li>
 *   <li>{@code save} does not catch broker errors &mdash; an {@code AmqpException}
 *       propagates to the core which emits
 *       {@code onEventDlqFailed(streamName, cause)}.</li>
 * </ul>
 */
public class AmqpDlqStore implements DlqStore {

    private static final Logger log = LoggerFactory.getLogger(AmqpDlqStore.class);

    /** Timeout for publisher confirms (ms). Generous default — broker failure dominates latency. */
    private static final long CONFIRM_TIMEOUT_MS = 10_000L;

    private final RabbitTemplate template;
    private final AmqpDlqProperties properties;
    private final AmqpDlqMessageBuilder messageBuilder;
    private final boolean confirmModeEffective;

    private final ConcurrentMap<String, AmqpDlqOptions> overrides = new ConcurrentHashMap<>();
    private final AtomicBoolean findByIdWarned = new AtomicBoolean(false);
    private final AtomicBoolean findByStreamNameWarned = new AtomicBoolean(false);

    public AmqpDlqStore(RabbitTemplate template,
                        AmqpDlqProperties properties,
                        AmqpDlqMessageBuilder messageBuilder,
                        boolean confirmModeEffective) {
        this.template = template;
        this.properties = properties;
        this.messageBuilder = messageBuilder;
        this.confirmModeEffective = confirmModeEffective;
    }

    /**
     * Registers per-stream overrides discovered via {@link AmqpDlqOptions} on
     * a Spring bean carrying {@code @ChangeStream + @DeadLetterQueue}.
     * Called by the satellite's bean post-processor on
     * {@code ApplicationReadyEvent}.
     */
    public void registerStream(String streamName, AmqpDlqOptions options) {
        overrides.put(streamName, options);
        log.debug("Registered AMQP DLQ overrides for stream '{}': exchange='{}', routingKey='{}', mandatory={}",
                streamName, options.exchange(), options.routingKey(), options.mandatory());
    }

    /**
     * Returns the override map (live view, unmodifiable in practice — used by
     * the autoconfig's ReturnsCallback to filter per-stream).
     */
    public ConcurrentMap<String, AmqpDlqOptions> getOverrides() {
        return overrides;
    }

    @Override
    public void save(FailedEvent event, DlqPolicy policy) {
        String streamName = event.streamName();
        AmqpDlqOptions opts = overrides.get(streamName);

        String exchange = (opts != null && !opts.exchange().isEmpty())
                ? opts.exchange()
                : properties.getExchange();
        String routingKey = (opts != null && !opts.routingKey().isEmpty())
                ? opts.routingKey()
                : properties.getRoutingKeyPrefix() + streamName;

        Message message = buildMessage(event, policy);

        if (confirmModeEffective) {
            template.invoke(operations -> {
                operations.send(exchange, routingKey, message);
                if (!operations.waitForConfirms(CONFIRM_TIMEOUT_MS)) {
                    throw new org.springframework.amqp.AmqpException(
                            "Broker NACK or timeout waiting for publisher confirm on stream '"
                                    + streamName + "' (exchange='" + exchange
                                    + "', routingKey='" + routingKey + "')");
                }
                return null;
            });
        } else {
            template.send(exchange, routingKey, message);
        }
    }

    private Message buildMessage(FailedEvent event, DlqPolicy policy) {
        String body = messageBuilder.toJson(event);
        MessageProperties props = new MessageProperties();
        props.setContentType("application/json");
        props.setContentEncoding("UTF-8");
        props.setMessageId(event.id());
        props.setHeader("flowwarden-event-id", event.id());
        props.setHeader("flowwarden-stream-name", event.streamName());
        props.setHeader("flowwarden-operation-type", event.operationType());
        props.setHeader("flowwarden-attempts", event.attempts());
        props.setHeader("flowwarden-status", event.status());
        if (event.firstAttemptAt() != null) {
            props.setHeader("flowwarden-first-attempt-at", event.firstAttemptAt().toEpochMilli());
        }
        props.setHeader("flowwarden-schema-version", AmqpDlqMessageBuilder.SCHEMA_VERSION);

        if (policy.retentionDays() > 0) {
            long ttlMs = TimeUnit.DAYS.toMillis(policy.retentionDays());
            props.setExpiration(String.valueOf(ttlMs));
        }

        return new Message(body.getBytes(StandardCharsets.UTF_8), props);
    }

    @Override
    public Optional<FailedEvent> findById(String id) {
        if (findByIdWarned.compareAndSet(false, true)) {
            log.warn("flowwarden-amqp: DlqStore.findById is not supported by this backend (publish-only). "
                    + "Subscribe an AMQP consumer to exchange '{}' (routing key pattern '{}*') "
                    + "for downstream processing of failed events.",
                    properties.getExchange(), properties.getRoutingKeyPrefix());
        }
        return Optional.empty();
    }

    @Override
    public List<FailedEvent> findByStreamName(String streamName) {
        if (findByStreamNameWarned.compareAndSet(false, true)) {
            log.warn("flowwarden-amqp: DlqStore.findByStreamName is not supported by this backend (publish-only). "
                    + "Subscribe an AMQP consumer to exchange '{}' (routing key pattern '{}*') "
                    + "for downstream processing of failed events.",
                    properties.getExchange(), properties.getRoutingKeyPrefix());
        }
        return List.of();
    }
}
