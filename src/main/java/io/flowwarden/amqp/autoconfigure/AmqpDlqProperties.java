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
package io.flowwarden.amqp.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the FlowWarden AMQP DLQ backend.
 *
 * <p>Bound to the {@code flowwarden.amqp} prefix. AMQP broker connection
 * properties (host, port, credentials, virtual host) remain configured
 * via the standard Spring Boot {@code spring.rabbitmq.*} namespace; this
 * class only covers FlowWarden-specific routing options.</p>
 */
@ConfigurationProperties("flowwarden.amqp")
public class AmqpDlqProperties {

    /**
     * Default AMQP exchange that failed events are published to. Per-stream
     * overrides are possible via {@code @AmqpDlqOptions(exchange = "...")}.
     */
    private String exchange = "flowwarden.dlq";

    /**
     * Default routing key prefix. The full routing key is computed as
     * {@code routingKeyPrefix + streamName} (e.g. {@code "dlq.orders-stream"}).
     * Per-stream full overrides are possible via
     * {@code @AmqpDlqOptions(routingKey = "...")}.
     */
    private String routingKeyPrefix = "dlq.";

    /**
     * Whether to wait for broker publisher confirms before
     * {@code AmqpDlqStore.save(...)} returns. When {@code true} (the default),
     * the call blocks until the broker ACKs the message; on NACK, timeout, or
     * broker crash before persist, an {@code AmqpException} is propagated and
     * the core emits {@code onEventDlqFailed}.
     *
     * <p>Requires {@code spring.rabbitmq.publisher-confirm-type=correlated}
     * (or {@code simple}); the autoconfig will force {@code correlated} if
     * the Spring property is not explicitly set. If the user has explicitly
     * configured {@code none}, the autoconfig logs a {@code WARN} and
     * disables confirm mode automatically.</p>
     *
     * <p>Set to {@code false} to fall back to fire-and-forget semantics (faster
     * but {@code onEventSentToDlq} can be a false positive on broker crash
     * pre-persist).</p>
     */
    private boolean confirmMode = true;

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public String getRoutingKeyPrefix() {
        return routingKeyPrefix;
    }

    public void setRoutingKeyPrefix(String routingKeyPrefix) {
        this.routingKeyPrefix = routingKeyPrefix;
    }

    public boolean isConfirmMode() {
        return confirmMode;
    }

    public void setConfirmMode(boolean confirmMode) {
        this.confirmMode = confirmMode;
    }
}
