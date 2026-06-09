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
package io.flowwarden.amqp.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Per-stream overrides for the AMQP DLQ backend, applied on top of the global
 * defaults configured via {@code flowwarden.amqp.*} properties.
 *
 * <p>Used alongside {@link io.flowwarden.stream.annotation.ChangeStream} and
 * {@link io.flowwarden.stream.annotation.DeadLetterQueue}. The satellite scans
 * Spring beans carrying {@code @ChangeStream} + {@code @DeadLetterQueue} at
 * {@code ApplicationReadyEvent} time; if {@code @AmqpDlqOptions} is also
 * present, the values declared here override the global config for that
 * stream's failed-event publishes.</p>
 *
 * <p>Example:</p>
 * <pre>{@code
 * @ChangeStream(name = "payments-stream", collection = "payments")
 * @DeadLetterQueue(retentionDays = 30)
 * @AmqpDlqOptions(
 *     exchange = "alerts-critical",
 *     routingKey = "alert.payment.failed",
 *     mandatory = true)
 * public class PaymentsHandler { ... }
 * }</pre>
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface AmqpDlqOptions {

    /**
     * Override the AMQP exchange for this stream. Empty string ({@code ""},
     * the default) falls back to {@code flowwarden.amqp.exchange} (default
     * {@code "flowwarden.dlq"}).
     */
    String exchange() default "";

    /**
     * Override the routing key for this stream. Empty string ({@code ""}, the
     * default) falls back to the global pattern
     * {@code flowwarden.amqp.routing-key-prefix + streamName} (e.g.
     * {@code "dlq.orders-stream"}).
     */
    String routingKey() default "";

    /**
     * AMQP {@code mandatory} flag. When {@code true}, the broker returns the
     * message to the producer if no queue is bound to the routing key,
     * instead of silently dropping it. Use this for streams where
     * "no consumer" is an anomaly that must be detected.
     *
     * <p>Returned messages are logged at {@code WARN} by the satellite; the
     * application can also wire a {@code RabbitTemplate.ReturnsCallback} for
     * custom handling.</p>
     */
    boolean mandatory() default false;
}
