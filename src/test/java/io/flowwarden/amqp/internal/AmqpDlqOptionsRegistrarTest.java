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
package io.flowwarden.amqp.internal;

import io.flowwarden.amqp.annotation.AmqpDlqOptions;
import io.flowwarden.amqp.autoconfigure.AmqpDlqProperties;
import io.flowwarden.amqp.dlq.AmqpDlqMessageBuilder;
import io.flowwarden.amqp.dlq.AmqpDlqStore;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.StaticApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

class AmqpDlqOptionsRegistrarTest {

    private RabbitTemplate template;
    private AmqpDlqStore store;

    @BeforeEach
    void setUp() {
        // Real RabbitTemplate + ConnectionFactory pointing nowhere — never opened in these tests.
        template = new RabbitTemplate(new CachingConnectionFactory("unused-host"));
        AmqpDlqProperties props = new AmqpDlqProperties();
        store = new AmqpDlqStore(template, props, new AmqpDlqMessageBuilder(), false);
    }

    @Test
    void registerOnStartup_streamWithAllThreeAnnotations_registersWithStore() {
        ApplicationContext context = contextWith("paymentsHandler", new PaymentsHandlerWithOverrides());

        new AmqpDlqOptionsRegistrar(context, store, template).registerOnStartup();

        assertThat(store.getOverrides()).containsOnlyKeys("payments-stream");
        AmqpDlqOptions registered = store.getOverrides().get("payments-stream");
        assertThat(registered.exchange()).isEqualTo("alerts-critical");
        assertThat(registered.routingKey()).isEqualTo("alert.payment.failed");
        assertThat(registered.mandatory()).isFalse();
    }

    @Test
    void registerOnStartup_streamWithoutAmqpDlqOptions_notRegistered() {
        ApplicationContext context = contextWith("ordersHandler", new OrdersHandlerNoOverrides());

        new AmqpDlqOptionsRegistrar(context, store, template).registerOnStartup();

        assertThat(store.getOverrides()).isEmpty();
    }

    @Test
    void registerOnStartup_streamWithDlqDisabled_notRegistered() {
        ApplicationContext context = contextWith("disabledHandler", new HandlerWithDlqDisabled());

        new AmqpDlqOptionsRegistrar(context, store, template).registerOnStartup();

        assertThat(store.getOverrides()).isEmpty();
    }

    @Test
    void registerOnStartup_streamWithoutDlqAnnotation_notRegistered() {
        ApplicationContext context = contextWith("plainHandler", new PlainHandler());

        new AmqpDlqOptionsRegistrar(context, store, template).registerOnStartup();

        assertThat(store.getOverrides()).isEmpty();
    }

    @Test
    void registerOnStartup_anyMandatoryStream_enablesTemplateMandatory() {
        ApplicationContext context = contextWith("paymentsHandler", new PaymentsHandlerWithMandatoryTrue());
        assertThat(template.isMandatoryFor(dummyMessage())).isFalse(); // baseline

        new AmqpDlqOptionsRegistrar(context, store, template).registerOnStartup();

        // setMandatory(true) was called on the template
        assertThat(template.isMandatoryFor(dummyMessage())).isTrue();
        assertThat(store.getOverrides()).containsKey("payments-stream");
    }

    @Test
    void registerOnStartup_noMandatoryStream_templateMandatoryUntouched() {
        ApplicationContext context = contextWith("paymentsHandler", new PaymentsHandlerWithOverrides());
        assertThat(template.isMandatoryFor(dummyMessage())).isFalse(); // baseline

        new AmqpDlqOptionsRegistrar(context, store, template).registerOnStartup();

        assertThat(template.isMandatoryFor(dummyMessage())).isFalse();
    }

    /**
     * Builds a minimal Spring context containing a single bean carrying the
     * test annotations. Uses {@link StaticApplicationContext} which lets us
     * register a singleton instance directly without an interface mock.
     */
    private ApplicationContext contextWith(String beanName, Object bean) {
        StaticApplicationContext ctx = new StaticApplicationContext();
        ctx.getBeanFactory().registerSingleton(beanName, bean);
        ctx.refresh();
        return ctx;
    }

    private static Message dummyMessage() {
        return new Message(new byte[0], new MessageProperties());
    }

    // Test fixtures: real classes carrying real annotations so AnnotationUtils.findAnnotation works.

    @ChangeStream(name = "payments-stream", collection = "payments")
    @DeadLetterQueue
    @AmqpDlqOptions(exchange = "alerts-critical", routingKey = "alert.payment.failed", mandatory = false)
    static class PaymentsHandlerWithOverrides {}

    @ChangeStream(name = "payments-stream", collection = "payments")
    @DeadLetterQueue
    @AmqpDlqOptions(mandatory = true)
    static class PaymentsHandlerWithMandatoryTrue {}

    @ChangeStream(name = "orders-stream", collection = "orders")
    @DeadLetterQueue
    static class OrdersHandlerNoOverrides {}

    @ChangeStream(name = "disabled-stream", collection = "disabled")
    @DeadLetterQueue(enabled = false)
    @AmqpDlqOptions(exchange = "x")
    static class HandlerWithDlqDisabled {}

    @ChangeStream(name = "plain-stream", collection = "plain")
    static class PlainHandler {}
}
