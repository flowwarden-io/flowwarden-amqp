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

import io.flowwarden.amqp.autoconfigure.AmqpDlqProperties;
import io.flowwarden.amqp.test.SharedAmqpContainer;
import io.flowwarden.stream.spi.DlqStore;
import io.flowwarden.stream.spi.testkit.DlqStoreContractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * The published testkit contract, finally adoptable by this publish-only
 * backend: since stream-core 1.0.0-rc.4 the read and count contracts are
 * opt-in capabilities ({@code supportsReplay()}/{@code supportsCount()},
 * both default {@code false} — matching the SPI where replay/count are
 * absent until a backend implements them). The write contract runs; the
 * read/count tests report as skipped assumptions, not failures.
 */
class AmqpDlqStoreContractIntegrationTest extends DlqStoreContractTest {

    private static final String EXCHANGE = "flowwarden.dlq.contract";

    private static CachingConnectionFactory connectionFactory;
    private static RabbitTemplate template;

    @BeforeAll
    static void setUpInfra() {
        connectionFactory = new CachingConnectionFactory(
                SharedAmqpContainer.RABBIT.getHost(),
                SharedAmqpContainer.RABBIT.getAmqpPort());
        connectionFactory.setUsername(SharedAmqpContainer.RABBIT.getAdminUsername());
        connectionFactory.setPassword(SharedAmqpContainer.RABBIT.getAdminPassword());
        template = new RabbitTemplate(connectionFactory);
        new RabbitAdmin(template).declareExchange(
                new TopicExchange(EXCHANGE, true, false));
    }

    @AfterAll
    static void tearDownInfra() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @Override
    protected DlqStore createDlqStore() {
        AmqpDlqProperties props = new AmqpDlqProperties();
        props.setExchange(EXCHANGE);
        props.setRoutingKeyPrefix("dlq.");
        return new AmqpDlqStore(template, props, new AmqpDlqMessageBuilder(), false);
    }

    @Override
    protected void cleanState() {
        // Publish-only: nothing to clean — every contract write lands on the
        // exchange and is dropped without a bound queue.
    }

    // supportsReplay()/supportsCount() deliberately NOT overridden: this
    // backend keeps the SPI's default reads (empty) and count (-1), so the
    // matching contract halves stay skipped by design.
}
