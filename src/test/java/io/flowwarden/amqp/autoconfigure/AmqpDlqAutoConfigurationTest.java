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

import io.flowwarden.amqp.dlq.AmqpDlqMessageBuilder;
import io.flowwarden.amqp.dlq.AmqpDlqStore;
import io.flowwarden.amqp.internal.AmqpDlqOptionsRegistrar;
import io.flowwarden.stream.spi.DlqStore;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class AmqpDlqAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(AmqpDlqAutoConfiguration.class))
            .withUserConfiguration(MockRabbitTemplateConfig.class);

    @Test
    void declaresAmqpDlqStoreAndMessageBuilderWhenRabbitTemplatePresent() {
        runner.withPropertyValues("spring.rabbitmq.publisher-confirm-type=correlated")
                .run(ctx -> {
                    assertThat(ctx).hasSingleBean(AmqpDlqMessageBuilder.class);
                    assertThat(ctx).hasSingleBean(AmqpDlqStore.class);
                    assertThat(ctx).hasSingleBean(DlqStore.class);
                    assertThat(ctx).hasSingleBean(AmqpDlqOptionsRegistrar.class);
                    assertThat(ctx.getBean(DlqStore.class)).isInstanceOf(AmqpDlqStore.class);
                });
    }

    @Test
    void userProvidedDlqStoreBeanWins() {
        runner.withPropertyValues("spring.rabbitmq.publisher-confirm-type=correlated")
                .withUserConfiguration(UserDlqStoreConfig.class)
                .run(ctx -> {
                    // @ConditionalOnMissingBean(DlqStore.class) skips our bean
                    assertThat(ctx).doesNotHaveBean(AmqpDlqStore.class);
                    assertThat(ctx.getBean(DlqStore.class)).isSameAs(UserDlqStoreConfig.USER_BEAN);
                });
    }

    @Test
    void confirmModeDefaultsToTrue() {
        runner.withPropertyValues("spring.rabbitmq.publisher-confirm-type=correlated")
                .run(ctx -> {
                    AmqpDlqProperties props = ctx.getBean(AmqpDlqProperties.class);
                    assertThat(props.isConfirmMode()).isTrue();
                });
    }

    @Test
    void exchangeAndRoutingKeyPrefixHaveSensibleDefaults() {
        runner.run(ctx -> {
            AmqpDlqProperties props = ctx.getBean(AmqpDlqProperties.class);
            assertThat(props.getExchange()).isEqualTo("flowwarden.dlq");
            assertThat(props.getRoutingKeyPrefix()).isEqualTo("dlq.");
        });
    }

    @Test
    void overridesViaProperties() {
        runner.withPropertyValues(
                "flowwarden.amqp.exchange=custom-exchange",
                "flowwarden.amqp.routing-key-prefix=err.",
                "flowwarden.amqp.confirm-mode=false")
                .run(ctx -> {
                    AmqpDlqProperties props = ctx.getBean(AmqpDlqProperties.class);
                    assertThat(props.getExchange()).isEqualTo("custom-exchange");
                    assertThat(props.getRoutingKeyPrefix()).isEqualTo("err.");
                    assertThat(props.isConfirmMode()).isFalse();
                });
    }

    @Test
    void withoutRabbitTemplate_autoConfigDoesNotRun() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(AmqpDlqAutoConfiguration.class))
                .run(ctx -> assertThat(ctx).doesNotHaveBean(AmqpDlqStore.class));
    }

    @Configuration
    static class MockRabbitTemplateConfig {
        @Bean
        public RabbitTemplate rabbitTemplate() {
            // CachingConnectionFactory without an actual broker — RabbitTemplate creation
            // doesn't connect; tests don't exercise publish.
            CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
            return new RabbitTemplate(cf);
        }
    }

    @Configuration
    static class UserDlqStoreConfig {
        static final DlqStore USER_BEAN = DlqStore.noOp();

        @Bean
        public DlqStore userDlqStore() {
            return USER_BEAN;
        }
    }
}
