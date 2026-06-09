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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/**
 * Auto-configuration for the FlowWarden AMQP DLQ backend.
 *
 * <p>Wires {@link AmqpDlqStore} as the active {@link DlqStore} when a
 * {@link RabbitTemplate} is present in the context and no other
 * {@code DlqStore} bean has been declared.</p>
 *
 * <p>Resolves the effective publisher-confirm mode at boot from
 * {@code flowwarden.amqp.confirm-mode} combined with the Spring
 * {@code spring.rabbitmq.publisher-confirm-type} property: confirms require
 * both {@code confirm-mode=true} (the FlowWarden default) and the Spring
 * property set to {@code correlated} or {@code simple}. Misconfiguration
 * logs a {@code WARN} and falls back to fire-and-forget rather than failing
 * the application.</p>
 */
@AutoConfiguration(after = RabbitAutoConfiguration.class)
@ConditionalOnClass(RabbitTemplate.class)
@ConditionalOnBean(RabbitTemplate.class)
@EnableConfigurationProperties(AmqpDlqProperties.class)
public class AmqpDlqAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(AmqpDlqAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    public AmqpDlqMessageBuilder amqpDlqMessageBuilder() {
        return new AmqpDlqMessageBuilder();
    }

    @Bean
    @ConditionalOnMissingBean(DlqStore.class)
    public AmqpDlqStore amqpDlqStore(RabbitTemplate template,
                                      AmqpDlqProperties properties,
                                      AmqpDlqMessageBuilder messageBuilder,
                                      Environment environment) {
        boolean confirmModeEffective = resolveConfirmModeEffective(properties, environment);
        log.info("flowwarden-amqp: DlqStore active on exchange='{}', routingKeyPrefix='{}', confirmMode={}",
                properties.getExchange(), properties.getRoutingKeyPrefix(), confirmModeEffective);
        return new AmqpDlqStore(template, properties, messageBuilder, confirmModeEffective);
    }

    @Bean
    @ConditionalOnBean(AmqpDlqStore.class)
    @ConditionalOnMissingBean
    public AmqpDlqOptionsRegistrar amqpDlqOptionsRegistrar(ApplicationContext context,
                                                            AmqpDlqStore store,
                                                            RabbitTemplate template) {
        return new AmqpDlqOptionsRegistrar(context, store, template);
    }

    private boolean resolveConfirmModeEffective(AmqpDlqProperties properties, Environment environment) {
        if (!properties.isConfirmMode()) {
            return false;
        }
        String confirmType = environment.getProperty("spring.rabbitmq.publisher-confirm-type");
        boolean publisherConfirmsAvailable = "correlated".equalsIgnoreCase(confirmType)
                || "simple".equalsIgnoreCase(confirmType);
        if (!publisherConfirmsAvailable) {
            log.warn("flowwarden-amqp: flowwarden.amqp.confirm-mode=true but "
                    + "spring.rabbitmq.publisher-confirm-type is '{}' (must be 'correlated' or 'simple'). "
                    + "Publisher confirms disabled — set spring.rabbitmq.publisher-confirm-type=correlated "
                    + "to honour confirm-mode.",
                    confirmType != null ? confirmType : "<unset>");
            return false;
        }
        return true;
    }
}
