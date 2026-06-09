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
import io.flowwarden.amqp.dlq.AmqpDlqStore;
import io.flowwarden.stream.annotation.ChangeStream;
import io.flowwarden.stream.annotation.DeadLetterQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.AnnotationUtils;

import java.util.Map;

/**
 * Scans Spring beans for {@link ChangeStream} + {@link DeadLetterQueue} +
 * {@link AmqpDlqOptions} at {@code ApplicationReadyEvent} and registers the
 * AMQP-specific overrides into {@link AmqpDlqStore}.
 *
 * <p>Also activates RabbitTemplate's {@code mandatory} flag and installs a
 * {@code ReturnsCallback} if at least one stream declares
 * {@code @AmqpDlqOptions(mandatory = true)}. The callback filters by
 * {@code flowwarden-stream-name} header and only logs at {@code WARN} for
 * streams that explicitly requested the mandatory semantics; returns for
 * non-mandatory streams are silently consumed (preserving the
 * fire-and-forget contract of {@code @AmqpDlqOptions.mandatory = false}).</p>
 *
 * <p>Annotation-based streams only — builder API support is out of scope for
 * rc.1.</p>
 */
public class AmqpDlqOptionsRegistrar {

    private static final Logger log = LoggerFactory.getLogger(AmqpDlqOptionsRegistrar.class);

    private final ApplicationContext context;
    private final AmqpDlqStore store;
    private final RabbitTemplate template;

    public AmqpDlqOptionsRegistrar(ApplicationContext context,
                                    AmqpDlqStore store,
                                    RabbitTemplate template) {
        this.context = context;
        this.store = store;
        this.template = template;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void registerOnStartup() {
        Map<String, Object> beans = context.getBeansWithAnnotation(ChangeStream.class);
        boolean anyMandatory = false;

        for (Object bean : beans.values()) {
            Class<?> targetClass = AopUtils.getTargetClass(bean);
            ChangeStream cs = AnnotationUtils.findAnnotation(targetClass, ChangeStream.class);
            if (cs == null) {
                continue;
            }
            DeadLetterQueue dlq = AnnotationUtils.findAnnotation(targetClass, DeadLetterQueue.class);
            if (dlq == null || !dlq.enabled()) {
                continue;
            }
            AmqpDlqOptions opts = AnnotationUtils.findAnnotation(targetClass, AmqpDlqOptions.class);
            if (opts == null) {
                continue;
            }
            store.registerStream(cs.name(), opts);
            if (opts.mandatory()) {
                anyMandatory = true;
            }
        }

        if (anyMandatory) {
            template.setMandatory(true);
            template.setReturnsCallback(this::onReturn);
            log.info("flowwarden-amqp: at least one stream declares @AmqpDlqOptions(mandatory=true); "
                    + "RabbitTemplate mandatory flag enabled, ReturnsCallback installed.");
        }
    }

    private void onReturn(ReturnedMessage returned) {
        Object streamNameHeader = returned.getMessage()
                .getMessageProperties()
                .getHeaders()
                .get("flowwarden-stream-name");
        String streamName = streamNameHeader != null ? streamNameHeader.toString() : "<unknown>";
        AmqpDlqOptions opts = store.getOverrides().get(streamName);
        if (opts != null && opts.mandatory()) {
            log.warn("flowwarden-amqp: returned message for stream '{}' (no queue bound to routing key '{}') — "
                    + "replyCode={}, replyText='{}'",
                    streamName, returned.getRoutingKey(),
                    returned.getReplyCode(), returned.getReplyText());
        }
        // Silent for non-mandatory streams: preserves the fire-and-forget contract.
    }
}
