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
package io.flowwarden.amqp.test;

import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Singleton RabbitMQ container shared across all integration tests of this
 * module. Started lazily on first class load; stopped via a JVM shutdown hook.
 *
 * <p>RabbitMQ is the canonical reference broker for AMQP 0.9.1 in
 * {@code flowwarden-amqp} CI. Cross-broker testing (LavinMQ, Qpid) is out of
 * scope for rc.1.</p>
 */
public final class SharedAmqpContainer {

    public static final RabbitMQContainer RABBIT =
            new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.13-management"));

    static {
        RABBIT.start();
        Runtime.getRuntime().addShutdownHook(new Thread(RABBIT::stop));
    }

    private SharedAmqpContainer() {}
}
