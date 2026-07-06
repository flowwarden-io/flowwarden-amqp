# flowwarden-amqp

[![CI](https://github.com/flowwarden-io/flowwarden-amqp/actions/workflows/ci.yml/badge.svg)](https://github.com/flowwarden-io/flowwarden-amqp/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java 17+](https://img.shields.io/badge/Java-17%2B-orange.svg)](https://openjdk.org/)
[![Spring Boot 3.x](https://img.shields.io/badge/Spring%20Boot-3.x-green.svg)](https://spring.io/projects/spring-boot)
[![Maven Central](https://img.shields.io/maven-central/v/io.flowwarden/flowwarden-amqp.svg)](https://central.sonatype.com/artifact/io.flowwarden/flowwarden-amqp)

AMQP-backed `DlqStore` implementation for [FlowWarden Stream Core](https://github.com/flowwarden-io/flowwarden-stream-core).

Publishes failed events to a topic exchange via Spring AMQP / `RabbitTemplate`. Multi-broker by construction — works with **RabbitMQ** (canonical reference), **LavinMQ**, **Qpid**, **ActiveMQ Classic**, **CloudAMQP**, **AWS MQ for RabbitMQ**, or any other broker speaking AMQP 0.9.1. No RabbitMQ-specific extensions used.

## What this is

`flowwarden-amqp` is a **publish-only** DLQ backend: when a stream's handler exhausts retries (or the user calls `ctx.sendToDlq(...)` manually), the failed event is serialized to JSON and published on a topic exchange. Downstream consumers subscribe with their own queues + bindings to do whatever they need: alerting (Slack, PagerDuty), re-processing (worker queue), archival (sink-to-blob), forensics.

What this is **not**:

- Not a persistent DLQ store with replay UI. `findById` / `findByStreamName` return empty — that's by design for a publish-only backend. If you need read + replay, keep `MongoDlqStore` from stream-core, or wait for a future `flowwarden-rabbit-streams` satellite.
- Not a generic event sink. This satellite implements the `DlqStore` SPI (failed events only). For forwarding normal events, see the `flowwarden-sink-*` family (not yet released).

## Add the dependency

### Maven (direct)

```xml
<dependency>
    <groupId>io.flowwarden</groupId>
    <artifactId>flowwarden-amqp</artifactId>
    <version>1.0.0-rc.1</version>
</dependency>
```

### Maven (via BOM)

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.flowwarden</groupId>
            <artifactId>flowwarden-bom</artifactId>
            <version>1.0.0-rc.3</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>io.flowwarden</groupId>
        <artifactId>flowwarden-stream-core</artifactId>
    </dependency>
    <dependency>
        <groupId>io.flowwarden</groupId>
        <artifactId>flowwarden-amqp</artifactId>
    </dependency>
</dependencies>
```

## Configure

Connection to your broker uses the standard Spring Boot `spring.rabbitmq.*` namespace:

```yaml
spring:
  rabbitmq:
    host: localhost
    port: 5672
    username: guest
    password: guest
    # Required if you want publisher confirms (recommended for DLQ):
    publisher-confirm-type: correlated
```

FlowWarden-specific routing options under `flowwarden.amqp.*`:

```yaml
flowwarden:
  amqp:
    exchange: flowwarden.dlq         # topic exchange (default)
    routing-key-prefix: "dlq."       # full key = prefix + streamName (default)
    confirm-mode: true               # block on publisher ACK (default)
```

### About `confirm-mode`

With `confirm-mode: true` (the default), `AmqpDlqStore.save(...)` waits for the broker to ACK the message before returning. A NACK, timeout, or broker crash before fsync propagates an `AmqpException` — which the core catches and routes to `StreamMetricsProvider.onEventDlqFailed(streamName, cause)`. Honest semantics: a successful `save(...)` means the message is durably on the broker.

`confirm-mode: true` requires `spring.rabbitmq.publisher-confirm-type=correlated` (or `simple`). If you set the FlowWarden flag without the Spring one, the autoconfig logs a `WARN` at startup and falls back to fire-and-forget rather than failing your app.

## What gets wired

When `RabbitTemplate` is on the classpath and a bean is available, `AmqpDlqAutoConfiguration` declares:

| Bean | Role |
|---|---|
| `AmqpDlqMessageBuilder` | Serializes `FailedEvent` to JSON (handles BSON `documentKey` / `resumeToken` / `fullDocument`) |
| `AmqpDlqStore` (`DlqStore`) | Publishes via `RabbitTemplate`; only created if no other `DlqStore` bean exists (user-defined beans win) |
| `AmqpDlqOptionsRegistrar` | Scans `@ChangeStream + @DeadLetterQueue + @AmqpDlqOptions` beans at startup and registers per-stream overrides |

## Per-stream overrides via `@AmqpDlqOptions`

To override the global exchange / routing key / mandatory flag for a specific stream:

```java
@ChangeStream(name = "payments-stream", collection = "payments")
@DeadLetterQueue(retentionDays = 30)
@AmqpDlqOptions(
    exchange = "alerts-critical",            // override default exchange
    routingKey = "alert.payment.failed",     // override full routing key
    mandatory = true)                         // require at least one bound queue
public class PaymentsHandler {

    @OnInsert
    public void onPayment(ChangeStreamContext<?> ctx) { ... }
}
```

Defaults (empty string / `false`) fall back to the global config under `flowwarden.amqp.*`. The annotation is scanned at `ApplicationReadyEvent`; beans defined later (via the builder API) are not yet supported in rc.1.

### About `mandatory`

When at least one stream sets `mandatory = true`, the satellite enables the `mandatory` flag on the underlying `RabbitTemplate` and installs a `ReturnsCallback` that logs a `WARN` when a message addressed to a `mandatory=true` stream comes back unrouted (no queue bound to the routing key). Returns for streams without `mandatory=true` are silently consumed — preserving fire-and-forget semantics for them.

## AMQP message headers

Every published message carries the following headers (filter / route without parsing the body):

| Header | Type | Example |
|---|---|---|
| `flowwarden-event-id` | String | `"01HNVZ..."` (FailedEvent.id) |
| `flowwarden-stream-name` | String | `"payments-stream"` |
| `flowwarden-operation-type` | String | `"INSERT"`, `"UPDATE"`, `"DELETE"`, `"REPLACE"` |
| `flowwarden-attempts` | int | `3` |
| `flowwarden-status` | String | `"PENDING"` |
| `flowwarden-first-attempt-at` | long | epoch ms |
| `flowwarden-schema-version` | int | `1` (bumped on breaking JSON body changes) |

## Consumer pattern

A downstream application binds its own queue to the exchange and consumes:

```java
@Configuration
public class DlqConsumerConfig {

    @Bean
    public TopicExchange dlqExchange() {
        return new TopicExchange("flowwarden.dlq", true, false);
    }

    @Bean
    public Queue alertQueue() {
        return new Queue("my-app.dlq-alert", true);
    }

    @Bean
    public Binding alertBinding(Queue alertQueue, TopicExchange dlqExchange) {
        return BindingBuilder.bind(alertQueue).to(dlqExchange).with("dlq.payments-*");
    }
}

@Component
public class DlqListener {

    @RabbitListener(queues = "my-app.dlq-alert")
    public void onFailure(
            @Payload String json,
            @Header("flowwarden-event-id") String eventId,
            @Header("flowwarden-stream-name") String streamName,
            @Header("flowwarden-attempts") int attempts) {
        // alerting, archival, replay, etc.
    }
}
```

The JSON body conforms to the `FailedEvent` record from `flowwarden-stream-core`. Use a lenient parser (Jackson default `FAIL_ON_UNKNOWN_PROPERTIES=false`) to remain forward-compatible across schema additions; check `flowwarden-schema-version` for breaking changes.

## Cohabitation with `MongoDlqStore`

If you want both persistence (Mongo) and downstream notification (AMQP), declare a composite `DlqStore` yourself:

```java
@Configuration
public class CompositeDlqConfig {

    @Bean
    @Primary
    public DlqStore compositeDlqStore(MongoDlqStore mongo, AmqpDlqStore amqp) {
        return new DlqStore() {
            @Override
            public void save(FailedEvent event, DlqPolicy policy) {
                mongo.save(event, policy);   // persist first
                try {
                    amqp.save(event, policy); // then notify
                } catch (RuntimeException e) {
                    // log but don't fail the save — Mongo persistence already succeeded
                    LoggerFactory.getLogger(getClass())
                            .warn("AMQP notify failed (Mongo persistence OK): {}", e.getMessage());
                }
            }
            @Override public Optional<FailedEvent> findById(String id) { return mongo.findById(id); }
            @Override public List<FailedEvent> findByStreamName(String s) { return mongo.findByStreamName(s); }
        };
    }
}
```

Pick the error policy that matches your operational priorities (Mongo-first / AMQP-first / fail-fast / best-effort).

## Compatibility matrix

| Component | Version |
|---|---|
| Java | 17+ |
| Spring Boot | 3.x |
| Spring AMQP | 3.x (transitively) |
| `flowwarden-stream-core` | 1.0.0-rc.3+ |
| RabbitMQ (reference) | 3.8+ |
| LavinMQ | 2.x+ (tested ad-hoc, not in CI) |
| Apache Qpid | 8.x+ (tested ad-hoc, not in CI) |

## FlowWarden Ecosystem

| Component | Description | License |
|-----------|-------------|---------|
| **[flowwarden-stream-core](https://github.com/flowwarden-io/flowwarden-stream-core)** | Declarative MongoDB Change Streams library for Spring Boot | Apache 2.0 |
| **[flowwarden-javers](https://github.com/flowwarden-io/flowwarden-javers)** | Native Javers audit stream integration | Apache 2.0 |
| **[flowwarden-redis](https://github.com/flowwarden-io/flowwarden-redis)** | Redis-backed `LockService` and `CheckpointStore` backends | Apache 2.0 |
| **[flowwarden-amqp](https://github.com/flowwarden-io/flowwarden-amqp)** | AMQP (RabbitMQ) publish-only dead-letter queue store | Apache 2.0 |
| **flowwarden-rabbit-streams** | RabbitMQ Streams-backed dead-letter queue store | Apache 2.0 |
| **flowwarden-reporter** | Connects your streams to FlowWarden Console for monitoring | Apache 2.0 |
| **FlowWarden Console** | Dashboard for monitoring, alerting, and managing Change Streams | Commercial |

## Documentation

Full documentation is available at **[docs.flowwarden.io](https://docs.flowwarden.io)** — start with the [AMQP backend guide](https://docs.flowwarden.io/amqp).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[Apache License 2.0](LICENSE).
