# Changelog

All notable changes to FlowWarden AMQP will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- The published `DlqStoreContractTest` is now adopted: the stream-core 1.0.0-rc.4 testkit made the read/count contracts opt-in capabilities, so the write contract finally fits a publish-only backend (read/count tests report as skipped assumptions by design).

### Changed
- `flowwarden-stream-core` baseline: 1.0.0-rc.4.

### Removed
- The fake read implementations (`findById`/`findByStreamName` returning empty with a one-shot WARN) are gone: since stream-core 1.0.0-rc.4 the SPI reads are `default` (empty) and `count` defaults to `-1` ("cannot count"), so this publish-only backend is honest by construction — no warning needed, and the core never emits a lying backlog gauge for it. Point an AMQP consumer at the configured exchange for downstream processing (see README).

### Fixed

### Deprecated

### Security

## [1.0.0-rc.1] — 2026-07-06

### Added
- Initial bootstrap of `flowwarden-amqp` — AMQP 0.9.1 publish-only `DlqStore` implementation for FlowWarden Stream Core. Multi-broker by construction: RabbitMQ canonical, also LavinMQ / Qpid / ActiveMQ Classic / CloudAMQP / AWS MQ for RabbitMQ.
- `AmqpDlqStore` publishes failed events to a topic exchange (default `flowwarden.dlq`, routing key `dlq.{streamName}`) via Spring AMQP `RabbitTemplate`. `save(...)` propagates `AmqpException` so the core's `onEventDlqFailed(streamName, cause)` signal fires on broker outages.
- Publisher confirms enabled by default (`flowwarden.amqp.confirm-mode=true`). `save(...)` blocks on broker ACK; NACK / timeout / crash-before-persist propagate `AmqpException`. Requires `spring.rabbitmq.publisher-confirm-type=correlated`; misconfiguration logs `WARN` and falls back to fire-and-forget.
- AMQP message headers on every publish: `flowwarden-event-id`, `flowwarden-stream-name`, `flowwarden-operation-type`, `flowwarden-attempts`, `flowwarden-status`, `flowwarden-first-attempt-at`, `flowwarden-schema-version=1`. Consumers can filter or route on headers without parsing the body.
- `@AmqpDlqOptions(exchange, routingKey, mandatory)` annotation for per-stream topology overrides. Lives in `io.flowwarden.amqp.annotation` — the satellite scans Spring beans carrying `@ChangeStream + @DeadLetterQueue` at `ApplicationReadyEvent` and registers the overrides into `AmqpDlqStore`. No coupling with `flowwarden-stream-core` annotations; the core never knows AMQP exists.
- JSON body serialization via `AmqpDlqMessageBuilder`. MongoDB BSON types (`BsonValue documentKey`, `BsonDocument resumeToken`, `Document fullDocument`) handled with manual conversion; `Instant` fields ISO-8601 via Jackson `JavaTimeModule`.
- `findById` / `findByStreamName` are degraded by design (publish-only backend): return empty `Optional` / `List` and log a one-shot `WARN` directing users to subscribe an AMQP consumer.

### Changed

### Removed

### Fixed

### Deprecated

### Security

[Unreleased]: https://github.com/flowwarden-io/flowwarden-amqp/compare/v1.0.0-rc.1...HEAD
[1.0.0-rc.1]: https://github.com/flowwarden-io/flowwarden-amqp/releases/tag/v1.0.0-rc.1
