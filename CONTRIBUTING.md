# Contributing to FlowWarden AMQP

Thank you for your interest in contributing to FlowWarden AMQP! Every contribution matters — whether it's a bug report, a feature suggestion, a documentation fix, or a code change.

Please note that this project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Ways to Contribute

> **Please open an issue before submitting a PR**, both for bug fixes and new features. This avoids wasted work on changes that may not align with the project direction, and keeps reviews focused.

- **Report bugs** — Open an issue with clear reproduction steps, expected vs. actual behavior, and your environment (Java version, Spring Boot version, AMQP broker + version). For non-trivial bugs, please provide a **runnable test case** (pushed to your fork) that isolates the issue. This dramatically speeds up triage and resolution.
- **Suggest features** — Open an issue describing the use case and why it would benefit the project.
- **Submit pull requests** — Code, tests, documentation improvements are all welcome.
- **Improve documentation** — Typos, unclear explanations, missing examples — every bit helps.

## Development Setup

### Prerequisites

- Java 17+ (tested on 17 and 21)
- Maven 3.8+
- Docker (required by Testcontainers)

### Clone & Build

```bash
git clone https://github.com/flowwarden-io/flowwarden-amqp.git
cd flowwarden-amqp
./mvnw clean verify
```

> **Note:** You do not need a local AMQP broker installation. Testcontainers automatically provisions a RabbitMQ container during integration tests.

> **Note:** When `main` temporarily depends on a `-SNAPSHOT` of `flowwarden-stream-core` (between core releases), resolve it either by configuring a GitHub PAT with `read:packages` for server id `github` in your `~/.m2/settings.xml`, or by building the core locally: `git clone https://github.com/flowwarden-io/flowwarden-stream-core && cd flowwarden-stream-core && ./mvnw install -DskipTests`.

## Architecture Constraints

Before writing code, please be aware of the following design points:

- **Minimal dependencies.** The library depends on `flowwarden-stream-core` (for the `DlqStore` SPI), `spring-boot-starter-amqp` (Spring AMQP / RabbitTemplate), `bson` (transitively from core, used for `FailedEvent` field serialization), and Spring Boot autoconfigure. Any new compile-scope dependency must be justified and approved via an issue before implementation.
- **Publish-only by design.** This backend implements `DlqStore.save(...)` against a topic exchange; `findById` / `findByStreamName` are degraded (empty + one-shot WARN). Read-side support would require Rabbit Streams (a separate satellite, not this one). PRs adding read paths here will be redirected.
- **Multi-broker AMQP 0.9.1.** The implementation uses only standard AMQP 0.9.1 features. RabbitMQ is the canonical reference (CI runs against `rabbitmq:3.13-management`), but the code must remain compatible with LavinMQ, Qpid, ActiveMQ Classic, etc. No RabbitMQ-specific extensions (streams, super-streams, x-delayed-message-exchange plugin) are accepted on this satellite.
- **Error propagation.** `AmqpDlqStore.save(...)` MUST propagate `AmqpException` instead of swallowing them, so the core's `onEventDlqFailed(streamName, cause)` metric signal fires on broker outages. No try/catch in the hot path.
- **Imperative only for rc.1.** No `ReactiveAmqpDlqStore` yet — the core off-loads `DlqStore.save` to `Schedulers.boundedElastic()`, so a blocking impl works fine for both imperative and reactive applications. Reactive variant added later only if explicitly requested.

## Coding Standards

- **Java 17 target.** Use records, pattern matching, `var`, sealed classes where they improve readability.
- **No Lombok.**
- **Apache 2.0 license headers** on every `.java` file (enforced by `license-maven-plugin` — run `./mvnw license:format` to apply).
- **Tests required.** New behavior needs new tests. Bug fixes need a regression test.
- **No new third-party dependencies** without prior discussion in an issue.

## Commit & PR Workflow

- Branch from `main` with a descriptive name: `feat/short-description`, `fix/issue-id-slug`, `chore/slug`.
- Follow [Conventional Commits](https://www.conventionalcommits.org/) — `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`.
- **DCO sign-off required** — `git commit -s` (or `git config --global format.signOff true`).
- **Signed commits required** — SSH ed25519 signature, enforced by branch protection.
- One concern per PR. Smaller PRs ship faster.
- Update `CHANGELOG.md` under `[Unreleased]` for any user-visible change.
- Wait for CI green before requesting review.

## Reporting Security Vulnerabilities

Please **do not** open public GitHub issues for security vulnerabilities. See [SECURITY.md](SECURITY.md) for the responsible disclosure process.

## License

By contributing to FlowWarden AMQP, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
