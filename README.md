# FSM Library — Java Finite State Machine for Distributed Systems

A lightweight, production-ready **Java finite state machine (FSM) library** for building stateful, resumable workflows. Designed for distributed Java applications where workflow execution spans multiple HTTP requests, process restarts, and service replicas.

Define your workflow as a **type-safe state machine** in Java 17. Run it synchronously in a single process, or drive it across multiple HTTP requests with full failure recovery and automatic retry — without writing any orchestration plumbing yourself.

> **Status:** 1.0.0-RC3 — public API is stable; finalising enhancements before 1.0.0 GA.

---

## Why use this library?

Most Java workflow libraries are either too heavy (full BPM engines) or too simple (no persistence). This library sits in between: a focused FSM engine built specifically for **distributed Java microservices** that need:

- **Failure recovery** — sub-steps are checkpointed; the machine resumes exactly where it stopped after a crash or restart
- **Automatic retry** — configurable exponential backoff or fixed delay between retry attempts
- **Concurrent execution safety** — per-execution locking prevents two requests, or two service replicas, from processing the same workflow at once
- **Database persistence** — built-in JDBC support for PostgreSQL, MySQL, MariaDB, H2, SQLite, and Oracle
- **Spring Boot integration** — zero-config autoconfiguration with `fsm-spring-boot-starter-jdbc`

---

## Features

- **Fluent DSL** — define states, transitions, guards, and sub-steps with a readable Java builder
- **Sub-step tracking** — granular named work units; completed ones are skipped on resume
- **Snapshot-based persistence** — failures are checkpointed; execution resumes exactly at the failed step
- **Automatic retry** — exponential backoff, fixed delay, or no-auto-retry; pluggable retry policies
- **HTTP manager** — per-execution locking, context loading, and the full load→execute→save cycle in one call
- **Explicit initialization** — `initialize()` for explicit setup in initial state without triggering events
- **State validation** — safe `isInitialState()` and `isTerminal()` checks without hardcoding state names
- **Exception details** — `getRootCause()` on results for targeted error handling and recovery logic
- **Startup recovery** — `recoverPendingRetries()` reschedules in-flight retries after a process restart
- **Event listeners** — lifecycle hooks for observability, auditing, and metrics
- **JDBC persistence** — `JdbcSnapshotRepository` for distributed multi-replica deployments
- **Spring Boot autoconfiguration** — optional `fsm-spring-boot-starter-jdbc` for zero-config JDBC setup
- **Pluggable storage** — `InMemorySnapshotRepository` (tests), `FileSnapshotRepository` (single-JVM), or `JdbcSnapshotRepository` (distributed)
- **Pluggable execution locking** — `ExecutionLockProvider` SPI; `ReentrantExecutionLockProvider` (default, single-JVM) or `JdbcExecutionLockProvider` (distributed, TTL-bounded stale-lock takeover via the `fsm_execution_locks` table)
- **Spring-friendly** — class-based `StateConfigurer` and `SubStepHandler` interfaces for Spring dependency injection

---

## Quick start

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>net.hypercell</groupId>
    <artifactId>fsm-core</artifactId>
    <version>1.0.0-RC3</version>
</dependency>
```

Define and run a state machine:

```java
StateMachineDefinition<OrderContext> machine = StateMachine.<OrderContext>define("order-workflow")
    .initial("PENDING")
    .snapshotRepository(StateMachine.inMemoryRepository())
    .listener(StateMachine.loggingListener("[ORDER]"))
    .state("PENDING")
        .on("APPROVE").to("PROCESSING").end()
        .on("CANCEL").to("CANCELLED").end()
        .and()
    .state("PROCESSING")
        .subStep("reserve-stock",  ctx -> reserveStock(ctx))
        .subStep("charge-payment", ctx -> chargePayment(ctx))
        .subStep("send-email",     ctx -> sendConfirmation(ctx))
        .on("COMPLETE").to("SHIPPED").end()
        .and()
    .state("SHIPPED").terminal().and()
    .state("CANCELLED").terminal().and()
    .build();

StateMachineInstance<OrderContext> instance = machine.newInstance(new OrderContext(orderId));
instance.trigger("APPROVE");     // PENDING → PROCESSING (runs all 3 sub-steps)
instance.trigger("COMPLETE");    // PROCESSING → SHIPPED (terminal, status = COMPLETED)
```

For distributed deployments with Spring Boot and PostgreSQL, add the starter:

```xml
<dependency>
    <groupId>net.hypercell</groupId>
    <artifactId>fsm-spring-boot-starter-jdbc</artifactId>
    <version>1.0.0-RC3</version>
</dependency>
```

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/myapp
fsm.jdbc.dialect=postgresql
```

---

## Documentation

| Document | Description |
|---|---|
| [Concepts](docs/01-concepts.md) | Mental model — states, transitions, sub-steps, lifecycle |
| [Getting started](docs/02-getting-started.md) | Step-by-step tutorial |
| [Use cases](docs/03-use-cases.md) | Recipes: HTTP manager, auto-retry, Spring DI, guards, state validation, JDBC, exception handling |
| [Threading & safety](docs/04-threading-and-safety.md) | Thread-safety contracts for every class |
| [Persistence & retry](docs/05-persistence-and-retry.md) | Snapshots, SnapshotStatus, retry policies, InMemory/File/JDBC repositories, startup recovery |
| [API reference](docs/06-api-reference.md) | Full reference for all public types |
| [JDBC & Spring Boot](docs/08-jdbc-and-spring-boot.md) | Distributed deployments with `JdbcSnapshotRepository` and Spring Boot autoconfiguration |
| [Limitations & roadmap](docs/07-limitations.md) | Known gaps and future improvement ideas |

---

## Project structure

```
fsm-library/
├── fsm-core/                       Core library (Java 17, SLF4J logging)
│   └── src/main/java/io/hypercell/fsm/
│       ├── StateMachine.java       entry point (static factories)
│       ├── builder/                fluent DSL
│       ├── core/                   public interfaces (definitions, instances, managers)
│       ├── execution/              runtime engine (internal)
│       ├── exception/              typed exceptions
│       ├── failure/                failure dispositions & policies
│       ├── listener/               event bus & lifecycle callbacks
│       ├── lock/                   per-execution mutual exclusion
│       ├── manager/                HTTP request orchestration
│       ├── resume/                 snapshots & persistence
│       ├── retry/                  retry policies & scheduling
│       └── scope/                  per-execution ambient state (MDC, tracing)
│
├── fsm-jdbc/                       JDBC persistence (PostgreSQL, MySQL, MariaDB, H2, SQLite, Oracle)
│   ├── JdbcSnapshotRepository      distributed snapshots with optimistic locking
│   └── JdbcExecutionLockProvider   distributed per-execution lock (fsm_execution_locks, TTL takeover)
│
├── fsm-spring-boot-starter-jdbc/   Spring Boot autoconfiguration for JDBC
│   └── auto-configures JdbcSnapshotRepository and JdbcExecutionLockProvider using Spring's DataSource
│
└── fsm-examples/                   Runnable examples demonstrating all patterns
    ├── SynchronousWorkflowExample  direct instance use
    ├── HttpManagerExample          HTTP-driven manager with context loaders
    └── FileSnapshotRetryExample    file-based persistence with auto-retry
```

---

## License

Copyright 2026 Ahmed Mehanna. Licensed under the [Apache License, Version 2.0](LICENSE).
