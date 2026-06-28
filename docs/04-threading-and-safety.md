# Threading and safety

This page documents the thread-safety contract for every class in the library. Read this before using the library in a multi-threaded application or a distributed deployment.

---

## Summary table

| Class / Interface | Thread-safe? | Notes |
|---|---|---|
| `StateMachineDefinition` | Yes — immutable | Safe to share across threads after `build()` |
| `StateMachineInstance` | **No** | One thread at a time; protected by the manager's lock |
| `ExecutionRecord` | **No** | Owned by the instance; same rule applies |
| `DefaultStateMachineManager` | Yes | Delegates per-`executionId` locking to `ExecutionLockProvider` |
| `ExecutionLockProvider` (SPI) | Depends on impl | Plug-in point for the locking strategy |
| `ReentrantExecutionLockProvider` | Yes | Default; `ConcurrentHashMap` of `ReentrantLock`; single-JVM only |
| `JdbcExecutionLockProvider` | Yes | Distributed lock via `fsm_execution_locks` table; stateless after construction |
| `InMemorySnapshotRepository` | Yes (single-JVM) | `ConcurrentHashMap`; not distributed |
| `FileSnapshotRepository` | Yes (single-JVM) | Atomic file writes; not distributed |
| `ThreadPoolRetryScheduler` | Yes | Daemon `ScheduledExecutorService` |
| `ExecutionSnapshot` | Yes — immutable | Safe to read from any thread |
| `EventBus` / listeners | **No** | Synchronous; runs on the calling thread |

---

## StateMachineDefinition — immutable after build

Once `build()` returns, the `StateMachineDefinition` is immutable. It is safe to share a single instance across any number of threads, requests, and retry callbacks.

```java
// Correct: one definition, many concurrent instances
@Bean
public StateMachineDefinition<OrderContext> orderMachine() {
    return StateMachine.<OrderContext>define("order-workflow")
        // ...
        .build();
}
```

---

## StateMachineInstance — NOT thread-safe

`StateMachineInstance` is mutable and **not thread-safe**. Its internal fields (`currentState`, `executionStatus`) are not `volatile`, and there are no `synchronized` blocks.

**Never pass an instance to another thread.** One thread must fully complete `trigger()` or `proceed()` before a different thread can call any method on the same instance.

In practice, instances are short-lived within a single request or task. If you use `StateMachineManager`, thread safety is handled for you — the manager ensures only one thread holds an instance for a given `executionId` at any time.

---

## DefaultStateMachineManager — per-executionId locking

The manager delegates per-`executionId` locking to an `ExecutionLockProvider`. The default provider uses a `ConcurrentHashMap<String, ReentrantLock>` (single-JVM). For distributed deployments, swap in `JdbcExecutionLockProvider`.

```
request 1: trigger("order-42", "APPROVE")
    → lockProvider.acquire("order-42") → handle acquired
    → load context, reconstitute, execute, save
    → handle.close() → lock released

request 2: trigger("order-42", "CANCEL")  [arrives while request 1 is running]
    → lockProvider.acquire("order-42") → ConcurrentExecutionException thrown immediately
    → map to HTTP 409
```

Key properties:

- **Non-blocking** — `acquire()` returns immediately or throws; it never queues the caller.
- **No deadlock risk** — each executionId gets its own independent lock; locks are never held in a hierarchy.
- **Pluggable** — swap `ExecutionLockProvider` implementations without changing any other code.
- **Lock cleanup (in-process)** — `ReentrantExecutionLockProvider` removes the map entry when no threads are waiting, preventing unbounded growth.

### What to catch

```java
try {
    ManagedTransitionResult<OrderContext> result = manager.trigger(orderId, event);
    // success
} catch (ConcurrentExecutionException e) {
    // HTTP 409 — client should retry after a short delay
} catch (ConcurrentRetryException e) {
    // A retry is already in progress for this executionId — do not proceed manually
}
```

---

## Single-JVM vs distributed deployments

| Layer | Single-JVM | Multi-JVM (distributed) |
|---|---|---|
| Execution locking | `ReentrantExecutionLockProvider` (default) | `JdbcExecutionLockProvider` (built-in) or custom SPI |
| Snapshot storage | `FileSnapshotRepository` | `JdbcSnapshotRepository` (built-in) or custom |
| Retry scheduling | `ThreadPoolRetryScheduler` | `recoverFailedExecutions()` from a leader node |

### Execution locking SPI

`ExecutionLockProvider` is a plug-in point. Swap implementations on the builder:

```java
// Default (single-JVM):
StateMachine.<OrderContext>define("order-workflow")
    // no .executionLockProvider() → uses ReentrantExecutionLockProvider automatically
    .build();

// Distributed (JDBC-backed):
StateMachine.<OrderContext>define("order-workflow")
    .executionLockProvider(new JdbcExecutionLockProvider(dataSource, new PostgreSqlDialect()))
    .build();
```

With Spring Boot, the `fsm-spring-boot-starter-jdbc` starter auto-configures a `JdbcExecutionLockProvider` bean when `fsm.jdbc.lock.enabled=true` (the default). Wire it into the definition bean; if none is wired the default `ReentrantExecutionLockProvider` is used.

### JdbcExecutionLockProvider — how it works

Backed by the `fsm_execution_locks` table (created by schema migration V3). Each in-flight execution occupies one row; releasing **deletes** the row, so the table stays sparse.

**Acquire (two-step, non-blocking):**
1. **UPDATE** — if a row exists but `locked_at` is older than the TTL (default 5 min), claim it (crashed-node takeover).
2. **INSERT** — no row exists (normal case or after release). The database PRIMARY KEY constraint guarantees exactly one concurrent INSERT succeeds; the loser receives a duplicate-key error converted to `ConcurrentExecutionException`.

**Release:** `DELETE WHERE execution_id=? AND locked_by=?` — the `AND locked_by=?` guard prevents a zombie process from deleting a lock taken over as stale.

### Snapshot optimistic locking

`JdbcSnapshotRepository` uses a `version` column for optimistic locking on save. Two replicas cannot commit conflicting updates to the same snapshot row — one will get a stale-version error, which surfaces as a `SnapshotException`.

### Remaining distributed limitations

- **Retry scheduling** — `ThreadPoolRetryScheduler` is in-process. For distributed retry, use `recoverFailedExecutions(maxAttempts)` from a leader-elected scheduled task (one replica).
- **Redis locking** — not provided; implement `ExecutionLockProvider` to plug in Redis `SET NX`.

---

## EventBus and listeners — synchronous, on the calling thread

`EventBus.publish()` iterates all registered listeners **sequentially and synchronously** on the thread that called `trigger()` or `proceed()`. There is no async dispatch, no thread pool, and no queueing.

Consequences:
- **Slow listeners slow down the machine.** Keep listeners fast. For heavy work (writing to a slow DB, calling an external API), publish to an async queue from the listener instead of doing the work inline.
- **Listener exceptions are caught and logged per-listener.** One failing listener does not stop event dispatch to subsequent listeners, and does not fail the state machine operation.
- **Listeners are not thread-safe by default.** If a listener instance is shared across machines, it must be thread-safe itself (because multiple machines may trigger concurrently from different threads).

---

## Retry scheduler — daemon threads

`ThreadPoolRetryScheduler` (and the builder's built-in inline scheduler) use daemon threads. They will stop when the JVM exits without needing explicit shutdown. For graceful shutdown:

```java
// If you kept a reference to the scheduler:
scheduler.shutdown(); // waits up to 30 seconds
```

The recovery executor in `DefaultStateMachineManager` is also a single-threaded daemon pool.

---

## What "NOT thread-safe" means for you

If you use the library **without** the manager (direct `StateMachineInstance` usage):

```java
// WRONG: two threads on one instance
new Thread(() -> instance.trigger("APPROVE")).start();
new Thread(() -> instance.trigger("CANCEL")).start(); // data corruption

// CORRECT: one thread owns the instance
instance.trigger("APPROVE"); // on thread A
// when fully done:
instance.trigger("COMPLETE"); // also on thread A (or safely handed off with visibility guarantee)
```

If you use the **manager**, you do not manage this yourself — the manager's lock ensures only one thread can hold an instance for a given `executionId` at any time. The instance is created, used, and discarded within a single lock scope.
