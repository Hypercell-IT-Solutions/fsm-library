# Persistence and retry

This page explains how the library saves progress, handles failures, and automatically or manually retries from the last safe point.

---

## The snapshot model

When a sub-step fails, the library saves an `ExecutionSnapshot` to the configured `SnapshotRepository`. The snapshot captures:

- `executionId` — the business entity ID (used as the storage key)
- `machineDefinitionId` — which machine type this execution belongs to
- `currentStateName` — where the machine is positioned
- `failedStateName` / `failedSubStepName` — the exact point of failure
- `completedSubStepResults` — a map of sub-steps that already succeeded
- `attemptNumber` — how many times we have tried (starts at 1)
- `lastErrorMessage` / `lastErrorType` — error details for retry policy decisions
- `status` — the snapshot's own lifecycle status (see below)
- `lastFailedAt` / `scheduledRetryAt` — timestamps

### What is NOT in the snapshot

- **The context object** is never serialized. On resume, the library calls `contextLoader(executionId)` to load a fresh copy. Your context must be loadable by ID.
- **Incomplete or failed sub-step results** — only successful results are stored. The failed step will be re-executed on resume.

### Snapshot key

Completed sub-steps are stored with a composite key: `"stateName::subStepName"`. This means a sub-step named `"charge-payment"` in state `"PROCESSING"` is stored as `"PROCESSING::charge-payment"`.

The `::` separator is reserved — state names and sub-step names that contain `::` are rejected at `build()` time.

---

## Context on resume

This is the most important constraint to understand when building workflows with multiple interdependent sub-steps.

### The problem

On resume, the library loads a **fresh context** by calling `contextLoader(executionId)`. Completed sub-steps are skipped — their code does not run. If a skipped sub-step mutated the context, that mutation is gone in the fresh context. Any subsequent sub-step that depends on it will receive a context that never had the value set.

```
State: PROCESSING
  sub-step 1 "reserve-stock"   → runs, sets ctx.reservationId = "RSV-42"  [success]
  sub-step 2 "charge-payment"  → fails (payment gateway timeout)

Snapshot saved: step 1 completed, step 2 failed.

--- process restarts ---

Resume:
  contextLoader("order-42") called → returns fresh OrderContext
    → ctx.reservationId is null  ← not restored!
  sub-step 1 skipped
  sub-step 2 runs → reads ctx.reservationId → NullPointerException
```

### The solution: persist intermediate results in the sub-step that produces them

Every sub-step that produces data consumed by a later step must write that data to durable storage as part of its own work. The `contextLoader` must then read and restore all such intermediate results.

```java
// Sub-step 1: reserve stock
.subStep("reserve-stock", ctx -> {
    String reservationId = inventoryService.reserve(ctx.getOrderId(), ctx.getItems());
    ctx.setReservationId(reservationId);

    // Persist the result so contextLoader can restore it on resume
    orderRepository.saveReservationId(ctx.getOrderId(), reservationId);

    return ActionResult.success();
})

// Sub-step 2: charge payment — depends on reservationId being present
.subStep("charge-payment", ctx -> {
    // reservationId must be available whether this is first run or a resume
    paymentService.charge(ctx.getOrderId(), ctx.getAmount(), ctx.getReservationId());
    return ActionResult.success();
})
```

```java
// contextLoader must restore all intermediate results, not just the base entity
Function<String, OrderContext> contextLoader = orderId -> {
    Order order = orderRepository.findById(orderId);
    OrderContext ctx = new OrderContext(order);

    // Restore intermediate results produced by completed sub-steps
    String reservationId = orderRepository.findReservationId(orderId);
    if (reservationId != null) {
        ctx.setReservationId(reservationId); // present if sub-step 1 completed
    }

    return ctx;
};
```

### Design rule

> **Each sub-step is responsible for its own durability.** If a sub-step produces data, it must save that data to durable storage before returning `ActionResult.success()`. The `contextLoader` reconstructs the full context as it would have been at the point of failure — not just the base entity, but all intermediate results from completed sub-steps.

This keeps sub-steps independently retryable and the resume mechanism predictable.

---

## SnapshotStatus lifecycle

The snapshot has its own status, separate from `ExecutionStatus` (the live instance status). The persisted status encodes precisely where in its lifecycle a workflow is at rest between request boundaries.

```
   (new execution)
        │ initial state entered / sub-steps run
        ▼
     RUNNING ──── sub-steps done, non-terminal ──────────────────────► WAITING
        │                                                                  │
        │ sub-step fails                                          next event arrives
        ▼                                                                  │
      FAILED ◄──────────────── retry also fails ──────────────── RUNNING (retry)
        │                                                                  │
        │ RetryPolicy.shouldRetry() == true                        retry succeeds
        ▼                                                                  │
  RETRY_SCHEDULED ─── scheduled retry fires ──────────────────────────────┤
                                                                           │
                                              terminal state reached       │
                                                      ▼                   │
                                                  TERMINATED ◄────────────┘
                                              (snapshot retained)
```

**Precise meanings:**

| Status | Meaning |
|---|---|
| `RUNNING` | Execution is **actively processing sub-steps**. A crash leaves the snapshot here. The startup sweep queries `WHERE status = 'RUNNING'` to find interrupted executions cheaply via the status index. |
| `WAITING` | Sub-steps completed; machine is **parked at a non-terminal state** awaiting the next event. This is the normal at-rest status between two transitions. `listInterrupted()` never returns `WAITING` rows. |
| `TERMINATED` | A terminal state was reached successfully. The snapshot is retained so that subsequent `trigger()` or `proceed()` calls correctly throw `CompletedMachineException`. Call `repository.delete(executionId)` to clean up. |
| `FAILED` | A sub-step failed; waiting for manual or scheduled retry. |
| `RETRY_SCHEDULED` | An automatic retry has been scheduled. Do not call `manualRetry()` here; the scheduled retry will fire. Cancel it first if you must force an immediate retry. |

Key rules:
- **`RETRY_SCHEDULED`** — do not call `manualRetry()` while here; the scheduled retry will fire. If you must cancel it, cancel the retry via the scheduler before calling `manualRetry()`. `trigger()` throws `IllegalTriggerStateException` on RETRY_SCHEDULED.
- **`RUNNING`** — execution is actively processing sub-steps. A crash leaves the snapshot here; `listInterrupted()` returns these rows for the startup sweep. `trigger()` throws `IllegalTriggerStateException` on RUNNING — call `resume()` first.
- **`WAITING`** — normal at-rest status; the machine is parked between transitions. These rows are NOT returned by `listInterrupted()`. This is the only non-absent state where `trigger()` succeeds.
- **`FAILED`** — `trigger()` throws `IllegalTriggerStateException` — call `proceed()` first.
- **`TERMINATED`** — execution finished. The snapshot is retained with this status so that subsequent `trigger()` or `proceed()` calls correctly throw `CompletedMachineException`. Call `repository.delete(executionId)` to clean up when you no longer need the record.

---

## SnapshotRepository implementations

### InMemorySnapshotRepository (testing)

```java
SnapshotRepository repo = StateMachine.inMemoryRepository();
```

- Backed by a `ConcurrentHashMap<String, ExecutionSnapshot>`
- Thread-safe within a single JVM
- **Data is lost on JVM shutdown** — only for unit tests and single-run scripts

### FileSnapshotRepository (single-JVM production)

```java
SnapshotRepository repo = StateMachine.fileRepository(Path.of("/var/fsm-snapshots"));
```

- Stores one `.snapshot` properties file per `executionId` in the given directory
- Each save/load/delete is an atomic file operation
- Survives JVM restarts as long as the directory persists
- **Not suitable for multi-JVM (distributed) deployments** — there is no cross-process locking

### JdbcSnapshotRepository (distributed with SQL databases)

For production deployments across multiple JVMs, use `JdbcSnapshotRepository`. It provides distributed, optimistic-locking-based persistence across PostgreSQL, MySQL, MariaDB, H2, SQLite, and Oracle.

**Dependency:**
```xml
<dependency>
    <groupId>net.hypercell</groupId>
    <artifactId>fsm-jdbc</artifactId>
    <version>1.0.0-RC2</version>
</dependency>
```

**Setup:**
```java
SnapshotRepository repo = new JdbcSnapshotRepository(dataSource);
// Schema is managed by the versioned migration runner (UPDATE mode by default)

StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-workflow")
    .initial("PENDING")
    .snapshotRepository(repo)
    .contextLoader(orderId -> orderRepository.findById(orderId))
    // ... rest of definition
    .build();

StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);
```

**Features:**
- Versioned schema migration runner applies the `fsm_snapshots` table and tracking tables automatically on startup (UPDATE mode, the default); see [JDBC & Spring Boot autoconfiguration](08-jdbc-and-spring-boot.md#schema-migrations) for `VALIDATE` and `OFF` modes
- Optimistic locking via `version` column prevents conflicting updates from concurrent replicas
- Sub-step results stored as JSON for portability across databases
- Supports connection pooling (tested with HikariCP)
- Snapshot keyed by `executionId`

**Spring Boot integration** (optional): See [JDBC & Spring Boot autoconfiguration](08-jdbc-and-spring-boot.md).

### Custom implementation (Redis or other backends)

For Redis or custom backends, implement `SnapshotRepository` directly:

```java
public interface SnapshotRepository {
    void save(String executionId, ExecutionSnapshot snapshot);
    Optional<ExecutionSnapshot> load(String executionId);
    void delete(String executionId);
    List<ExecutionSnapshot> listPendingRetries(); // return FAILED + RETRY_SCHEDULED
}
```

Your `save()` implementation should use optimistic locking to prevent two replicas from committing conflicting snapshots simultaneously. See [Threading & safety — Distributed deployments](04-threading-and-safety.md#single-jvm-vs-distributed-deployments).

---

## Retry policies

A `RetryPolicy` determines whether to automatically retry after a failure, and how long to wait between attempts.

### Exponential backoff

```java
StateMachine.exponentialBackoff(int maxAttempts, Duration baseDelay, Duration maxDelay)
```

Delay doubles on each attempt, capped at `maxDelay`:

```java
StateMachine.exponentialBackoff(5, Duration.ofSeconds(2), Duration.ofMinutes(10))
// Attempt 1: immediate
// Attempt 2: 2 s
// Attempt 3: 4 s
// Attempt 4: 8 s
// Attempt 5: 10 min (capped)
// After 5 attempts: stays FAILED, awaiting manual retry
```

### Fixed delay

```java
StateMachine.fixedDelay(int maxAttempts, Duration delay)
```

Same delay between every attempt:

```java
StateMachine.fixedDelay(3, Duration.ofSeconds(30))
// Attempt 1: immediate
// Attempt 2: 30 s
// Attempt 3: 30 s
// After 3 attempts: stays FAILED
```

### No auto-retry (default)

```java
StateMachine.noAutoRetry()
```

The snapshot is saved on failure (so manual retry via `proceed()` is always possible), but no automatic retry is scheduled. This is the default when no retry policy is specified.

Use this when:
- A human must decide whether to retry (fraud holds, manual approvals)
- The failure requires a code fix before retrying
- You want full control over retry timing

### Custom policy

Implement `RetryPolicy` directly:

```java
public class RetryOnTimeoutOnly implements RetryPolicy {
    @Override
    public boolean shouldRetry(int attemptNumber, Throwable lastError) {
        return attemptNumber <= 3
            && lastError instanceof TimeoutException;
    }

    @Override
    public Duration backoffFor(int attemptNumber) {
        return Duration.ofSeconds(10L * attemptNumber);
    }

    @Override
    public int maxAttempts() { return 3; }
}
```

---

## RetryScheduler

The scheduler is responsible for executing a retry action after the backoff delay expires.

### Built-in: ThreadPoolRetryScheduler

```java
StateMachine.threadPoolScheduler(int threadPoolSize)
```

- Daemon `ScheduledExecutorService` with the given number of threads
- Default thread pool size: 10 (configurable via constructor)
- Named `"fsm-retry-scheduler"` for observability
- Tracks pending futures in a `ConcurrentHashMap` to support cancellation
- `cancel(executionId)` stops a pending retry without interrupting a running one

The builder also provides a built-in 2-thread inline scheduler when no explicit `retryScheduler` is configured. For high throughput, configure an explicit `ThreadPoolRetryScheduler` with a larger pool.

### Custom scheduler

Implement `RetryScheduler` to integrate with a distributed task queue, a database-backed scheduler, or any other mechanism:

```java
public interface RetryScheduler {
    void schedule(String executionId, Duration delay, Runnable retryAction);
    void cancel(String executionId);
    default void shutdown() {}
}
```

---

## The RetryCoordinator lifecycle

`RetryCoordinator` is an internal component that orchestrates the failure→snapshot→schedule→resume flow. You do not call it directly — the manager and instance call it for you. Understanding its lifecycle helps when debugging:

```
1. sub-step fails
      │
      ▼
2. instance.handleFailure()
      ├── status = FAILED
      ├── snapshot saved (status = FAILED)
      └── RetryCoordinator.onFailure() called
              │
              ├── retryPolicy.shouldRetry(attemptNumber, error) == true?
              │       yes: update snapshot status → RETRY_SCHEDULED
              │             schedule retry with backoff delay
              │       no:  snapshot stays FAILED
              │
              ▼
3. [time passes — backoff delay]
      │
      ▼
4. scheduled retry fires (on RetryScheduler thread)
      ├── snapshot status → RUNNING
      ├── contextLoader(executionId) called
      ├── definition.resume(context, snapshot) creates new instance
      └── instance.proceed() runs failed sub-steps (skips completed ones)
              │
              ├── success: status → COMPLETED, snapshot retained
              └── failure: snapshot saved again (attemptNumber++)
                           back to step 2
```

Note: in the diagram above, "retry succeeds" sets `SnapshotStatus.TERMINATED` (not `COMPLETED` — that name was retired in 1.0.0-RC2).

### attempt number increment

`attemptNumber` is incremented in `RetryCoordinator.onFailure()` each time a failure occurs. It starts at 1. After 3 failures, `attemptNumber` is 3. `RetryPolicy.shouldRetry(3, error)` is called; if it returns `false`, no more auto-retries occur.

The attempt number survives process restarts because it is stored in the snapshot. `recoverPendingRetries()` reads it from the snapshot and passes it to `shouldRetry()` to decide whether to reschedule.

---

## Crash recovery and per-sub-step checkpointing

### The problem

Before 1.0.0-RC2, a checkpoint was only saved at the *end* of a successful transition. If the process crashed while sub-steps were executing, the already-completed sub-steps were not durably recorded. When the operation was re-driven, those sub-steps would run again — breaking non-idempotent steps (for example, a one-time backend call that fails on the second invocation).

### The fix

Starting with 1.0.0-RC2, the library saves a `RUNNING` checkpoint after **each successfully executed sub-step**. Because `ExecutionSnapshot.checkpoint()` already captures `currentStateName`, `status=RUNNING`, and the map of completed sub-step results, no schema change is required — only the *frequency* of checkpointing changed.

**Idempotency guarantee:** when the process restarts and the interrupted execution is re-driven, `ResumePolicy` skips every sub-step that has a completed entry in the snapshot. Only the sub-steps that had not yet run will execute. The transition action and entry/exit hooks are **not** re-invoked — they already ran before the crash.

### Detecting an interrupted execution

With the new status model, interrupted detection is a simple status check: **an execution is interrupted when its snapshot status is `RUNNING`**. At-rest executions (sub-steps completed, machine parked between transitions) are now saved as `WAITING`, so the startup sweep only needs to query `WHERE status = 'RUNNING'` — a cheap indexed lookup that never returns legitimately parked executions.

### Strict `trigger()` contract (1.0.0-RC2+)

`trigger()` applies **exactly one transition**. It succeeds only when the execution is ready:

| Snapshot status | `trigger()` behaviour |
|---|---|
| Absent (no snapshot) | First event — creates a new instance and fires the event. |
| `WAITING` | Normal next-event path — reconstitutes and fires the event. |
| `FAILED` | Throws `IllegalTriggerStateException` — call `proceed()` first. |
| `RETRY_SCHEDULED` | Throws `IllegalTriggerStateException` — wait for the auto-retry. |
| `RUNNING` (interrupted) | Throws `IllegalTriggerStateException` — call `resume()` first. |
| `TERMINATED` | Throws `CompletedMachineException`. |

This replaces the old "auto-proceed" and "auto-resume" behaviour where a single `trigger()` call could silently apply two transitions.

### Introspection helpers

Use `eligibilityOf(executionId)` and `currentState(executionId)` as advisory read-only checks before calling `trigger()`:

```java
import io.hypercell.fsm.manager.TriggerEligibility;

TriggerEligibility eligibility = manager.eligibilityOf("order-42");
// READY          → trigger() will succeed
// NEEDS_PROCEED  → call proceed() first (FAILED or RETRY_SCHEDULED)
// NEEDS_RESUME   → call resume() first (RUNNING / interrupted)
// TERMINATED     → trigger() would throw CompletedMachineException

Optional<String> state = manager.currentState("order-42");
// "PENDING", "PROCESSING", etc. — empty if execution doesn't exist yet
```

> **Advisory only.** A concurrent operation may change status between `eligibilityOf()` and `trigger()`. `trigger()` re-validates authoritatively under its lock and throws if ineligible.

### Explicit recover-then-trigger pattern

```java
// Check eligibility first (optional but useful for clean HTTP responses)
TriggerEligibility eligibility = manager.eligibilityOf(orderId);
switch (eligibility) {
    case NEEDS_PROCEED -> manager.proceed(orderId);    // retry failed sub-steps
    case NEEDS_RESUME  -> manager.resume(orderId);     // complete interrupted transition
    case TERMINATED    -> throw new OrderAlreadyCompletedException(orderId);
    case READY         -> {} // fall through — trigger() is safe
}

// Now READY — apply the next event
ManagedTransitionResult<OrderContext> result = manager.trigger(orderId, event);
```

### Two recovery paths

#### Explicit lazy resume (always safe, including distributed deployments)

When the loaded snapshot is `RUNNING` (interrupted), `trigger()` throws `IllegalTriggerStateException`. The caller must call `resume()` first, which completes the in-flight transition (skipping already-checkpointed sub-steps). Once `resume()` returns with `WAITING` status, `trigger()` can be called with the next event.

Explicit resume:

```java
ManagedTransitionResult<OrderContext> result = manager.resume("order-42");
// result.getExecutionStatus() == RUNNING  if resume completed successfully
// result.getExecutionStatus() == FAILED   if a remaining sub-step failed during resume
```

`resume(executionId)` behaviour by status:
- `RUNNING` → interrupted mid-transition; completes the remaining sub-steps and returns the new state.
- `WAITING` → at-rest between transitions; no-op, returns the current state.
- `FAILED` → throws `IllegalStateException`; use `proceed()` instead.
- `TERMINATED` → throws `IllegalStateException`.

#### Startup sweep (single-instance only, requires configured executor)

For single-instance deployments, you can recover all interrupted executions in parallel at startup. **A `recoveryExecutor` must be configured on the builder** — without one, `recoverInterruptedExecutions()` throws `IllegalStateException`:

```java
// Configure a recovery executor on the definition
StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order")
    // ... states, transitions, etc.
    .recoveryExecutor(Executors.newFixedThreadPool(4))  // consumer owns lifecycle
    .build();

// Spring Boot example — single-instance only
@PostConstruct
void recoverOnStartup() {
    manager.recoverPendingRetries();          // reschedule FAILED/RETRY_SCHEDULED retries
    int submitted = manager.recoverInterruptedExecutions();  // async — returns submitted count
    log.info("Submitted {} interrupted executions for recovery", submitted);
}
```

`recoverInterruptedExecutions()`:
- **Requires** a `recoveryExecutor` on the definition; throws `IllegalStateException` if none is configured. Use `resume(executionId)` per execution as the alternative.
- Keyset-paginates `repository.listInterrupted(limit, afterId)` to collect all interrupted execution IDs (stable under concurrent status flips as resumed rows flip to `WAITING`/`TERMINATED`).
- Submits `resume(executionId)` for each to the consumer-supplied executor (best-effort; one failure does not stop others).
- **Returns the count submitted immediately (async)** — executions may still be in-flight when this method returns.

> **WARNING — single-instance only.** Do **not** call `recoverInterruptedExecutions()` in a multi-replica (clustered) deployment. A starting node could resume an execution that is actively being processed by a live peer, causing duplicate sub-step execution. Multi-replica deployments should rely on **lazy resume** instead: `trigger()` and `resume(executionId)` detect and complete an interrupted execution on the next incoming request, which is inherently safe because the client is re-driving a specific execution it owns.

---

### Consumer-driven sweep for FAILED executions: `recoverFailedExecutions(maxAttempts)`

`recoverFailedExecutions(int maxAttempts)` is the distributed-safe alternative to the in-process coordinator path (`recoverPendingRetries`). It does **not** require a `RetryCoordinator` — the consumer owns the distribution strategy (leader election, cron, scheduled task).

```java
// Configure a recovery executor on the definition
StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order")
    // ... states, transitions, etc.
    .recoveryExecutor(Executors.newFixedThreadPool(4))  // consumer owns lifecycle
    .build();

// Driven by the consumer (e.g. from a leader-elected cron job or @Scheduled)
int submitted = manager.recoverFailedExecutions(5); // retry FAILED rows with attempt_number < 5
log.info("Submitted {} failed executions for retry", submitted);
```

**Scope: `FAILED` only.** `RETRY_SCHEDULED` rows belong to the in-process coordinator and are left untouched. Do **not** run `recoverFailedExecutions` and an auto-retry coordinator on the same definition simultaneously — with a coordinator, `FAILED` means "policy exhausted", and the sweep would override that decision.

**Bounded by `maxAttempts`.** Only executions with `attempt_number < maxAttempts` are fetched. Rows at or above the cap are never loaded; exhausted rows stop being retried naturally.

**Attempt-number semantics.** Each call to `recoverFailedExecutions` that retries an execution increments its `attempt_number` by exactly 1 before invoking `proceed()`. Generic `proceed()` and the coordinator path do **not** increment this counter — `attempt_number` therefore reads as "number of sweep retry attempts made by `recoverFailedExecutions`".

**Requires a `recoveryExecutor`.** Throws `IllegalStateException` if none is configured on the definition builder. `maxAttempts` must be `> 0`; passing `0` or a negative value throws `IllegalArgumentException`.

**Async — returns submitted count.** Tasks are submitted to the executor immediately; the method returns the count submitted (executions may still be in-flight on return).

**Single-instance or leader-election for distributed deployments.** Do not call this from every replica — multiple concurrent sweeps would race to retry the same executions.

**Contrast with `recoverPendingRetries`:**

| | `recoverPendingRetries()` | `recoverFailedExecutions(maxAttempts)` |
|---|---|---|
| Scope | `FAILED` + `RETRY_SCHEDULED` | `FAILED` only |
| Requires | `RetryCoordinator` on definition | `recoveryExecutor` on definition |
| Threading | Internal single-thread daemon | Consumer-supplied executor |
| Distribution | In-process only | Consumer-driven (leader election recommended) |
| Attempt counter | Managed by coordinator | Incremented by sweep (`attempt_number < maxAttempts`) |

The V2 schema migration (see [JDBC & Spring Boot](08-jdbc-and-spring-boot.md#schema-migrations)) adds a composite index on `(status, attempt_number)` to accelerate the sweep query `WHERE status = 'FAILED' AND attempt_number < ?`.

### Cost

Per-sub-step checkpointing adds one repository write per executed sub-step (vs. one per transition previously). This is required for the idempotency guarantee. In a future release this may be made opt-out per sub-step if write volume is a concern.

---

## Startup recovery (retries)

When the process restarts, retries that were scheduled or in-flight are not automatically resumed. Call `manager.recoverPendingRetries()` once on startup:

```java
// Spring Boot example
@PostConstruct
void recoverRetries() {
    manager.recoverPendingRetries();
}
```

For each snapshot with status `FAILED` or `RETRY_SCHEDULED`:
- If `RETRY_SCHEDULED` and the scheduled time is in the future: re-schedule with the remaining delay
- If `RETRY_SCHEDULED` and the scheduled time is in the past: fire immediately
- If `FAILED` and `retryPolicy.shouldRetry(attemptNumber, null)` returns `true`: schedule immediately
- If `FAILED` and the policy says no more retries: skip (leave for manual retry)

Each recovery runs on a dedicated single-threaded daemon executor (`"fsm-recovery"`) and is isolated — one recovery failure does not stop others from running.

---

## Manual snapshot inspection

```java
Optional<ExecutionSnapshot> snapshot = manager.snapshotOf("order-42");

snapshot.ifPresent(s -> {
    System.out.println("Status:      " + s.getStatus());
    System.out.println("State:       " + s.getCurrentStateName());
    System.out.println("Failed at:   " + s.getFailedStateName() + "/" + s.getFailedSubStepName());
    System.out.println("Attempts:    " + s.getAttemptNumber());
    System.out.println("Last error:  " + s.getLastErrorMessage());
    System.out.println("Retry at:    " + s.getScheduledRetryAt());
});
```

`snapshotOf()` is a read-only call — it does not modify any state.
