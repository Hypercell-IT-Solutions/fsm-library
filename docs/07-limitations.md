# Limitations and future improvements

This document captures design constraints and missing features discovered during
the initial documentation pass. Each entry is framed as: the current behaviour,
why it matters, and the suggested improvement.
Use this as a starting point for roadmap planning and issue creation.

---

## Architecture gaps (missing for production readiness)

### 1. No test suite

**Problem:** There is no `src/test` directory anywhere in the project. The library has
zero unit or integration tests. There is no way to verify correctness after a code change
or to accept community contributions safely.

**Suggested improvement:** Add a `fsm-core` test suite covering at minimum:
- The happy path: define → newInstance → trigger → COMPLETED
- Sub-step failure → FAILED status and snapshot saved
- `proceed()` skipping completed sub-steps and running from the failed one
- Guard evaluation (multiple transitions, first-true-wins)
- Terminal state completion
- Manager locking (`ConcurrentExecutionException`)
- Retry coordinator scheduling and backoff
- Startup recovery via `recoverPendingRetries()`

---

### 2. No built-in Redis locking or snapshot storage

**Status (partially resolved):** `JdbcSnapshotRepository` (with optimistic locking) and
`JdbcExecutionLockProvider` (with TTL-based stale-lock takeover) are now built-in for SQL
databases. The `fsm-spring-boot-starter-jdbc` module auto-configures both.

**Remaining gap:** There is no Redis-backed implementation of `ExecutionLockProvider` or
`SnapshotRepository`. Redis users must implement the `ExecutionLockProvider` SPI
(`SET NX` with TTL) and a custom `SnapshotRepository`.

**Suggested improvement:** Ship an optional `fsm-redis` module providing
`RedisExecutionLockProvider` and `RedisSnapshotRepository`.

---

### 3. No `shutdown()` lifecycle on `StateMachineManager`

**Problem:** `StateMachineManager` has no `close()` or `shutdown()` method. The retry
scheduler (`ThreadPoolRetryScheduler`) and recovery executor buried inside
`DefaultStateMachineManager` are daemon threads and will stop at JVM exit, but there is
no way to gracefully drain in-flight retries before shutdown (e.g. in a Spring
`@PreDestroy` hook). Users who need graceful shutdown must keep a direct reference to the
`RetryScheduler` and call `scheduler.shutdown()` separately.

**Suggested improvement:** Add `void shutdown()` to the `StateMachineManager` interface
and implement it to shut down the scheduler and recovery executor with a configurable
drain timeout.

---

### 4. Synchronous-only listeners (no async dispatch)

**Problem:** `EventBus.publish()` dispatches all events synchronously on the thread that
called `trigger()` or `proceed()`. Slow listeners (writing to a slow database, calling
an HTTP endpoint) block the state machine. There is no built-in async or fire-and-forget
dispatch option.

**Suggested improvement:** Add an `AsyncMachineEventListener` variant or an
`EventBus.publishAsync(executor, event)` dispatch mode that fans events out to a thread
pool or a user-supplied `Executor`.

---

### 5. Synchronous-only actions and sub-steps (`AsyncAction<C>` missing)

**Problem:** `Action<C>` executes synchronously on the calling thread. There is no
first-class support for async/reactive sub-steps (e.g. `CompletableFuture`,
Project Reactor `Mono`). The existing code comments in `Action.java` acknowledge this as
a planned future addition.

**Suggested improvement:** Introduce `AsyncAction<C>` returning `CompletableFuture<ActionResult>`.
The `SubStepRunner` would need an async execution path. The retry and snapshot
integration would remain unchanged since the outcome is still an `ActionResult`.

---

### 6. No parallel sub-step execution

**Problem:** Sub-steps always execute sequentially. Independent sub-steps that call
different external systems (e.g. "notify-warehouse" and "send-confirmation-email") must
wait for each other even though they have no data dependency.

**Suggested improvement:** Add a `.parallelSubSteps(...)` builder method that runs a
group of sub-steps concurrently (using a fork-join or `CompletableFuture.allOf`) and
waits for all to complete before continuing. The snapshot model would need to track
completion per-step within a group, which the existing `stateName::subStepName` key
scheme already supports.

---

## Design constraints (by design, but worth understanding)

### 7. Context mutations from completed sub-steps are not persisted automatically

**Constraint:** The library never serializes the context object. On resume, a fresh
context is loaded via `contextLoader(executionId)`. Any in-memory mutations made by
completed sub-steps are lost. Sub-steps that produce data needed by later steps must
persist that data to durable storage themselves, and `contextLoader` must restore it.

**Why it is this way:** Serializing arbitrary user domain objects would require imposing
a serialization contract (Jackson, Java Serializable, etc.) on the context type, which
would couple the library to a specific serialization framework and restrict what context
objects can contain.

**Workaround:** Document and enforce the "smart context loading" pattern — see
[Persistence & retry — Context on resume](05-persistence-and-retry.md#context-on-resume)
and [`FileSnapshotRetryExample`](../fsm-examples/src/main/java/io/hypercell/fsm/examples/FileSnapshotRetryExample.java).

**Future option:** Offer an opt-in `ContextSerializer<C>` interface. When configured,
the library serializes the context into the snapshot. When absent (default), the current
behaviour is preserved.

---

### 8. No timeout support for sub-steps or states

**Constraint:** There is no built-in mechanism to time out a sub-step or enforce a
maximum dwell time in a state. A sub-step that blocks indefinitely (e.g. waiting for a
network response without a configured timeout) holds the calling thread forever.

**Workaround:** Apply timeouts within the sub-step implementation using
`Future.get(timeout, unit)`, `CompletableFuture.orTimeout(...)`, or a library such as
Resilience4j.

**Future option:** Add a `.timeout(Duration)` builder method on `TransitionBuilder`
or `StateBuilder.subStep(...)` that wraps the action in a timed future.

---

### 9. Hook failures are hard stops with no retry granularity

**Constraint:** If `onEntry` or `onExit` throws, the machine enters `FAILED` status but
the failure point recorded in the snapshot is the hook itself (not a named sub-step).
`proceed()` re-runs the hook on the next attempt; there is no way to skip a hook the way
completed sub-steps are skipped.

**Workaround:** Keep hooks side-effect-free and idempotent. Move any work that could fail
and needs retry tracking into a named sub-step instead.

---

### 14. Failure dispositions cover sub-step failures only

**Constraint:** [`FailurePolicy`](05-persistence-and-retry.md#failure-dispositions) is consulted for
sub-step failures and nothing else, because a sub-step failure is the only kind that produces a
persisted `FAILED` snapshot in the first place. Failures elsewhere still escape as exceptions with
no snapshot written and therefore no disposition to classify:

| Failure site | Behaviour |
|---|---|
| Sub-step throws / returns `FAILED` / returns `null` | `FAILED` snapshot, classified by `FailurePolicy` |
| Transition action throws | `StateMachineException` propagates; **no snapshot written** |
| `onEntry` / `onExit` hook throws | `StateMachineException` propagates; no snapshot (see #9) |
| Guard throws | propagates |
| `ContextLoader` throws | propagates |

**Consequence:** a transition action that fails leaves no durable trace — the execution stays at its
previous state with whatever snapshot it already had, and no recovery sweep will ever see it.

**Workaround:** put work that can fail into a named sub-step of the target state rather than into a
transition action or hook. Transition actions are best kept to pure in-memory routing decisions.

**Fix:** write a snapshot on transition-action failure so those failures join the same recovery
model. This is a behavioural change to the transition path and is deliberately out of scope for the
disposition feature.

---

### 15. No compensation routing on failure

**Constraint:** a `FailurePolicy` can decide how a failure is *recovered*, but it cannot redirect the
machine to a different state — there is no `COMPENSATE(event)` disposition that fires a transition
to, say, a `REVERT` state when a sub-step fails. Compensation must be driven by the caller reacting
to the returned `ManagedTransitionResult`, or by an operator.

**Why it is not implemented:** auto-firing a compensating transition raises its own design question —
what happens when the compensating transition itself fails, and how deep the recursion is allowed to
go — which deserves independent treatment rather than being bolted onto the disposition enum.

**Extension point:** `FailureDisposition` is where such a mode would be added.

---

## Minor issues

### 10. `fsm-core` pom declares Java 16 compiler target

**Where:** `fsm-core/pom.xml`.

**Problem:** The `maven-compiler-plugin` configuration sets `<source>16</source>` and
`<target>16</target>` while the codebase uses Java 17 features and the parent pom defines
`maven.compiler.source=17`. This is inconsistent and could cause issues if Java 17
class file features are needed or verified by tooling.

**Fix:** Change `fsm-core/pom.xml` to `<source>17</source><target>17</target>` (or
inherit from the parent by removing the plugin configuration entirely).

---

### 11. `ExecutionSnapshot.setStatus()` is public and mutating on an otherwise immutable class

**Where:** `ExecutionSnapshot.setStatus()`.

**Problem:** `ExecutionSnapshot` is documented as immutable and provides copy-with
methods for all mutations except `setStatus()`, which mutates in place. This
inconsistency can cause subtle bugs if a caller holds a reference and another path
mutates the status under it.

**Fix:** Make `setStatus()` package-private (it is only used within the `resume` package)
or remove it in favour of `withStatus(SnapshotStatus)` in all callers.

---

### 12. No Spring Boot autoconfiguration module

**Constraint:** `StateConfigurer<C>` and `SubStepHandler<C>` are designed for
Spring injection, but there is no `spring-boot-autoconfigure` module. Users must write
`@Configuration` classes manually to wire the definition and manager as beans.

**Suggested improvement:** Ship an optional `fsm-spring-boot-starter` module that
auto-configures a `StateMachineDefinition` bean from `StateConfigurer` and
`SubStepHandler` beans on the classpath, following the Spring Boot convention.

---

### 13. `RetryCoordinator` is public but not intended for consumer use

**Where:** `StateMachineDefinition.retryCoordinator()`.

**Problem:** `RetryCoordinator` is a public class and `retryCoordinator()` is a public
method on the definition interface. Consumers can call it directly, bypassing the
concurrency guards in `DefaultStateMachineManager`. The only legitimate caller is the
manager itself.

**Fix:** Make `RetryCoordinator` package-private (move it to the `execution` package), or
remove `retryCoordinator()` from the `StateMachineDefinition` public interface and have
`DefaultStateMachineDefinition` cast internally where needed.

---

## Summary: priority order for roadmap

| Priority | Item | Type |
|---|---|---|
| High | No test suite (#1) | Architecture gap |
| Medium | No `shutdown()` on manager (#3) | Architecture gap |
| Medium | `ExecutionSnapshot.setStatus()` mutability (#11) | Minor |
| Low | No Redis lock/snapshot module (#2) | Architecture gap |
| Low | Async listeners (#4) | Architecture gap |
| Low | Async actions (#5) | Architecture gap |
| Low | Parallel sub-steps (#6) | Architecture gap |
| Low | Java 16 compiler target in pom (#10) | Minor |
| Future | Opt-in context serialization (#7) | Design |
| Future | Sub-step timeouts (#8) | Design |
| Future | Snapshot on transition-action failure (#14) | Design |
| Future | Compensation routing on failure (#15) | Design |
| Future | Spring Boot autoconfiguration (#12) | Architecture gap |
| Future | `RetryCoordinator` visibility (#13) | Minor |
