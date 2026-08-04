# Concepts

This page builds the mental model you need to use the library effectively. Read it once before diving into code.

---

## The two objects you always deal with

### StateMachineDefinition — the blueprint

`StateMachineDefinition<C>` is an **immutable, validated description** of your workflow: what states exist, what events are allowed, which guards apply, and what work to do. You build it once (e.g. as a Spring `@Bean`) and reuse it to create as many instances as you need.

Think of it like a `Class` — it describes behaviour; it doesn't run anything.

### StateMachineInstance — one running execution

`StateMachineInstance<C>` is **one live execution** of a definition. It is mutable (it tracks the current state, status, and completed sub-steps). Each execution of an order, a payment, a user onboarding flow — each gets its own instance.

Think of it like an `Object` — it holds state and responds to messages.

```
StateMachineDefinition  →  newInstance(context)  →  StateMachineInstance
       (shared)                                          (per-workflow)
```

---

## Context

The type parameter `C` (context) is **your** domain object — `OrderContext`, `PaymentRequest`, a `Map<String,Object>`, whatever makes sense. The state machine passes it to every guard, action, and sub-step. Actions and sub-steps may mutate it; guards must read it only.

The library never serializes the context itself. Only `ActionResult` output maps and snapshot metadata are persisted.

---

## States

A **state** is a stable position the machine can be in. States have:

- A unique **name** (string)
- An optional ordered list of **sub-steps** (work executed on entry)
- Optional **onEntry / onExit hooks** (lightweight setup / teardown)
- Outgoing **transitions** (unless terminal)

**Terminal states** end the workflow. When the machine enters a terminal state and all its sub-steps complete, the instance status becomes `COMPLETED`. Terminal states can have sub-steps and hooks; they just have no outgoing transitions.

---

## Transitions

A **transition** moves the machine from one state to another in response to an **event**. Each transition has:

- A source state and a target state
- An **event name** — the string you pass to `trigger(event)`
- An optional **guard** — a condition that must be true for the transition to fire
- An optional **action** — work executed during the move (between `onExit` and `onEntry`)

Multiple transitions may share the same event name (distinguished by guards). They are evaluated in definition order; the first whose guard returns `true` fires.

---

## Guards

A `Guard<C>` is a **pure read-only condition** evaluated before a transition fires:

```java
Guard<OrderContext> isPremium = ctx -> ctx.getCustomer().isPremium();
```

Key rules:
- **Never modify the context** inside a guard — guards may be called multiple times per event.
- Guards run synchronously on the calling thread.
- If no transition matches (all guards return `false` or there are no transitions for the event), `InvalidEventException` is thrown.

---

## Actions

An `Action<C>` is a **unit of work** that returns an `ActionResult`. It appears in two places:

1. **Transition actions** — execute while the machine is moving between states
2. **Sub-step actions** — execute after the machine has entered the new state

```java
Action<OrderContext> chargePayment = ctx -> {
    boolean ok = paymentService.charge(ctx.getOrderId(), ctx.getAmount());
    return ok ? ActionResult.success() : ActionResult.failed("Payment declined");
};
```

Actions may read and write the context. Exceptions thrown from an action are caught by the library and converted to `ActionResult.failed(exception)`.

---

## Sub-steps

Sub-steps are the **granular named work units** executed when a state is entered. They are the key concept behind the library's resumability:

- Each sub-step has a **stable name** (e.g. `"reserve-stock"`, `"charge-payment"`)
- When a sub-step fails, the machine saves a snapshot listing which sub-steps **completed**
- On resume, completed sub-steps are **skipped** — only the failed step (and any after it) re-run

This means even complex multi-step operations are safe to retry without double-charging customers or double-sending emails.

> **Naming is a contract.** Treat sub-step names like database column names. Renaming a sub-step breaks any snapshot saved under the old name.

Sub-steps are also the unit at which failures are **classified** — see [failure dispositions](#failure-dispositions) below.

### Context mutations and resume

Skipping a sub-step means its **code does not run again** — including any in-memory mutations it made to the context. On resume, the library calls `contextLoader(executionId)` to load a **fresh context**. A fresh context does not carry the in-memory changes made by skipped steps.

This creates a dependency: if sub-step 2 relies on a value that sub-step 1 wrote to the context, and sub-step 1 is skipped on resume, sub-step 2 receives a context that never had that value set.

**The required pattern:** any sub-step that produces data needed by a later step must persist that data to durable storage (database, cache, etc.) as part of its own work. The `contextLoader` must then read and restore that data when rebuilding the context.

```
Sub-step 1 runs:
  → calls external API, gets reservationId
  → writes ctx.setReservationId(reservationId)  ← in-memory only
  → ALSO: saves reservationId to DB              ← durable

Sub-step 2 fails.

On resume, contextLoader is called:
  → loads order from DB
  → ALSO: loads reservationId from DB and sets ctx.setReservationId(...)  ← restored

Sub-step 1 is skipped. Sub-step 2 runs with reservationId available.
```

Think of `contextLoader` as reconstructing the full context as it would have been at the point of failure — not just the base entity, but all intermediate results produced by completed sub-steps.

See [Persistence & retry — Context on resume](05-persistence-and-retry.md#context-on-resume) for a complete example.

---

## Hooks vs sub-steps

| | `onEntry` / `onExit` hooks | Sub-steps |
|---|---|---|
| Purpose | Lightweight setup / teardown | Real business work |
| Retry tracking | **Not tracked** — always re-run on resume | **Tracked** — skipped if already completed |
| Failure handling | Hard stop; not individually retried | Machine enters `FAILED`; resumable |
| Output | None | `ActionResult` with optional output map |

Use hooks for things like setting a timestamp or initializing a context field. Use sub-steps for anything you wouldn't want to repeat (API calls, DB writes, emails).

---

## ActionResult

Every action and sub-step returns an `ActionResult` describing what happened:

| Factory | Meaning |
|---|---|
| `ActionResult.success()` | Work completed, no output |
| `ActionResult.success(Map<String,Object>)` | Work completed, with output data |
| `ActionResult.failed(Throwable)` | Expected failure; machine will enter `FAILED` |
| `ActionResult.failed(String message)` | Same but from a string message |
| `ActionResult.skipped()` | Set internally by the library on resume; don't construct manually |

Output values must be serialization-friendly (String, Number, Boolean, etc.) because they are stored in the snapshot.

---

## ExecutionStatus

The instance's status at any point in time:

```
newInstance()
     │
     ▼
  RUNNING ──── trigger() succeeds, terminal state ──► COMPLETED
     │
     │ sub-step/hook failure
     ▼
  FAILED
     │
     │ proceed() called (manual or auto-retry)
     └────────────────────────────────────────────────► RUNNING
```

- `trigger()` and `proceed()` require status `RUNNING`
- `proceed()` transitions `FAILED → RUNNING`; subsequent failure puts it back to `FAILED`

---

## Execution order for a single trigger

Before any of this, the moment the instance is built the library publishes an `InstanceCreatedEvent` carrying an `InstanceOrigin` (`NEW`, `RECONSTITUTED`, `RESUMED_FAILED`, `RESUMED_INTERRUPTED`). It is the first event of an execution and precedes all work — including the initial state's sub-steps, which run inside the constructor of a fresh instance. Every event from here on carries the machine's context via `getContext()`.

When you call `instance.trigger("SOME_EVENT")`:

```
1. validate: status must be RUNNING
2. find matching transition (event name + guard evaluation)
3. sourceState.onExit(context)
4. transition.action(context)          ← optional
5. move to targetState
6. publish TransitionFiredEvent
7. targetState.onEntry(context)
8. publish StateEnteredEvent
9. for each sub-step in targetState:
     if already completed in snapshot → skip (publish SubStepSkippedEvent)
     else run → success: publish SubStepCompletedEvent
               → failure: status=FAILED, save snapshot, throw SubStepExecutionException
10. if targetState.isTerminal():
      status = COMPLETED
      save snapshot with status COMPLETED
      publish MachineCompletedEvent
    else:
      save checkpoint (status RUNNING)
```

---

## Snapshots

A **snapshot** is a serializable point-in-time capture saved when the machine fails. It records:

- Which state the machine is in
- Which sub-step failed
- Which sub-steps **completed** (keyed as `"stateName::subStepName"`)
- The attempt number and error details
- The **failure disposition** — how this failure should be handled
- A scheduled retry time (if an auto-retry is pending)

Snapshots are stored in a `SnapshotRepository`. The library uses the `executionId` as the storage key.

See [Persistence & retry](05-persistence-and-retry.md) for the full picture.

---

## Failure dispositions

A snapshot's `SnapshotStatus` says *where* an execution is at rest. Its **failure disposition** says
*how* the failure should be handled — a separate question, because two executions can both be
`FAILED` and still need completely different recovery.

By default every sub-step failure is `RETRY`: save a snapshot, and let the recovery sweep retry it
from the failure point. A `FailurePolicy` can classify a failure differently at the moment it
happens, based on which state, which sub-step, and which exception:

| Disposition | What it means |
|---|---|
| `RETRY` | Transient — retry from the failure point. The default. |
| `MANUAL` | Stays `FAILED` and resumable, but no automatic recovery touches it. |
| `REWIND` | Nothing was committed — abandon the transition and park back at the source state, so the caller can re-fire the same event. |
| `ABORT` | Permanent. Retrying cannot help; `proceed()` refuses it. |

Policies attach at machine, state, or sub-step level, most specific wins:

```java
.state("PROCESSING")
    .failurePolicy(FailurePolicy.onFirstSubStep(FailureDisposition.REWIND))
    .subStep("reserve-stock",  ctx -> reserve(ctx))   // fails → transition rewound
    .subStep("charge-payment", ctx -> charge(ctx))    // fails → normal retry
```

See [Failure dispositions](05-persistence-and-retry.md#failure-dispositions) for the full rules,
including the safety constraints on `REWIND`.

---

## The manager (HTTP-driven workflows)

For workflows driven by HTTP requests (where each request may come to a different process replica), the `StateMachineManager<C>` handles everything:

```
HTTP request
     │
     ▼
manager.trigger(executionId, event)
     │
     ├── acquire per-executionId lock (→ 409 if already locked)
     ├── load snapshot
     ├── load context (via contextLoader)
     ├── branch:
     │     no snapshot     → newInstance  → trigger
     │     RUNNING status  → reconstitute → trigger
     │     FAILED status   → resume → proceed → trigger
     │     COMPLETED       → throw CompletedMachineException
     ├── save checkpoint / snapshot
     └── release lock
          │
          ▼
   ManagedTransitionResult
```

See [Use cases](03-use-cases.md) for a complete HTTP example.
