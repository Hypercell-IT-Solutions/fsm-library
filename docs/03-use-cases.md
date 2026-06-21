# Use cases

Practical recipes for common integration patterns. Runnable examples live in the
[`fsm-examples`](../fsm-examples/src/main/java/io/hypercell/fsm/examples/)
module (separate from the library JAR, includes Logback for visible log output):

| Example class | Covers |
|---|---|
| [`SynchronousWorkflowExample`](../fsm-examples/src/main/java/io/hypercell/fsm/examples/SynchronousWorkflowExample.java) | Direct `newInstance` + `trigger`, failure, `proceed()`, sub-step skip |
| [`HttpManagerExample`](../fsm-examples/src/main/java/io/hypercell/fsm/examples/HttpManagerExample.java) | `StateMachineManager`, contextLoader, 409/concurrent, completed-machine guard, `initialize()` |
| [`FileSnapshotRetryExample`](../fsm-examples/src/main/java/io/hypercell/fsm/examples/FileSnapshotRetryExample.java) | `FileSnapshotRepository`, failure→snapshot→restart→manual retry, smart contextLoader |
| `JdbcSnapshotExample` | `JdbcSnapshotRepository`, distributed persistence, multi-replica setup |

Run any example with:
```
mvn -pl fsm-examples compile exec:java -Dexec.mainClass=io.hypercell.fsm.examples.<ClassName>
```

---

---

## A. Synchronous in-process workflow

The simplest pattern: define once, run as many instances as you need in the same process. Best for batch jobs, unit tests, command-line tools, and single-threaded pipelines.

```java
StateMachineDefinition<OrderContext> machine = StateMachine.<OrderContext>define("order-workflow")
    .initial("PENDING")
    .state("PENDING")
        .on("APPROVE").to("PROCESSING").end()
        .and()
    .state("PROCESSING")
        .subStep("reserve-stock",  ctx -> reserveStock(ctx))
        .subStep("charge-payment", ctx -> chargePayment(ctx))
        .on("COMPLETE").to("SHIPPED").end()
        .and()
    .state("SHIPPED").terminal().and()
    .build();

// Each order gets its own instance
StateMachineInstance<OrderContext> instance = machine.newInstance(new OrderContext("order-1"));
instance.trigger("APPROVE");
instance.trigger("COMPLETE");

System.out.println(instance.status()); // COMPLETED
```

**One instance = one execution.** Instances are not thread-safe; do not share them across threads. See [Threading & safety](04-threading-and-safety.md).

---

## B. HTTP request-driven workflow

When events arrive across multiple HTTP requests (and potentially to different process replicas), use `StateMachineManager<C>`. It handles the full load → execute → save cycle and prevents concurrent access to the same execution.

```java
// Define once (e.g. as a Spring @Bean)
StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-workflow")
    .initial("PENDING")
    .snapshotRepository(StateMachine.fileRepository(Path.of("/var/fsm-snapshots")))
    .contextLoader(orderId -> orderRepository.findById(orderId)) // how to reload context
    .state("PENDING")
        .on("APPROVE").to("PROCESSING").end()
        .and()
    .state("PROCESSING")
        .subStep("reserve-stock",  ctx -> reserveStock(ctx))
        .subStep("charge-payment", ctx -> chargePayment(ctx))
        .on("COMPLETE").to("SHIPPED").end()
        .and()
    .state("SHIPPED").terminal().and()
    .build();

// Create a manager (also a @Bean in Spring)
StateMachineManager<OrderContext> manager = StateMachine.manager(definition,
    orderId -> orderRepository.findById(orderId));

// --- In your HTTP endpoint ---
// POST /orders/{orderId}/events  body: {"event": "APPROVE"}
ManagedTransitionResult<OrderContext> result = manager.trigger(orderId, "APPROVE");

switch (result.getExecutionStatus()) {
    case RUNNING    -> respond(200, result.getToState());
    case COMPLETED  -> respond(200, "workflow-complete");
    case FAILED     -> respond(500, "failed-at: " + result.getFailedSubStepName());
}
```

### Handling concurrent requests (HTTP 409)

If two requests for the same `executionId` arrive simultaneously, the second throws `ConcurrentExecutionException`. Map it to HTTP 409:

```java
try {
    ManagedTransitionResult<OrderContext> result = manager.trigger(orderId, event);
    return ResponseEntity.ok(result);
} catch (ConcurrentExecutionException e) {
    return ResponseEntity.status(409).body("Another request is already processing this order");
} catch (CompletedMachineException e) {
    return ResponseEntity.status(409).body("Order workflow already completed");
} catch (InvalidEventException e) {
    return ResponseEntity.status(400).body("Invalid event: " + e.getMessage());
}
```

### Startup recovery

Call `recoverPendingRetries()` once on application startup to reschedule any retries that were in-flight when the process was last stopped:

```java
@PostConstruct
void init() {
    manager.recoverPendingRetries();
}
```

---

## C. Automatic retry

Configure a retry policy to automatically re-run failed sub-steps after a backoff delay. Requires `snapshotRepository` and `contextLoader`.

```java
StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-workflow")
    .initial("PENDING")
    .snapshotRepository(StateMachine.fileRepository(Path.of("/var/fsm-snapshots")))
    .retryPolicy(StateMachine.exponentialBackoff(5,
        Duration.ofSeconds(2),   // first retry after 2s
        Duration.ofMinutes(10))) // cap at 10 minutes
    .retryScheduler(StateMachine.threadPoolScheduler(2))
    .contextLoader(orderId -> orderRepository.findById(orderId))
    .state("PENDING")
        .on("APPROVE").to("PROCESSING").end()
        .and()
    .state("PROCESSING")
        .subStep("reserve-stock",  ctx -> reserveStock(ctx))
        .subStep("charge-payment", ctx -> chargePayment(ctx))  // this can fail
        .subStep("send-email",     ctx -> sendEmail(ctx))
        .on("COMPLETE").to("SHIPPED").end()
        .and()
    .state("SHIPPED").terminal().and()
    .build();
```

**Retry delays for `exponentialBackoff(5, 2s, 10min)`:**

| Attempt | Delay |
|---|---|
| 1 | immediate |
| 2 | 2 s |
| 3 | 4 s |
| 4 | 8 s |
| 5 | 10 min (capped) |

After 5 attempts the snapshot stays `FAILED` — you can still call `manager.proceed()` manually.

Fixed delay:
```java
.retryPolicy(StateMachine.fixedDelay(3, Duration.ofSeconds(30)))
```

No auto-retry (snapshot saved, manual retry only):
```java
.retryPolicy(StateMachine.noAutoRetry())
```

---

## D. Manual retry / human-in-the-loop

Use `noAutoRetry()` when a business owner should decide whether to retry (e.g. fraud holds, manual approvals):

```java
StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-workflow")
    .initial("PENDING")
    .snapshotRepository(StateMachine.fileRepository(Path.of("/var/fsm-snapshots")))
    .retryPolicy(StateMachine.noAutoRetry())
    // no contextLoader needed when using manager.proceed(executionId, contextOverride)
    .state("PENDING")
        .on("APPROVE").to("PROCESSING").end()
        .and()
    .state("PROCESSING")
        .subStep("charge-payment", ctx -> chargePayment(ctx))
        .on("COMPLETE").to("SHIPPED").end()
        .and()
    .state("SHIPPED").terminal().and()
    .build();

StateMachineManager<OrderContext> manager = definition.newManager(
    StateMachine.fileRepository(Path.of("/var/fsm-snapshots")),
    orderId -> orderRepository.findById(orderId));

// --- After a support agent approves a retry ---
// POST /orders/{orderId}/retry
ManagedTransitionResult<OrderContext> result = manager.proceed(orderId);

// Or if you have the context in the HTTP request body:
ManagedTransitionResult<OrderContext> result2 = manager.proceed(orderId, requestBodyContext);
```

---

## E. Startup recovery

After a process restart, retries that were scheduled or in-flight may be lost. Call `recoverPendingRetries()` on startup to restore them:

```java
@Component
public class StateMachineConfig {

    @Bean
    public StateMachineManager<OrderContext> orderManager(OrderRepository repo) {
        StateMachineDefinition<OrderContext> def = StateMachine.<OrderContext>define("order-workflow")
            .initial("PENDING")
            .snapshotRepository(StateMachine.fileRepository(Path.of("/var/fsm-snapshots")))
            .retryPolicy(StateMachine.exponentialBackoff(5, Duration.ofSeconds(2), Duration.ofMinutes(10)))
            .retryScheduler(StateMachine.threadPoolScheduler(2))
            .contextLoader(repo::findById)
            // ... states ...
            .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(def, repo::findById);
        manager.recoverPendingRetries(); // re-schedule any pending retries from last run
        return manager;
    }
}
```

`recoverPendingRetries()` uses the remaining delay for `RETRY_SCHEDULED` snapshots (fires immediately if the scheduled time has already passed). It skips `FAILED` snapshots that were intentionally left for manual retry.

---

## F. Spring DI — class-based states and sub-steps

For complex states with many injected dependencies, implement `StateConfigurer<C>` and `SubStepHandler<C>` as Spring beans instead of inline lambdas.

### Sub-step as a class

```java
@Component
public class ChargePaymentStep implements SubStepHandler<OrderContext> {

    private final PaymentService paymentService;

    public ChargePaymentStep(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @Override
    public String name() {
        return "charge-payment"; // stable snapshot key — don't rename this
    }

    @Override
    public ActionResult execute(OrderContext ctx) throws Exception {
        boolean ok = paymentService.charge(ctx.getOrderId(), ctx.getAmount());
        return ok ? ActionResult.success() : ActionResult.failed("Payment declined");
    }
}
```

### State as a class

```java
@Component
public class ProcessingStateConfigurer implements StateConfigurer<OrderContext> {

    private final ReserveStockStep reserveStock;
    private final ChargePaymentStep chargePayment;
    private final SendEmailStep sendEmail;

    public ProcessingStateConfigurer(ReserveStockStep r, ChargePaymentStep c, SendEmailStep s) {
        reserveStock = r; chargePayment = c; sendEmail = s;
    }

    @Override
    public String stateName() { return "PROCESSING"; }

    @Override
    public void configure(StateBuilder<OrderContext> state) {
        state
            .subStep(reserveStock)
            .subStep(chargePayment)
            .subStep(sendEmail)
            .on("COMPLETE").to("SHIPPED").end();
    }
}
```

### Wire them together

```java
@Configuration
public class OrderMachineConfig {

    @Bean
    public StateMachineDefinition<OrderContext> orderMachine(
            ProcessingStateConfigurer processing,
            List<MachineEventListener<OrderContext>> listeners,
            SnapshotRepository repository) {

        StateMachineBuilder<OrderContext> builder = StateMachine.<OrderContext>define("order-workflow")
            .initial("PENDING")
            .snapshotRepository(repository);

        listeners.forEach(builder::listener);

        return builder
            .state("PENDING")
                .on("APPROVE").to("PROCESSING").end()
                .and()
            .state(processing)
            .state("SHIPPED").terminal().and()
            .build();
    }
}
```

---

## G. Guards — multiple transitions for one event

Use guards when the same event leads to different states depending on context:

```java
StateMachineDefinition<OrderContext> machine = StateMachine.<OrderContext>define("order-workflow")
    .initial("PAYMENT_RECEIVED")
    .state("PAYMENT_RECEIVED")
        // Same event "PROCESS", two targets selected by guard
        .on("PROCESS")
            .when(ctx -> ctx.requiresManualReview())
            .to("MANUAL_REVIEW")
            .end()
        .on("PROCESS")
            .when(ctx -> !ctx.requiresManualReview())
            .to("PROCESSING")
            .end()
        .and()
    .state("MANUAL_REVIEW")
        .on("APPROVE").to("PROCESSING").end()
        .on("REJECT").to("CANCELLED").end()
        .and()
    .state("PROCESSING").terminal().and()
    .state("CANCELLED").terminal().and()
    .build();
```

Guards are evaluated in **definition order**. The first guard returning `true` fires. If no guard matches, `InvalidEventException` is thrown.

---

## H. Explicit initialization with `initialize()`

When using `StateMachineManager`, you can explicitly initialize an execution (setup in the initial state) before triggering any events. This is useful for workflows that require explicit initialization or when you want to validate setup before processing.

```java
// Initialize the execution without any event
ManagedTransitionResult<OrderContext> initResult = manager.initialize("order-123");

if (initResult.getExecutionStatus() == ExecutionStatus.RUNNING) {
    System.out.println("Order initialized in: " + initResult.getToState()); // PENDING
}

// Later, trigger events
ManagedTransitionResult<OrderContext> approveResult = manager.trigger("order-123", "APPROVE");
```

`initialize()` is idempotent — calling it multiple times returns the current state without re-executing initial sub-steps. This is useful for:
- Workflows requiring explicit activation before processing
- Defensive initialization with validation
- Two-phase setup: initialize once, then trigger events as they arrive

You can also supply a context override:

```java
OrderContext providedContext = new OrderContext("order-123");
providedContext.setPreloadedData(...);

ManagedTransitionResult<OrderContext> result = manager.initialize("order-123", providedContext);
```

---

## I. State validation — safe initial/terminal checks

The manager and instance both expose `isInitialState()` and `isTerminal()` for safe state type validation without hardcoding state names:

```java
@PostMapping("/orders/{orderId}/trigger/{event}")
public ResponseEntity<?> triggerEvent(
        @PathVariable String orderId,
        @PathVariable String event) {

    // Validate state names defensively
    if (!manager.isInitialState("PENDING")) {
        return ResponseEntity.status(500)
            .body("Configuration error: PENDING is not the initial state");
    }

    Optional<ExecutionSnapshot> snapshot = manager.snapshotOf(orderId);
    
    // Fresh orders start in initial state
    if (snapshot.isEmpty()) {
        ManagedTransitionResult<OrderContext> initResult = manager.initialize(orderId);
        if (initResult.isFailed()) {
            return ResponseEntity.status(500).body("Initialization failed");
        }
    }

    ManagedTransitionResult<OrderContext> result = manager.trigger(orderId, event);
    
    // Detect terminal states
    if (manager.isTerminal(result.getToState())) {
        // Cleanup or audit logging
        logger.info("Order {} completed in state {}", orderId, result.getToState());
    }

    return ResponseEntity.ok(result);
}
```

This pattern protects against configuration changes (e.g., renaming the initial state) without requiring code updates.

---

## J. Distributed deployment with JDBC and Spring Boot

For multi-replica production deployments, use `JdbcSnapshotRepository` with Spring Boot autoconfiguration. This provides distributed, database-backed persistence with a versioned schema migration runner that keeps the database schema up to date automatically.

See [JDBC & Spring Boot autoconfiguration](08-jdbc-and-spring-boot.md) for a complete example. Quick setup:

```xml
<dependency>
    <groupId>net.hypercell</groupId>
    <artifactId>fsm-spring-boot-starter-jdbc</artifactId>
    <version>1.0.0-RC2</version>
</dependency>
```

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/myapp
spring.datasource.username=fsm_user
spring.datasource.password=secure_password
```

```java
@Bean
public StateMachineManager<OrderContext> orderManager(
        StateMachineDefinition<OrderContext> definition,
        JdbcSnapshotRepository snapshotRepository) {
    return StateMachine.manager(definition, snapshotRepository);
}
```

All `trigger()` and `proceed()` calls use optimistic locking to ensure only one replica commits a snapshot at a time. The library automatically retries transient conflicts.

---

## K. Exception handling with `getRootCause()`

When a sub-step fails, `ManagedTransitionResult` exposes the underlying exception via `getRootCause()`. This lets you make informed recovery decisions without catching exceptions:

```java
ManagedTransitionResult<OrderContext> result = manager.trigger(orderId, "PROCESS");

if (result.isFailed()) {
    String failedStep = result.getFailedSubStepName();
    Throwable rootCause = result.getRootCause();

    if (rootCause instanceof InventoryException) {
        // Out of stock — retry or notify customer
        logger.warn("Inventory failure at step {}: {}", failedStep, rootCause.getMessage());
        // Trigger manual process or escalation
    } else if (rootCause instanceof PaymentException) {
        // Payment gateway issue — auto-retry or manual review
        logger.warn("Payment failure at step {}: {}", failedStep, rootCause.getMessage());
        // Request manual payment retry
    } else if (rootCause instanceof TimeoutException) {
        // Transient failure — likely to succeed on retry
        logger.warn("Timeout at step {}", failedStep);
        // Will auto-retry based on retry policy
    } else {
        // Unexpected error
        logger.error("Unexpected failure at step {}", failedStep, rootCause);
        // Manual escalation required
    }

    return ResponseEntity.status(500).body(Map.of(
        "failedStep", failedStep,
        "errorType", rootCause.getClass().getSimpleName(),
        "errorMessage", rootCause.getMessage()
    ));
}
```

This pattern enables:
- **Differentiated error responses** — 503 for transient (timeout), 409 for conflict (inventory), 400 for validation
- **Targeted recovery logic** — retry policy decisions per error type
- **Better observability** — expose error details in API responses and logs

Guards must be **pure** (read-only, side-effect-free). See [Concepts — Guards](01-concepts.md#guards).

---

## H. Transition actions

Use a transition action for work that belongs to the movement between states (auditing, logging a state change to a DB):

```java
.state("PENDING")
    .on("APPROVE")
        .action(ctx -> {
            auditLog.record(ctx.getOrderId(), "PENDING → PROCESSING");
            return ActionResult.success();
        })
        .to("PROCESSING")
        .end()
    .and()
```

The action runs **after** `sourceState.onExit()` and **before** `targetState.onEntry()`. If the transition action fails (throws or returns `ActionResult.failed`), the machine enters `FAILED` with the machine still logically in the source state.

---

## I. Entry and exit hooks

Use hooks for lightweight state setup and teardown (timestamps, context initialization):

```java
.state("PROCESSING")
    .onEntry(ctx -> ctx.setProcessingStartedAt(Instant.now()))
    .onExit(ctx  -> ctx.setProcessingEndedAt(Instant.now()))
    .subStep("charge-payment", ctx -> chargePayment(ctx))
    .on("COMPLETE").to("SHIPPED").end()
    .and()
```

**Hooks are not retry-tracked.** They always run when entering/exiting a state, even on resume. Keep them idempotent or side-effect-free. For anything non-idempotent (API calls, DB writes), use a sub-step instead.

---

## J. Interdependent sub-steps and smart context loading

When a sub-step produces data that a later sub-step needs, the producer must persist that data and the `contextLoader` must restore it. On resume, completed sub-steps are skipped entirely — their in-memory mutations to the context are lost when `contextLoader` builds a fresh context.

### The problem illustrated

```
PROCESSING state:
  step 1 "reserve-stock"  → runs, sets ctx.reservationId = "RSV-42"  [success, SKIPPED on resume]
  step 2 "charge-payment" → fails (payment gateway timeout)
  step 3 "send-email"     → not reached

On resume:
  contextLoader("order-42") is called → returns a fresh OrderContext
  ctx.reservationId is null  ← the in-memory mutation from step 1 is gone
  step 1 is skipped
  step 2 runs → reads ctx.reservationId → null → wrong behaviour
```

### The solution: persist in the sub-step, restore in contextLoader

```java
// Step 1: produce the reservationId and write it to durable storage
.subStep("reserve-stock", ctx -> {
    String reservationId = inventoryService.reserve(ctx.getOrderId(), ctx.getItems());

    ctx.setReservationId(reservationId);                          // in-memory (lost on fresh load)
    orderRepository.saveReservationId(ctx.getOrderId(), reservationId); // durable

    return ActionResult.success();
})

// Step 2: reads reservationId — must work on first run AND on resume
.subStep("charge-payment", ctx -> {
    // ctx.getReservationId() is guaranteed non-null because contextLoader restores it
    paymentService.charge(ctx.getOrderId(), ctx.getAmount(), ctx.getReservationId());
    return ActionResult.success();
})
```

```java
// contextLoader restores all intermediate results, not just the base entity
Function<String, OrderContext> contextLoader = orderId -> {
    Order order = orderRepository.findById(orderId);
    OrderContext ctx = new OrderContext(order);

    // Restore reservationId if step 1 already completed in a previous run
    String reservationId = orderRepository.findReservationId(orderId);
    if (reservationId != null) {
        ctx.setReservationId(reservationId);
    }

    return ctx;
};
```

### The rule

> Each sub-step is responsible for its own durability. If a sub-step produces output needed by a later step, it must save that output to durable storage before returning `ActionResult.success()`. The `contextLoader` then reconstructs the full context as it would have been at the point of failure.

This is the same contract that any idempotent distributed system imposes: a step must be safe to skip only if its observable side-effects (in the context and in storage) are fully recoverable from durable state.

See [Persistence & retry — Context on resume](05-persistence-and-retry.md#context-on-resume) for the detailed explanation.
