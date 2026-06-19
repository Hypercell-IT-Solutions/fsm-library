# Getting started

This tutorial walks you through defining and running a realistic state machine from scratch. By the end you will have a working order workflow with sub-steps, a logging listener, and failure/recovery.

If you want a concepts primer first, read [Concepts](01-concepts.md).

---

## 1. Add the dependency

```xml
<dependency>
    <groupId>net.hypercell</groupId>
    <artifactId>fsm-core</artifactId>
    <version>1.0.0-RC2</version>
</dependency>
```

The library requires **Java 17** and has a single runtime dependency: SLF4J API 2.x. Wire in your preferred SLF4J backend (Logback, Log4j2, etc.).

---

## 2. Define your context

The context is the domain object the machine passes to every guard, action, and sub-step. It can be anything — a rich domain object, a simple record, or a `Map`.

```java
public class OrderContext {
    private final String orderId;
    private boolean stockReserved;
    private boolean paymentCharged;

    public OrderContext(String orderId) { this.orderId = orderId; }

    public String getOrderId()             { return orderId; }
    public boolean isStockReserved()       { return stockReserved; }
    public void setStockReserved(boolean v){ stockReserved = v; }
    public boolean isPaymentCharged()      { return paymentCharged; }
    public void setPaymentCharged(boolean v){ paymentCharged = v; }
}
```

---

## 3. Define the state machine

All configuration starts from `StateMachine.define(id)`. The id is a stable identifier for this machine *type* (not a specific execution).

```java
import io.hypercell.fsm.StateMachine;
import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.core.StateMachineDefinition;

StateMachineDefinition<OrderContext> orderMachine = StateMachine.<OrderContext>define("order-workflow")
    .initial("PENDING")
    .listener(StateMachine.loggingListener("[ORDER]"))
    .snapshotRepository(StateMachine.inMemoryRepository())

    .state("PENDING")
        .on("APPROVE").to("PROCESSING").end()
        .on("CANCEL").to("CANCELLED").end()
        .and()

    .state("PROCESSING")
        .subStep("reserve-stock", ctx -> {
            // call your inventory service here
            ctx.setStockReserved(true);
            return ActionResult.success();
        })
        .subStep("charge-payment", ctx -> {
            // call your payment service here
            ctx.setPaymentCharged(true);
            return ActionResult.success();
        })
        .subStep("send-confirmation", ctx -> {
            // send email / notification
            return ActionResult.success();
        })
        .on("COMPLETE").to("SHIPPED").end()
        .and()

    .state("SHIPPED").terminal().and()
    .state("CANCELLED").terminal().and()
    .build();
```

### What `build()` validates

- At least one state is defined
- `.initial("STATE_NAME")` was called and refers to a declared state
- Every transition target names an existing state
- No duplicate state names or sub-step names
- Terminal states have no outgoing transitions
- State names and sub-step names do not contain `::` (reserved separator)
- If a retry policy is configured: both `snapshotRepository` and `contextLoader` are also configured

A `StateMachineConfigurationException` is thrown at `build()` time for any violation.

---

## 4. Create and run an instance

```java
import io.hypercell.fsm.core.StateMachineInstance;

OrderContext ctx = new OrderContext("order-42");
StateMachineInstance<OrderContext> instance = orderMachine.newInstance(ctx);

// instance is now RUNNING and positioned at PENDING
System.out.println(instance.currentState().name()); // PENDING
System.out.println(instance.status());              // RUNNING
```

---

## 5. Trigger events

```java
instance.trigger("APPROVE");
// Runs: PENDING.onExit → move → PROCESSING.onEntry
//       → reserve-stock → charge-payment → send-confirmation
// Now positioned at PROCESSING, status RUNNING

instance.trigger("COMPLETE");
// Runs: PROCESSING.onExit → move → SHIPPED.onEntry (terminal)
// Status becomes COMPLETED
System.out.println(instance.status()); // COMPLETED
```

---

## 6. Handle invalid events

```java
try {
    instance.trigger("APPROVE"); // machine is COMPLETED, not RUNNING
} catch (StateMachineException e) {
    // thrown because status != RUNNING
}

StateMachineInstance<OrderContext> fresh = orderMachine.newInstance(new OrderContext("order-99"));
try {
    fresh.trigger("COMPLETE"); // no COMPLETE transition from PENDING
} catch (InvalidEventException e) {
    // thrown because no valid transition matches
}
```

---

## 7. Sub-step failure and recovery

If a sub-step throws or returns `ActionResult.failed(...)`, the machine enters `FAILED` status and saves a snapshot.

```java
StateMachineDefinition<OrderContext> machineWithFailure = StateMachine.<OrderContext>define("order-workflow")
    .initial("PENDING")
    .snapshotRepository(StateMachine.inMemoryRepository())
    .state("PENDING")
        .on("APPROVE").to("PROCESSING").end()
        .and()
    .state("PROCESSING")
        .subStep("reserve-stock", ctx -> ActionResult.success())
        .subStep("charge-payment", ctx -> {
            throw new RuntimeException("Payment gateway timeout");
        })
        .subStep("send-confirmation", ctx -> ActionResult.success())
        .on("COMPLETE").to("SHIPPED").end()
        .and()
    .state("SHIPPED").terminal().and()
    .build();

StateMachineInstance<OrderContext> instance = machineWithFailure.newInstance(new OrderContext("order-10"));

try {
    instance.trigger("APPROVE");
} catch (SubStepExecutionException e) {
    System.out.println(instance.status()); // FAILED
    // reserve-stock already ran — it is recorded in the snapshot
    // charge-payment failed and will be retried from here
}

// Later, after fixing the payment gateway:
instance.proceed();
// Skips reserve-stock (already completed)
// Re-runs charge-payment, then send-confirmation

System.out.println(instance.status()); // RUNNING
instance.trigger("COMPLETE");
System.out.println(instance.status()); // COMPLETED
```

---

## 8. Observe lifecycle events

The logging listener writes a structured line for every event. For custom behaviour (metrics, audit trail, webhooks), implement `MachineEventListener<C>`:

```java
import io.hypercell.fsm.listener.MachineEventListener;
import io.hypercell.fsm.listener.MachineEvent.*;

MachineEventListener<OrderContext> auditListener = new MachineEventListener<>() {
    @Override
    public void onTransitionFired(TransitionFiredEvent<OrderContext> e) {
        audit.record(e.executionId(), e.fromState(), e.toState(), e.event());
    }

    @Override
    public void onMachineCompleted(MachineCompletedEvent<OrderContext> e) {
        metrics.increment("order.completed");
    }

    @Override
    public void onMachineFailed(MachineFailedEvent<OrderContext> e) {
        metrics.increment("order.failed");
        alerts.notify(e.executionId(), e.failedSubStepName());
    }
};

StateMachineDefinition<OrderContext> machine = StateMachine.<OrderContext>define("order-workflow")
    .initial("PENDING")
    .listener(StateMachine.loggingListener("[ORDER]"))
    .listener(auditListener)   // multiple listeners supported
    // ...
    .build();
```

Listeners run **synchronously** on the thread that called `trigger()` or `proceed()`. Keep them fast.

---

## 9. Specify an explicit execution ID

By default, `newInstance()` generates a UUID as the execution ID. If your business entity already has a meaningful ID, supply it:

```java
StateMachineInstance<OrderContext> instance =
    orderMachine.newInstance(new OrderContext("order-42"), "order-42");

System.out.println(instance.executionId()); // order-42
```

This also becomes the snapshot storage key, making it easy to look up the snapshot for a specific order.

---

## Next steps

- **HTTP-driven workflows** — see [Use cases — HTTP manager](03-use-cases.md#b-http-request-driven-workflow)
- **Auto-retry** — see [Use cases — Automatic retry](03-use-cases.md#c-automatic-retry)
- **Spring DI** — see [Use cases — Spring integration](03-use-cases.md#f-spring-di-class-based-states-and-sub-steps)
- **Full API** — see [API reference](06-api-reference.md)
