# JDBC and Spring Boot autoconfiguration

For Spring Boot applications, the `fsm-spring-boot-starter-jdbc` module provides automatic configuration of `JdbcSnapshotRepository`, reducing boilerplate and integrating seamlessly with Spring's `DataSource`.

---

## Quick start

**1. Add the Spring Boot starter dependency:**

```xml
<dependency>
    <groupId>net.hypercell</groupId>
    <artifactId>fsm-spring-boot-starter-jdbc</artifactId>
    <version>1.0.0-RC3</version>
</dependency>
```

(This automatically pulls in `fsm-core` and `fsm-jdbc`.)

**2. Configure your database in `application.properties` or `application.yml`:**

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/myapp
spring.datasource.username=user
spring.datasource.password=pass
spring.datasource.driver-class-name=org.postgresql.Driver
```

**3. Inject the auto-configured beans and use them:**

```java
@Component
public class OrderWorkflowService {

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private JdbcSnapshotRepository snapshotRepository;

    @Autowired
    private ExecutionLockProvider executionLockProvider;  // auto-configured by starter

    @Bean
    public StateMachineDefinition<OrderContext> orderWorkflow() {
        return StateMachine.<OrderContext>define("order-workflow")
            .initial("PENDING")
            .snapshotRepository(snapshotRepository)         // auto-configured bean
            .executionLockProvider(executionLockProvider)   // distributed lock
            .contextLoader(orderId -> orderRepository.findById(orderId))
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
    }

    @Bean
    public StateMachineManager<OrderContext> orderManager(
            StateMachineDefinition<OrderContext> definition) {
        return StateMachine.manager(definition, snapshotRepository);
    }

    // ... rest of service
}
```

**4. Use the manager in your endpoints:**

```java
@RestController
@RequestMapping("/orders/{orderId}")
public class OrderController {

    @Autowired
    private StateMachineManager<OrderContext> orderManager;

    @PostMapping("/events")
    public ResponseEntity<?> triggerEvent(
            @PathVariable String orderId,
            @RequestBody EventRequest event) {
        try {
            // 1.0.0-RC2+: trigger() is strict — requires WAITING (or no snapshot).
            // Use eligibilityOf() to recover before triggering.
            TriggerEligibility eligibility = orderManager.eligibilityOf(orderId);
            if (eligibility == TriggerEligibility.NEEDS_PROCEED) {
                orderManager.proceed(orderId);
            } else if (eligibility == TriggerEligibility.NEEDS_RESUME) {
                orderManager.resume(orderId);
            } else if (eligibility == TriggerEligibility.TERMINATED) {
                return ResponseEntity.status(409).body("Workflow already completed");
            }

            ManagedTransitionResult<OrderContext> result = 
                orderManager.trigger(orderId, event.name());
            
            return ResponseEntity.ok(Map.of(
                "status", result.getExecutionStatus(),
                "currentState", result.getToState()
            ));
        } catch (ConcurrentExecutionException e) {
            return ResponseEntity.status(409).body("Another request is processing this order");
        } catch (CompletedMachineException e) {
            return ResponseEntity.status(409).body("Workflow already completed");
        } catch (IllegalTriggerStateException e) {
            return ResponseEntity.status(409).body("Execution not ready: " + e.getMessage());
        }
    }
}
```

---

## What the starter auto-configures

The `fsm-spring-boot-starter-jdbc` module provides:

1. **`JdbcSnapshotRepository` bean** — automatically instantiated with Spring's `DataSource`
2. **`JdbcExecutionLockProvider` bean** — distributed execution lock backed by `fsm_execution_locks`; prevents simultaneous processing of the same snapshot across replicas. Enabled by default; disable with `fsm.jdbc.lock.enabled=false` or supply your own `ExecutionLockProvider` bean.
3. **Versioned schema migration** — a Liquibase-style migration runner creates and upgrades the schema automatically on startup; two tracking tables (`fsm_schema_history`, `fsm_schema_lock`) record applied versions and guard against concurrent multi-replica migrations
4. **Connection pooling** — uses Spring's configured `DataSource` (typically HikariCP)
5. **No additional properties to set** — works with standard `spring.datasource.*` config

---

## Multi-database support

`JdbcSnapshotRepository` adapts its SQL dialect to the configured database. Set the dialect via `fsm.jdbc.dialect` (default: `postgresql`); the value selects the corresponding migration SQL files bundled in the `fsm-jdbc` jar (`io/hypercell/fsm/db/migrations/<dialect>/`). Tested and supported on:

- **PostgreSQL** 12+ (`postgresql`)
- **MySQL** 8.0+ (`mysql`)
- **MariaDB** 10.6+ (`mysql`)
- **H2** 2.0+ (`h2`, in-memory or file-based)
- **SQLite** 3.40+ (`sqlite`)
- **Oracle** 21c+ (`oracle`)

---

## Example: Order workflow with Spring Boot and PostgreSQL

Here's a complete, runnable Spring Boot application using FSM with JDBC:

```java
@SpringBootApplication
public class OrderApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }
}

@Data
class OrderContext {
    private String orderId;
    private List<String> items;
    private BigDecimal amount;
    private String reservationId;

    public OrderContext(String orderId) {
        this.orderId = orderId;
    }
}

@Repository
public interface OrderRepository extends JpaRepository<Order, String> {}

@Service
public class OrderService {

    @Autowired private OrderRepository orderRepository;
    @Autowired private StateMachineManager<OrderContext> manager;

    public ManagedTransitionResult<OrderContext> approveOrder(String orderId) {
        return manager.trigger(orderId, "APPROVE");
    }

    public ManagedTransitionResult<OrderContext> completeOrder(String orderId) {
        return manager.trigger(orderId, "COMPLETE");
    }
}

@Configuration
public class OrderWorkflowConfig {

    @Bean
    public StateMachineDefinition<OrderContext> orderWorkflow(
            JdbcSnapshotRepository snapshotRepository,
            OrderRepository orderRepository) {

        return StateMachine.<OrderContext>define("order-workflow")
            .initial("PENDING")
            .snapshotRepository(snapshotRepository)
            .contextLoader(orderId -> {
                Order order = orderRepository.findById(orderId)
                    .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));
                return new OrderContext(order.getId());
            })
            .retryPolicy(StateMachine.exponentialBackoff(3, Duration.ofSeconds(2), Duration.ofMinutes(5)))
            .retryScheduler(StateMachine.threadPoolScheduler(2))
            .state("PENDING")
                .on("APPROVE").to("PROCESSING").end()
                .and()
            .state("PROCESSING")
                .subStep("reserve-stock", ctx -> {
                    // Inventory service call — may fail
                    ctx.setReservationId(inventoryService.reserve(ctx.getOrderId(), ctx.getItems()));
                    inventoryService.persistReservation(ctx.getOrderId(), ctx.getReservationId());
                    return ActionResult.success();
                })
                .subStep("charge-payment", ctx -> {
                    paymentService.charge(ctx.getOrderId(), ctx.getAmount(), ctx.getReservationId());
                    return ActionResult.success();
                })
                .on("COMPLETE").to("SHIPPED").end()
                .and()
            .state("SHIPPED").terminal().and()
            .build();
    }

    @Bean
    public StateMachineManager<OrderContext> orderManager(
            StateMachineDefinition<OrderContext> definition,
            JdbcSnapshotRepository snapshotRepository) {
        return StateMachine.manager(definition, snapshotRepository);
    }
}

@RestController
@RequestMapping("/orders/{orderId}")
public class OrderController {

    @Autowired private StateMachineManager<OrderContext> manager;

    @PostMapping("/initialize")
    public ResponseEntity<?> initialize(@PathVariable String orderId) {
        try {
            ManagedTransitionResult<OrderContext> result = manager.initialize(orderId);
            return ResponseEntity.ok(Map.of(
                "status", result.getExecutionStatus(),
                "state", result.getToState()
            ));
        } catch (Exception e) {
            return ResponseEntity.status(500).body(e.getMessage());
        }
    }

    @PostMapping("/trigger/{event}")
    public ResponseEntity<?> trigger(@PathVariable String orderId, @PathVariable String event) {
        try {
            // 1.0.0-RC2+: trigger() requires the execution to be WAITING (or new).
            // Use eligibilityOf() to recover before triggering.
            TriggerEligibility eligibility = manager.eligibilityOf(orderId);
            switch (eligibility) {
                case NEEDS_PROCEED -> manager.proceed(orderId);   // retry failed sub-steps first
                case NEEDS_RESUME  -> manager.resume(orderId);    // complete interrupted transition first
                case TERMINATED -> {
                    return ResponseEntity.status(409).body("Order already completed");
                }
                case READY -> {} // fall through — safe to trigger
            }

            ManagedTransitionResult<OrderContext> result = manager.trigger(orderId, event);
            return ResponseEntity.ok(Map.of(
                "status", result.getExecutionStatus(),
                "currentState", result.getToState()
            ));
        } catch (ConcurrentExecutionException e) {
            return ResponseEntity.status(409).body("Another request is processing this order");
        } catch (CompletedMachineException e) {
            return ResponseEntity.status(409).body("Order already completed");
        } catch (IllegalTriggerStateException e) {
            // Concurrent modification between eligibilityOf() and trigger()
            return ResponseEntity.status(409).body("Execution not ready: " + e.getMessage());
        }
    }

    @GetMapping("/status")
    public ResponseEntity<?> getStatus(@PathVariable String orderId) {
        return manager.snapshotOf(orderId)
            .map(snapshot -> Map.of(
                "orderId", snapshot.getExecutionId(),
                "currentState", snapshot.getCurrentStateName(),
                "status", snapshot.getStatus(),
                "attemptNumber", snapshot.getAttemptNumber()
            ))
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }
}
```

**Configuration in `application.properties`:**

```properties
spring.application.name=order-service
spring.datasource.url=jdbc:postgresql://localhost:5432/orders_db
spring.datasource.username=fsm_user
spring.datasource.password=secure_password
spring.datasource.hikari.maximum-pool-size=10
spring.jpa.hibernate.ddl-auto=validate
server.port=8080
```

---

## Startup: schema migration and recovery

On application startup, the FSM library:

1. **Runs the schema migration runner** — bootstraps the two tracking tables (`fsm_schema_history`, `fsm_schema_lock`), acquires a distributed DB lock, applies any not-yet-applied versioned migrations in order (V1: creates the `fsm_snapshots` table and its status index; V2: adds the composite `(status, attempt_number)` index for the failed-execution sweep; V3: creates the `fsm_execution_locks` table for distributed execution locking), verifies checksums of previously-applied migrations, and releases the lock. The migration is safe under concurrent multi-replica startup. See [Schema migrations](#schema-migrations) below for configuration options.
2. **Calls `recoverPendingRetries()`** to resume any failed executions that are scheduled for retry.
3. *(Optional, single-instance only)* **Calls `recoverInterruptedExecutions()`** to complete executions whose process crashed mid-transition.

Add this to your configuration:

```java
@Component
public class OrderWorkflowStartupListener implements ApplicationListener<ContextRefreshedEvent> {

    @Autowired
    private StateMachineManager<OrderContext> manager;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        // Reschedule FAILED/RETRY_SCHEDULED retries (in-process coordinator path)
        manager.recoverPendingRetries();

        // Resume executions interrupted by a crash mid-sub-step.
        // WARNING: call only in single-instance deployments (see below).
        // Requires recoveryExecutor configured on the definition.
        int submitted = manager.recoverInterruptedExecutions();  // async — returns submitted count
        if (submitted > 0) {
            log.info("Submitted {} interrupted executions for recovery", submitted);
        }

        // For consumer-driven FAILED retry (no coordinator required):
        // Call recoverFailedExecutions(maxAttempts) from a leader-elected scheduler or
        // a single designated node.  Do NOT call from every replica.
        // int failedSubmitted = manager.recoverFailedExecutions(5);
    }
}
```

### Snapshot status model

The persisted `SnapshotStatus` encodes precisely where an execution is at rest:

| Status | Meaning |
|---|---|
| `RUNNING` | Execution is **actively processing sub-steps**. A crash leaves the snapshot here. `listInterrupted()` returns only these rows (cheap indexed query). |
| `WAITING` | Sub-steps completed; machine is **parked at a non-terminal state** awaiting the next event. Normal at-rest status. **Not** returned by `listInterrupted()`. |
| `TERMINATED` | A terminal state was reached successfully. Retained so re-triggering throws `CompletedMachineException`. |
| `FAILED` | A sub-step failed; awaiting manual or scheduled retry. |
| `RETRY_SCHEDULED` | An automatic retry has been scheduled. |

### Interrupted executions: lazy vs. startup sweep

The library persists a **`RUNNING` checkpoint** after each successfully executed sub-step (1.0.0-RC2+). When all sub-steps complete and the machine parks at a non-terminal state, it saves a **`WAITING` checkpoint**. If the process crashes mid-sub-step, the snapshot stays `RUNNING`. Two strategies detect and resume interrupted executions:

| Strategy | Safety | When to use |
|---|---|---|
| **Lazy** (built-in to `trigger()` and `resume()`) | Safe in all deployments, including distributed | All deployments — the client re-driving a specific execution is inherently safe |
| **Startup sweep** (`recoverInterruptedExecutions()`) | **Single-instance only** | Single-JVM deployments where you want to eagerly recover on startup |

> **Distributed deployments must not call `recoverInterruptedExecutions()` on startup.** A starting node could resume an execution that is actively being processed by a live peer, causing duplicate sub-step execution. In a multi-replica environment, rely on lazy resume: the next `trigger()` or `resume()` call for a specific execution will automatically detect and complete any in-flight transition, which is safe because the client owns that execution.

**`recoverInterruptedExecutions()` requires a `recoveryExecutor`** configured on the definition builder. Without one, it throws `IllegalStateException`. The method is **async** — it collects all interrupted IDs via keyset-paginated `listInterrupted()` calls, submits each to the executor, and returns the **count submitted** immediately (executions may still be in-flight on return).

```java
// Configure recovery executor on the definition bean
@Bean
public StateMachineDefinition<OrderContext> orderWorkflow(
        JdbcSnapshotRepository snapshotRepository,
        OrderRepository orderRepository) {
    return StateMachine.<OrderContext>define("order-workflow")
        // ... states, transitions
        .snapshotRepository(snapshotRepository)
        .contextLoader(orderId -> orderRepository.findById(orderId))
        .recoveryExecutor(Executors.newFixedThreadPool(4))  // consumer owns lifecycle
        .build();
}
```

`listInterrupted()` in `JdbcSnapshotRepository` executes `SELECT … WHERE status = 'RUNNING' AND execution_id > ? ORDER BY execution_id LIMIT ?` — a keyset-paginated, indexed scan that returns only genuinely interrupted rows. No additional column or index is needed beyond the existing `status` index created by V1.

#### Consumer-driven FAILED retry: `recoverFailedExecutions(maxAttempts)`

`recoverFailedExecutions(int maxAttempts)` retries all `FAILED` executions with `attempt_number < maxAttempts` in parallel via the consumer-supplied `recoveryExecutor`. It is the distributed-safe alternative to the in-process coordinator path — no `RetryCoordinator` is required.

- **Scope: `FAILED` only.** Do **not** run this sweep and an auto-retry coordinator on the same definition — with a coordinator, `FAILED` means "policy exhausted", and the sweep would override that decision.
- **Bounded by `maxAttempts`.** Only rows with `attempt_number < maxAttempts` are fetched; exhausted rows stop being returned naturally.
- **Attempt-number semantics.** Each sweep invocation that retries an execution increments its `attempt_number` by exactly 1 before calling `proceed()`. Generic `proceed()` does not increment this counter.
- **Requires** a `recoveryExecutor` on the definition; throws `IllegalStateException` if none is configured. `maxAttempts` must be `> 0`.
- **Async — returns submitted count.** Tasks are submitted immediately; the returned count may include in-flight executions.
- **Single-instance or leader-election for distributed deployments.** Do not call from every replica.

```java
// Example: leader-elected Spring scheduled task
@Scheduled(cron = "0 */5 * * * *")   // every 5 minutes
public void sweepFailedExecutions() {
    int submitted = manager.recoverFailedExecutions(5);
    log.info("Failed-execution sweep submitted {} retries", submitted);
}
```

The V2 migration adds a composite index on `(status, attempt_number)` to accelerate the sweep query `WHERE status = 'FAILED' AND attempt_number < ?`.

---

### Schema migrations

The migration runner is controlled by `fsm.jdbc.migration.*` properties:

| Property | Default | Description |
|---|---|---|
| `fsm.jdbc.migration.mode` | `UPDATE` | `UPDATE` — apply pending migrations automatically at startup. `VALIDATE` — fail fast if the schema is behind; logs the full pending SQL for operators to run out-of-band. `OFF` — disable schema management entirely. |
| `fsm.jdbc.migration.strict-checksum` | `false` | When `true`, a checksum mismatch on an already-applied migration causes a hard startup failure. When `false` (default), a warning is logged. |
| `fsm.jdbc.migration.lock-ttl` | `5m` | How long the distributed migration lock is considered valid before being treated as stale and taken over by another node. |
| `fsm.jdbc.migration.lock-wait-timeout` | `30s` | How long to wait for the migration lock before aborting startup with an error. |
| `fsm.jdbc.lock.enabled` | `true` | When `true`, expose a `JdbcExecutionLockProvider` bean. Set to `false` to opt out and supply your own `ExecutionLockProvider` bean. |
| `fsm.jdbc.lock.ttl` | `5m` | How long an acquired execution lock is considered valid before it is treated as stale and taken over by another node (crashed-node recovery). |

**Full `application.yml` example:**

```yaml
fsm:
  jdbc:
    enabled: true
    dialect: postgresql          # postgresql (default), mysql, h2, sqlite, oracle
    migration:
      mode: UPDATE               # UPDATE (default), VALIDATE, or OFF
      strict-checksum:  true     # fail on checksum mismatch (false: warn only)
      lock-ttl: 5m               # stale migration-lock TTL (default: 5 minutes)
      lock-wait-timeout: 30s     # how long to wait for the migration lock (default: 30s)
    lock:
      enabled: true              # expose ExecutionLockProvider bean (default: true)
      ttl: 5m                    # stale execution-lock TTL (default: 5 minutes)
```

**Managing schema out-of-band (production teams that apply DDL themselves):**

Set `mode: VALIDATE` to disable automatic DDL while still verifying the schema at startup — the runner fails fast if the database is behind and logs the exact pending SQL statements for you to apply. Set `mode: OFF` to skip schema management entirely and take full responsibility for keeping the schema in sync.

The bundled per-dialect SQL files are located at `io/hypercell/fsm/db/migrations/<dialect>/` inside the `fsm-jdbc` jar (e.g. extract from the jar or view in source). Each dialect folder contains:
- `bootstrap.sql` — creates `fsm_schema_history` and `fsm_schema_lock` and seeds the lock row
- `V1__create_snapshots.sql` — creates the `fsm_snapshots` table and its `status` index
- `V2__add_attempt_number_index.sql` — adds the composite `(status, attempt_number)` index for the `recoverFailedExecutions` sweep
- `V3__create_execution_locks.sql` — creates the `fsm_execution_locks` table used by `JdbcExecutionLockProvider`

You can apply these files directly with your database CLI, or feed them into an existing Flyway/Liquibase pipeline. `VALIDATE` mode is the recommended choice for production teams that apply DDL through a controlled change-management process: it guarantees the application will not start against a schema that is behind, without ever touching the database itself.

---

## Distributed execution locking

The `fsm-spring-boot-starter-jdbc` starter auto-configures a `JdbcExecutionLockProvider` bean that prevents two service instances from processing the same snapshot simultaneously.

### How it works

Before any `trigger()`, `proceed()`, `resume()`, or recovery call, the manager acquires a lock for the `executionId`. `JdbcExecutionLockProvider` uses the `fsm_execution_locks` table (created by migration V3):

| Column | Type | Description |
|---|---|---|
| `execution_id` | VARCHAR(255) PK | The snapshot key being locked |
| `locked_by` | VARCHAR(255) | `hostname@pid#threadId` of the owning node |
| `locked_at` | VARCHAR(255) | ISO-8601 timestamp when the lock was acquired |

Acquire is a two-step, non-blocking operation:
1. **UPDATE (stale takeover)** — if a row exists with `locked_at` older than the TTL (default 5 min), claim it as stale. Handles crashed-node recovery automatically.
2. **INSERT (normal path)** — no row exists (fresh execution or lock was released). The PRIMARY KEY constraint ensures exactly one concurrent INSERT wins; the loser receives `ConcurrentExecutionException` immediately.

Release **deletes** the row rather than NULLing columns, keeping the table sparse (only in-flight and stale locks have rows).

### Wiring into your definition

The starter exposes `ExecutionLockProvider` as a Spring bean. Wire it into your definition:

```java
@Configuration
public class OrderWorkflowConfig {

    @Bean
    public StateMachineDefinition<OrderContext> orderWorkflow(
            JdbcSnapshotRepository snapshotRepository,
            ExecutionLockProvider executionLockProvider,  // auto-configured by starter
            OrderRepository orderRepository) {

        return StateMachine.<OrderContext>define("order-workflow")
            .initial("PENDING")
            .snapshotRepository(snapshotRepository)
            .executionLockProvider(executionLockProvider)   // <- distributed lock
            .contextLoader(orderId -> { ... })
            // ... states
            .build();
    }
}
```

If no `ExecutionLockProvider` is wired, the definition falls back to `ReentrantExecutionLockProvider` (single-JVM only).

### Disabling or replacing the lock provider

```yaml
# Disable the auto-configured bean (define your own @Bean ExecutionLockProvider):
fsm:
  jdbc:
    lock:
      enabled: false
```

Or supply a custom `@Bean ExecutionLockProvider` — `@ConditionalOnMissingBean` ensures the starter's bean is skipped.

### Stale-lock TTL

If a node crashes mid-execution, its lock row remains in the table. The next node that tries to acquire the same lock will claim it via the UPDATE path once `locked_at` is older than `fsm.jdbc.lock.ttl` (default 5 minutes). Set this to a value comfortably longer than your longest expected execution:

```yaml
fsm:
  jdbc:
    lock:
      ttl: 10m   # override if sub-steps can run longer than 5 minutes
```

---

## State validation and error handling

Use `eligibilityOf()` and `currentState()` for advisory pre-checks, and handle the new `IllegalTriggerStateException` for FAILED/RUNNING/RETRY_SCHEDULED executions:

```java
@PostMapping("/trigger/{event}")
public ResponseEntity<?> trigger(@PathVariable String orderId, @PathVariable String event) {
    // Safely check if we're starting a fresh workflow
    Optional<ExecutionSnapshot> snapshot = manager.snapshotOf(orderId);
    if (snapshot.isEmpty() && !manager.isInitialState("PENDING")) {
        return ResponseEntity.status(400).body("Initial state is not PENDING");
    }

    try {
        // 1.0.0-RC2+: trigger() is strict — recover before triggering.
        TriggerEligibility eligibility = manager.eligibilityOf(orderId);
        if (eligibility == TriggerEligibility.NEEDS_PROCEED) {
            manager.proceed(orderId);
        } else if (eligibility == TriggerEligibility.NEEDS_RESUME) {
            manager.resume(orderId);
        }

        ManagedTransitionResult<OrderContext> result = manager.trigger(orderId, event);
        
        // Inspect root cause of failure if needed
        if (result.isFailed()) {
            Throwable rootCause = result.getRootCause();
            if (rootCause instanceof InventoryException) {
                // Handle inventory shortage
            } else if (rootCause instanceof PaymentException) {
                // Handle payment failure
            }
        }
        
        return ResponseEntity.ok(result);
    } catch (ConcurrentExecutionException e) {
        return ResponseEntity.status(409).body("Concurrent request");
    } catch (IllegalTriggerStateException e) {
        // Execution is FAILED/RUNNING/RETRY_SCHEDULED — guide caller to recover first
        return ResponseEntity.status(409).body(e.getMessage());
    }
}
```

---

## Testing with H2 in-memory database

For unit tests, use H2 in-memory:

```properties
# application-test.properties
spring.datasource.url=jdbc:h2:mem:testdb
spring.datasource.driver-class-name=org.h2.Driver
spring.jpa.hibernate.ddl-auto=create-drop
```

```java
@SpringBootTest
@ActiveProfiles("test")
public class OrderWorkflowTest {

    @Autowired
    private StateMachineManager<OrderContext> manager;

    @Test
    public void testApproveAndProcess() {
        ManagedTransitionResult<OrderContext> result = manager.initialize("order-1");
        assertEquals(ExecutionStatus.RUNNING, result.getExecutionStatus());
        assertEquals("PENDING", result.getToState());

        result = manager.trigger("order-1", "APPROVE");
        assertEquals(ExecutionStatus.RUNNING, result.getExecutionStatus());
        assertEquals("PROCESSING", result.getToState());
    }
}
```

---

## See also

- [Persistence and retry](05-persistence-and-retry.md) — snapshot model, retry policies, recovery
- [Use cases](03-use-cases.md) — more integration patterns
- [JDBC module README](../fsm-jdbc/README.md) — lower-level JDBC API reference
