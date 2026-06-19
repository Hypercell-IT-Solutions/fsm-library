# JDBC and Spring Boot autoconfiguration

For Spring Boot applications, the `fsm-spring-boot-starter-jdbc` module provides automatic configuration of `JdbcSnapshotRepository`, reducing boilerplate and integrating seamlessly with Spring's `DataSource`.

---

## Quick start

**1. Add the Spring Boot starter dependency:**

```xml
<dependency>
    <groupId>net.hypercell</groupId>
    <artifactId>fsm-spring-boot-starter-jdbc</artifactId>
    <version>1.0.0-RC2</version>
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

**3. Inject the auto-configured `JdbcSnapshotRepository` and use it:**

```java
@Component
public class OrderWorkflowService {

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private JdbcSnapshotRepository snapshotRepository;

    @Bean
    public StateMachineDefinition<OrderContext> orderWorkflow() {
        return StateMachine.<OrderContext>define("order-workflow")
            .initial("PENDING")
            .snapshotRepository(snapshotRepository)  // auto-configured bean
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
        }
    }
}
```

---

## What the starter auto-configures

The `fsm-spring-boot-starter-jdbc` module provides:

1. **`JdbcSnapshotRepository` bean** — automatically instantiated with Spring's `DataSource`
2. **Versioned schema migration** — a Liquibase-style migration runner creates and upgrades the schema automatically on startup; two tracking tables (`fsm_schema_history`, `fsm_schema_lock`) record applied versions and guard against concurrent multi-replica migrations
3. **Connection pooling** — uses Spring's configured `DataSource` (typically HikariCP)
4. **No additional properties to set** — works with standard `spring.datasource.*` config

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
            ManagedTransitionResult<OrderContext> result = manager.trigger(orderId, event);
            return ResponseEntity.ok(Map.of(
                "status", result.getExecutionStatus(),
                "currentState", result.getToState()
            ));
        } catch (ConcurrentExecutionException e) {
            return ResponseEntity.status(409).body("Another request is processing this order");
        } catch (CompletedMachineException e) {
            return ResponseEntity.status(409).body("Order already completed");
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

1. **Runs the schema migration runner** — bootstraps the two tracking tables (`fsm_schema_history`, `fsm_schema_lock`), acquires a distributed DB lock, applies any not-yet-applied versioned migrations in order (currently V1: creates the `fsm_snapshots` table and its status index), verifies checksums of previously-applied migrations, and releases the lock. The migration is safe under concurrent multi-replica startup. See [Schema migrations](#schema-migrations) below for configuration options.
2. **Calls `recoverPendingRetries()`** to resume any failed executions that are scheduled for retry

Add this to your configuration:

```java
@Component
public class OrderWorkflowStartupListener implements ApplicationListener<ContextRefreshedEvent> {

    @Autowired
    private StateMachineManager<OrderContext> manager;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        // Resume any failed executions with scheduled retries
        manager.recoverPendingRetries();
    }
}
```

---

### Schema migrations

The migration runner is controlled by `fsm.jdbc.migration.*` properties:

| Property | Default | Description |
|---|---|---|
| `fsm.jdbc.migration.mode` | `UPDATE` | `UPDATE` — apply pending migrations automatically at startup. `VALIDATE` — fail fast if the schema is behind; logs the full pending SQL for operators to run out-of-band. `OFF` — disable schema management entirely. |
| `fsm.jdbc.migration.strict-checksum` | `false` | When `true`, a checksum mismatch on an already-applied migration causes a hard startup failure. When `false` (default), a warning is logged. |
| `fsm.jdbc.migration.lock-ttl` | `5m` | How long the distributed migration lock is considered valid before being treated as stale and taken over by another node. |
| `fsm.jdbc.migration.lock-wait-timeout` | `30s` | How long to wait for the migration lock before aborting startup with an error. |

**Full `application.yml` example:**

```yaml
fsm:
  jdbc:
    enabled: true
    dialect: postgresql          # postgresql (default), mysql, h2, sqlite, oracle
    migration:
      mode: UPDATE               # UPDATE (default), VALIDATE, or OFF
      strict-checksum:  true     # fail on checksum mismatch (false: warn only)
      lock-ttl: 5m               # stale-lock TTL (default: 5 minutes)
      lock-wait-timeout: 30s     # how long to wait for the migration lock (default: 30s)
```

**Managing schema out-of-band (production teams that apply DDL themselves):**

Set `mode: VALIDATE` to disable automatic DDL while still verifying the schema at startup — the runner fails fast if the database is behind and logs the exact pending SQL statements for you to apply. Set `mode: OFF` to skip schema management entirely and take full responsibility for keeping the schema in sync.

The bundled per-dialect SQL files are located at `io/hypercell/fsm/db/migrations/<dialect>/` inside the `fsm-jdbc` jar (e.g. extract from the jar or view in source). Each dialect folder contains:
- `bootstrap.sql` — creates `fsm_schema_history` and `fsm_schema_lock` and seeds the lock row
- `V1__create_snapshots.sql` — creates the `fsm_snapshots` table and its status index

You can apply these files directly with your database CLI, or feed them into an existing Flyway/Liquibase pipeline. `VALIDATE` mode is the recommended choice for production teams that apply DDL through a controlled change-management process: it guarantees the application will not start against a schema that is behind, without ever touching the database itself.

---

## State validation and error handling

Use the new state validation methods to safely guard against configuration changes:

```java
@PostMapping("/trigger/{event}")
public ResponseEntity<?> trigger(@PathVariable String orderId, @PathVariable String event) {
    // Safely check if we're starting a fresh workflow
    Optional<ExecutionSnapshot> snapshot = manager.snapshotOf(orderId);
    if (snapshot.isEmpty() && !manager.isInitialState("PENDING")) {
        return ResponseEntity.status(400).body("Initial state is not PENDING");
    }

    try {
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
