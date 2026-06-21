package io.hypercell.fsm.integration;

import io.hypercell.fsm.OrderContext;
import io.hypercell.fsm.StateMachine;
import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.core.StateMachineDefinition;
import io.hypercell.fsm.manager.StateMachineManager;
import io.hypercell.fsm.resume.ExecutionSnapshot;
import io.hypercell.fsm.resume.InMemorySnapshotRepository;
import io.hypercell.fsm.resume.SnapshotStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for {@link StateMachineManager#recoverFailedExecutions(int)}.
 * <p>
 * Uses {@link InMemorySnapshotRepository} and a consumer-supplied executor to
 * drive retries, mirroring the style of {@link CrashRecoveryTest}.
 */
class RecoverFailedTest {

    // ------------------------------------------------------------------ validation

    @Test
    void recoverFailedExecutions_noExecutor_throwsIllegalState() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-no-exec-failed")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .state("PENDING")
                .on("GO").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        assertThatThrownBy(() -> manager.recoverFailedExecutions(3))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recoveryExecutor");
    }

    @Test
    void recoverFailedExecutions_zeroMaxAttempts_throwsIllegalArgument() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-zero-max")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .recoveryExecutor(executor)
                .state("PENDING")
                .on("GO").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);
        executor.shutdown();

        assertThatThrownBy(() -> manager.recoverFailedExecutions(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAttempts");
    }

    @Test
    void recoverFailedExecutions_negativeMaxAttempts_throwsIllegalArgument() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-neg-max")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .recoveryExecutor(executor)
                .state("PENDING")
                .on("GO").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);
        executor.shutdown();

        assertThatThrownBy(() -> manager.recoverFailedExecutions(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAttempts");
    }

    // ------------------------------------------------------------------ basic retry

    /**
     * A FAILED execution is retried; attemptNumber is incremented by exactly 1 per sweep.
     * Uses a permanently-failing sub-step so the failure snapshot captures the bumped
     * attempt_number (the success path's WAITING checkpoint uses takeCheckpoint() which
     * does not preserve attempt_number in the current library design).
     */
    @Test
    void recoverFailed_retries_failedExecution_incrementsAttemptNumberByOne() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        AtomicInteger workCount = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-retry-bump")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .recoveryExecutor(executor)
                .state("PENDING")
                .on("GO").to("WORKING").end()
                .and()
                .state("WORKING")
                .subStep("doWork", ctx -> {
                    workCount.incrementAndGet();
                    throw new RuntimeException("still failing"); // always fails so snapshot is saved
                })
                .on("DONE").to("FINISHED").end()
                .and()
                .state("FINISHED").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        // Seed a FAILED execution at attempt_number=1
        repo.save("exec-failed-1", new ExecutionSnapshot.Builder()
                .executionId("exec-failed-1")
                .machineDefinitionId("order-retry-bump")
                .currentStateName("WORKING")
                .failedStateName("WORKING")
                .failedSubStepName("doWork")
                .lastTriggerEvent("GO")
                .attemptNumber(1)
                .lastFailedAt(Instant.now())
                .lastErrorMessage("simulated failure")
                .status(SnapshotStatus.FAILED)
                .capturedAt(Instant.now())
                .build());

        int count = manager.recoverFailedExecutions(5);

        assertThat(count).isEqualTo(1);

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        // doWork should have run once during the sweep
        assertThat(workCount.get()).isEqualTo(1);

        // Still FAILED, but attempt_number bumped by exactly 1 (1 → 2).
        // The failure path saves via takeSnapshot() which preserves this.attemptNumber.
        assertThat(repo.load("exec-failed-1")).isPresent()
                .hasValueSatisfying(s -> {
                    assertThat(s.isFailed()).isTrue();
                    assertThat(s.getAttemptNumber()).isEqualTo(2);
                });
    }

    /**
     * A FAILED execution that fails again on retry stays FAILED at the bumped attempt number.
     */
    @Test
    void recoverFailed_retryFailsAgain_staysFailedAtBumpedAttemptNumber() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        ExecutorService executor = Executors.newFixedThreadPool(2);

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-retry-fail")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .recoveryExecutor(executor)
                .state("PENDING")
                .on("GO").to("WORKING").end()
                .and()
                .state("WORKING")
                .subStep("doWork", ctx -> {
                    throw new RuntimeException("always fails");
                })
                .on("DONE").to("FINISHED").end()
                .and()
                .state("FINISHED").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        // Seed a FAILED execution at attempt_number=1
        repo.save("exec-always-fail", new ExecutionSnapshot.Builder()
                .executionId("exec-always-fail")
                .machineDefinitionId("order-retry-fail")
                .currentStateName("WORKING")
                .failedStateName("WORKING")
                .failedSubStepName("doWork")
                .lastTriggerEvent("GO")
                .attemptNumber(1)
                .lastFailedAt(Instant.now())
                .lastErrorMessage("always fails")
                .status(SnapshotStatus.FAILED)
                .capturedAt(Instant.now())
                .build());

        int count = manager.recoverFailedExecutions(5);
        assertThat(count).isEqualTo(1);

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        // Still FAILED, but attempt_number bumped to 2
        assertThat(repo.load("exec-always-fail")).isPresent()
                .hasValueSatisfying(s -> {
                    assertThat(s.isFailed()).isTrue();
                    assertThat(s.getAttemptNumber()).isEqualTo(2);
                });
    }

    // ------------------------------------------------------------------ bounded by maxAttempts

    /**
     * A FAILED row at attempt_number == maxAttempts is NOT fetched or retried.
     */
    @Test
    void recoverFailed_atCap_notRetried() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        AtomicInteger workCount = new AtomicInteger(0);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-cap")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .recoveryExecutor(executor)
                .state("PENDING")
                .on("GO").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        // At the cap — must NOT be fetched
        repo.save("at-cap", new ExecutionSnapshot.Builder()
                .executionId("at-cap")
                .machineDefinitionId("order-cap")
                .currentStateName("PENDING")
                .failedStateName("PENDING")
                .failedSubStepName("step")
                .attemptNumber(3)
                .lastFailedAt(Instant.now())
                .status(SnapshotStatus.FAILED)
                .capturedAt(Instant.now())
                .build());

        int count = manager.recoverFailedExecutions(3);
        assertThat(count).isEqualTo(0);

        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.SECONDS);

        // Snapshot unchanged
        assertThat(repo.load("at-cap")).isPresent()
                .hasValueSatisfying(s -> assertThat(s.getAttemptNumber()).isEqualTo(3));

        assertThat(workCount.get()).isEqualTo(0);
    }

    /**
     * A permanently-failing row stops being retried once it reaches the cap after enough sweeps.
     */
    @Test
    void recoverFailed_poisonRow_stopsAfterMaxAttemptsSweeps() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        ExecutorService executor;

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-poison")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .recoveryExecutor(Executors.newFixedThreadPool(2))
                .state("PENDING")
                .on("GO").to("WORKING").end()
                .and()
                .state("WORKING")
                .subStep("fail", ctx -> {
                    throw new RuntimeException("poison");
                })
                .on("DONE").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        // Seed at attempt_number=1 — eligible for 2 more sweeps before hitting cap=3
        repo.save("poison", new ExecutionSnapshot.Builder()
                .executionId("poison")
                .machineDefinitionId("order-poison")
                .currentStateName("WORKING")
                .failedStateName("WORKING")
                .failedSubStepName("fail")
                .lastTriggerEvent("GO")
                .attemptNumber(1)
                .lastFailedAt(Instant.now())
                .status(SnapshotStatus.FAILED)
                .capturedAt(Instant.now())
                .build());

        // Sweep 1: attempt_number 1 → 2, fails → FAILED at 2
        executor = definition.recoveryExecutor();
        manager.recoverFailedExecutions(3);
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        assertThat(repo.load("poison").get().getAttemptNumber()).isEqualTo(2);

        // Rebuild manager with a new executor for sweep 2
        StateMachineDefinition<OrderContext> definition2 = StateMachine.<OrderContext>define("order-poison")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .recoveryExecutor(Executors.newFixedThreadPool(2))
                .state("PENDING")
                .on("GO").to("WORKING").end()
                .and()
                .state("WORKING")
                .subStep("fail", ctx -> {
                    throw new RuntimeException("poison");
                })
                .on("DONE").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager2 = StateMachine.manager(definition2, repo);

        // Sweep 2: attempt_number 2 → 3, fails → FAILED at 3
        manager2.recoverFailedExecutions(3);
        definition2.recoveryExecutor().shutdown();
        definition2.recoveryExecutor().awaitTermination(5, TimeUnit.SECONDS);
        assertThat(repo.load("poison").get().getAttemptNumber()).isEqualTo(3);

        // Sweep 3: attempt_number == maxAttempts (3) — not fetched → count = 0
        StateMachineDefinition<OrderContext> definition3 = StateMachine.<OrderContext>define("order-poison")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .recoveryExecutor(Executors.newSingleThreadExecutor())
                .state("PENDING")
                .on("GO").to("WORKING").end()
                .and()
                .state("WORKING")
                .subStep("fail", ctx -> {
                    throw new RuntimeException("poison");
                })
                .on("DONE").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager3 = StateMachine.manager(definition3, repo);
        int count3 = manager3.recoverFailedExecutions(3);
        definition3.recoveryExecutor().shutdown();
        definition3.recoveryExecutor().awaitTermination(2, TimeUnit.SECONDS);

        assertThat(count3).isEqualTo(0);
        // attempt_number stays at 3 — not touched by sweep 3
        assertThat(repo.load("poison").get().getAttemptNumber()).isEqualTo(3);
    }

    // ------------------------------------------------------------------ only FAILED rows swept

    @Test
    void recoverFailed_onlyFailedRows_otherStatusesUntouched() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        AtomicInteger workCount = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-filter")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .recoveryExecutor(executor)
                .state("PENDING")
                .on("GO").to("WORKING").end()
                .and()
                .state("WORKING")
                .subStep("doWork", ctx -> {
                    workCount.incrementAndGet();
                    return ActionResult.success();
                })
                .on("DONE").to("FINISHED").end()
                .and()
                .state("FINISHED").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        // Only this FAILED row should be retried
        repo.save("eligible", new ExecutionSnapshot.Builder()
                .executionId("eligible")
                .machineDefinitionId("order-filter")
                .currentStateName("WORKING")
                .failedStateName("WORKING")
                .failedSubStepName("doWork")
                .lastTriggerEvent("GO")
                .attemptNumber(1)
                .lastFailedAt(Instant.now())
                .status(SnapshotStatus.FAILED)
                .capturedAt(Instant.now())
                .build());

        // WAITING — must not be touched
        repo.save("waiting-1", new ExecutionSnapshot.Builder()
                .executionId("waiting-1").machineDefinitionId("order-filter")
                .currentStateName("WORKING")
                .status(SnapshotStatus.WAITING).capturedAt(Instant.now()).build());

        // RUNNING — must not be touched
        repo.save("running-1", new ExecutionSnapshot.Builder()
                .executionId("running-1").machineDefinitionId("order-filter")
                .currentStateName("WORKING")
                .status(SnapshotStatus.RUNNING).capturedAt(Instant.now()).build());

        // TERMINATED — must not be touched
        repo.save("terminated-1", new ExecutionSnapshot.Builder()
                .executionId("terminated-1").machineDefinitionId("order-filter")
                .currentStateName("FINISHED")
                .status(SnapshotStatus.TERMINATED).capturedAt(Instant.now()).build());

        int count = manager.recoverFailedExecutions(5);

        assertThat(count).isEqualTo(1);

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        assertThat(workCount.get()).isEqualTo(1);

        // Non-FAILED rows must be unchanged
        assertThat(repo.load("waiting-1").get().getStatus()).isEqualTo(SnapshotStatus.WAITING);
        assertThat(repo.load("running-1").get().getStatus()).isEqualTo(SnapshotStatus.RUNNING);
        assertThat(repo.load("terminated-1").get().getStatus()).isEqualTo(SnapshotStatus.TERMINATED);
    }

    // ------------------------------------------------------------------ returns submitted count

    @Test
    void recoverFailed_returnsSubmittedCount() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        ExecutorService executor = Executors.newFixedThreadPool(4);

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-count")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .recoveryExecutor(executor)
                .state("PENDING")
                .on("GO").to("WORKING").end()
                .and()
                .state("WORKING")
                .subStep("doWork", ctx -> ActionResult.success())
                .on("DONE").to("FINISHED").end()
                .and()
                .state("FINISHED").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        for (int i = 1; i <= 4; i++) {
            repo.save("exec-" + i, new ExecutionSnapshot.Builder()
                    .executionId("exec-" + i)
                    .machineDefinitionId("order-count")
                    .currentStateName("WORKING")
                    .failedStateName("WORKING")
                    .failedSubStepName("doWork")
                    .lastTriggerEvent("GO")
                    .attemptNumber(1)
                    .lastFailedAt(Instant.now())
                    .status(SnapshotStatus.FAILED)
                    .capturedAt(Instant.now())
                    .build());
        }

        int count = manager.recoverFailedExecutions(5);

        assertThat(count).isEqualTo(4);

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void recoverFailed_noFailed_returnsZero() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-empty-failed")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .recoveryExecutor(executor)
                .state("PENDING")
                .on("GO").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);
        executor.shutdown();

        assertThat(manager.recoverFailedExecutions(3)).isEqualTo(0);
    }

    // ------------------------------------------------------------------ multi-page pagination

    @Test
    void recoverFailed_spansMultiplePages_recoversAll() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        AtomicInteger workCount = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(4);

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-bulk-failed")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .recoveryExecutor(executor)
                .state("PENDING")
                .on("GO").to("WORKING").end()
                .and()
                .state("WORKING")
                .subStep("doWork", ctx -> {
                    workCount.incrementAndGet();
                    return ActionResult.success();
                })
                .on("DONE").to("FINISHED").end()
                .and()
                .state("FINISHED").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        // 250 FAILED executions (UUID ids) — exceeds default PAGE (100) by more than 2x
        int total = 250;
        for (int i = 0; i < total; i++) {
            String id = java.util.UUID.randomUUID().toString();
            repo.save(id, new ExecutionSnapshot.Builder()
                    .executionId(id)
                    .machineDefinitionId("order-bulk-failed")
                    .currentStateName("WORKING")
                    .failedStateName("WORKING")
                    .failedSubStepName("doWork")
                    .lastTriggerEvent("GO")
                    .attemptNumber(1)
                    .lastFailedAt(Instant.now())
                    .status(SnapshotStatus.FAILED)
                    .capturedAt(Instant.now())
                    .build());
        }

        int count = manager.recoverFailedExecutions(5);

        assertThat(count).isEqualTo(total);

        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(workCount.get()).isEqualTo(total);
    }

    @Test
    void recoverFailed_customPageSize_recoversAll() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        AtomicInteger workCount = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-page-failed")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .recoveryExecutor(executor)
                .recoveryPageSize(7)
                .state("PENDING")
                .on("GO").to("WORKING").end()
                .and()
                .state("WORKING")
                .subStep("doWork", ctx -> {
                    workCount.incrementAndGet();
                    return ActionResult.success();
                })
                .on("DONE").to("FINISHED").end()
                .and()
                .state("FINISHED").terminal().and()
                .build();

        assertThat(definition.recoveryPageSize()).isEqualTo(7);

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        int total = 23; // 23 / 7 = 4 pages (7,7,7,2)
        for (int i = 0; i < total; i++) {
            String id = java.util.UUID.randomUUID().toString();
            repo.save(id, new ExecutionSnapshot.Builder()
                    .executionId(id)
                    .machineDefinitionId("order-page-failed")
                    .currentStateName("WORKING")
                    .failedStateName("WORKING")
                    .failedSubStepName("doWork")
                    .lastTriggerEvent("GO")
                    .attemptNumber(1)
                    .lastFailedAt(Instant.now())
                    .status(SnapshotStatus.FAILED)
                    .capturedAt(Instant.now())
                    .build());
        }

        assertThat(manager.recoverFailedExecutions(5)).isEqualTo(total);
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        assertThat(workCount.get()).isEqualTo(total);
    }

    // ------------------------------------------------------------------ success moves to WAITING

    @Test
    void recoverFailed_successOnRetry_movesToWaiting_notReturnedByListFailed() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        ExecutorService executor = Executors.newFixedThreadPool(2);

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-succeed-on-retry")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .recoveryExecutor(executor)
                .state("PENDING")
                .on("GO").to("WORKING").end()
                .and()
                .state("WORKING")
                .subStep("doWork", ctx -> ActionResult.success())
                .on("DONE").to("FINISHED").end()
                .and()
                .state("FINISHED").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        repo.save("exec-succeed", new ExecutionSnapshot.Builder()
                .executionId("exec-succeed")
                .machineDefinitionId("order-succeed-on-retry")
                .currentStateName("WORKING")
                .failedStateName("WORKING")
                .failedSubStepName("doWork")
                .lastTriggerEvent("GO")
                .attemptNumber(1)
                .lastFailedAt(Instant.now())
                .status(SnapshotStatus.FAILED)
                .capturedAt(Instant.now())
                .build());

        manager.recoverFailedExecutions(3);
        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        // After a successful retry, the execution should be WAITING
        assertThat(repo.load("exec-succeed")).isPresent()
                .hasValueSatisfying(s -> assertThat(s.getStatus()).isEqualTo(SnapshotStatus.WAITING));

        // It must not appear in the next sweep's listFailed
        assertThat(repo.listFailed(10, null, 5)).isEmpty();
    }

    // ------------------------------------------------------------------ proceed() does NOT change attemptNumber

    /**
     * Generic manual {@link StateMachineManager#proceed(String)} must NOT increment
     * {@code attempt_number}. Only {@code recoverFailedExecutions} increments it.
     * <p>
     * Uses an always-failing sub-step so the failure snapshot is saved; the failure path
     * ({@code takeSnapshot}) preserves {@code this.attemptNumber} from the machine instance,
     * which was loaded from the original snapshot. If proceed were incrementing the counter,
     * the saved attempt_number would be higher.
     */
    @Test
    void manualProceed_doesNotChangeAttemptNumber() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-manual-proceed")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .state("PENDING")
                .on("GO").to("WORKING").end()
                .and()
                .state("WORKING")
                .subStep("doWork", ctx -> {
                    throw new RuntimeException("still failing"); // always fails to force failure snapshot
                })
                .on("DONE").to("FINISHED").end()
                .and()
                .state("FINISHED").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        // Seed a FAILED execution at attempt_number=2
        repo.save("exec-manual", new ExecutionSnapshot.Builder()
                .executionId("exec-manual")
                .machineDefinitionId("order-manual-proceed")
                .currentStateName("WORKING")
                .failedStateName("WORKING")
                .failedSubStepName("doWork")
                .lastTriggerEvent("GO")
                .attemptNumber(2)
                .lastFailedAt(Instant.now())
                .status(SnapshotStatus.FAILED)
                .capturedAt(Instant.now())
                .build());

        // Manual proceed — must NOT increment attemptNumber
        manager.proceed("exec-manual");

        // After proceed fails, the failure snapshot is saved with this.attemptNumber from
        // the machine instance, which was loaded as 2 from the original snapshot.
        // If proceed had incremented it, we'd see 3 here.
        assertThat(repo.load("exec-manual")).isPresent()
                .hasValueSatisfying(s -> {
                    assertThat(s.isFailed()).isTrue();
                    assertThat(s.getAttemptNumber()).isEqualTo(2);
                });
    }
}
