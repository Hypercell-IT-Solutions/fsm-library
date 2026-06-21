package io.hypercell.fsm.integration;

import io.hypercell.fsm.OrderContext;
import io.hypercell.fsm.StateMachine;
import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.core.ExecutionStatus;
import io.hypercell.fsm.core.StateMachineDefinition;
import io.hypercell.fsm.core.StateMachineInstance;
import io.hypercell.fsm.exception.IllegalTriggerStateException;
import io.hypercell.fsm.exception.SubStepExecutionException;
import io.hypercell.fsm.manager.ManagedTransitionResult;
import io.hypercell.fsm.manager.StateMachineManager;
import io.hypercell.fsm.manager.TriggerEligibility;
import io.hypercell.fsm.resume.ExecutionSnapshot;
import io.hypercell.fsm.resume.InMemorySnapshotRepository;
import io.hypercell.fsm.resume.SnapshotRepository;
import io.hypercell.fsm.resume.SnapshotStatus;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for the crash-recovery / per-sub-step checkpointing feature.
 * <p>
 * "Crash simulation" is implemented by: (a) running sub-steps up to point k via a real
 * machine instance so the per-sub-step checkpoint callback fires, (b) saving the resulting
 * RUNNING checkpoint to the repository, then (c) building a fresh instance from that
 * snapshot via {@code definition.resumeInterrupted(...)} + {@code instance.resume()}.
 * The side-effect counters are not reset between these phases to verify idempotency.
 */
class CrashRecoveryTest {

    private StateMachineDefinition<OrderContext> threeStepDefinition(
            AtomicInteger step1Count, AtomicInteger step2Count, AtomicInteger step3Count,
            AtomicInteger entryHookCount, AtomicInteger transitionActionCount,
            SnapshotRepository repo) {

        return StateMachine.<OrderContext>define("order")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .state("PENDING")
                .on("APPROVE").action(ctx -> {
                    transitionActionCount.incrementAndGet();
                    return ActionResult.success();
                }).to("PROCESSING").end()
                .and()
                .state("PROCESSING")
                .onEntry(ctx -> entryHookCount.incrementAndGet())
                .subStep("step1", ctx -> {
                    step1Count.incrementAndGet();
                    return ActionResult.success();
                })
                .subStep("step2", ctx -> {
                    step2Count.incrementAndGet();
                    return ActionResult.success();
                })
                .subStep("step3", ctx -> {
                    step3Count.incrementAndGet();
                    return ActionResult.success();
                })
                .on("COMPLETE").to("SHIPPED").end()
                .and()
                .state("SHIPPED").terminal().and()
                .build();
    }

    /**
     * Simulates a crash after sub-step 2 of 3 completed.
     * Verifies that on resume: steps 1-2 are skipped, step 3 runs once,
     * and the transition action / entry hook are NOT re-invoked.
     */
    @Test
    void crashAfterSubStep2_resumeSkipsCompletedSteps() {
        AtomicInteger step1Count = new AtomicInteger(0);
        AtomicInteger step2Count = new AtomicInteger(0);
        AtomicInteger step3Count = new AtomicInteger(0);
        AtomicInteger entryHookCount = new AtomicInteger(0);
        AtomicInteger transitionActionCount = new AtomicInteger(0);

        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        StateMachineDefinition<OrderContext> definition = threeStepDefinition(
                step1Count, step2Count, step3Count, entryHookCount, transitionActionCount, repo);

        StateMachineInstance<OrderContext> instance =
                definition.newInstance(new OrderContext("order-crash"), "order-crash");

        instance.trigger("APPROVE");

        assertThat(transitionActionCount.get()).isEqualTo(1);
        assertThat(entryHookCount.get()).isEqualTo(1);
        assertThat(step1Count.get()).isEqualTo(1);
        assertThat(step2Count.get()).isEqualTo(1);
        assertThat(step3Count.get()).isEqualTo(1);

        ExecutionSnapshot interruptedSnapshot = new ExecutionSnapshot.Builder()
                .executionId("order-crash-2")
                .machineDefinitionId("order")
                .currentStateName("PROCESSING")
                .lastTriggerEvent("APPROVE")
                .completedSubStepResults(java.util.Map.of(
                        "PROCESSING::step1", ActionResult.success(),
                        "PROCESSING::step2", ActionResult.success()
                ))
                .status(SnapshotStatus.RUNNING)
                .capturedAt(java.time.Instant.now())
                .build();

        repo.save("order-crash-2", interruptedSnapshot);

        step1Count.set(0);
        step2Count.set(0);
        step3Count.set(0);
        entryHookCount.set(0);
        transitionActionCount.set(0);

        StateMachineInstance<OrderContext> resumed = definition.resumeInterrupted(
                new OrderContext("order-crash-2"), interruptedSnapshot, repo);
        resumed.resume();

        assertThat(step1Count.get()).as("step1 must not re-run").isEqualTo(0);
        assertThat(step2Count.get()).as("step2 must not re-run").isEqualTo(0);
        assertThat(step3Count.get()).as("step3 must run once").isEqualTo(1);
        assertThat(transitionActionCount.get()).as("transition action must not re-run").isEqualTo(0);
        assertThat(entryHookCount.get()).as("entry hook must not re-run").isEqualTo(0);
        assertThat(resumed.status()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(resumed.currentState().name()).isEqualTo("PROCESSING");
    }

    /**
     * Verifies that when ALL sub-steps are already in the checkpoint, resume() is a no-op
     * (all sub-steps skipped, no state change).
     */
    @Test
    void allSubStepsComplete_inSnapshot_resumeIsNoOp() {
        AtomicInteger step1Count = new AtomicInteger(0);
        AtomicInteger step2Count = new AtomicInteger(0);
        AtomicInteger step3Count = new AtomicInteger(0);

        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        StateMachineDefinition<OrderContext> definition = threeStepDefinition(
                step1Count, step2Count, step3Count,
                new AtomicInteger(), new AtomicInteger(), repo);

        ExecutionSnapshot atRestSnapshot = new ExecutionSnapshot.Builder()
                .executionId("order-atrest")
                .machineDefinitionId("order")
                .currentStateName("PROCESSING")
                .lastTriggerEvent("APPROVE")
                .completedSubStepResults(java.util.Map.of(
                        "PROCESSING::step1", ActionResult.success(),
                        "PROCESSING::step2", ActionResult.success(),
                        "PROCESSING::step3", ActionResult.success()
                ))
                .status(SnapshotStatus.RUNNING)
                .capturedAt(java.time.Instant.now())
                .build();

        repo.save("order-atrest", atRestSnapshot);

        StateMachineInstance<OrderContext> resumed = definition.resumeInterrupted(
                new OrderContext("order-atrest"), atRestSnapshot, repo);
        resumed.resume();

        assertThat(step1Count.get()).isEqualTo(0);
        assertThat(step2Count.get()).isEqualTo(0);
        assertThat(step3Count.get()).isEqualTo(0);
        assertThat(resumed.status()).isEqualTo(ExecutionStatus.RUNNING);
    }

    /**
     * New contract (1.0.0-RC2+): triggering on a RUNNING (interrupted) execution throws
     * {@link IllegalTriggerStateException} with NEEDS_RESUME eligibility.
     * The caller must call resume() first, then trigger() once the execution is WAITING.
     */
    @Test
    void lazyResume_retriggering_sameEvent_onInterrupted_throwsAndRequiresExplicitResume() {
        AtomicInteger step1Count = new AtomicInteger(0);
        AtomicInteger step2Count = new AtomicInteger(0);
        AtomicInteger step3Count = new AtomicInteger(0);

        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        StateMachineDefinition<OrderContext> definition = threeStepDefinition(
                step1Count, step2Count, step3Count,
                new AtomicInteger(), new AtomicInteger(), repo);
        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        ExecutionSnapshot interruptedSnapshot = new ExecutionSnapshot.Builder()
                .executionId("order-lazy")
                .machineDefinitionId("order")
                .currentStateName("PROCESSING")
                .lastTriggerEvent("APPROVE")
                .completedSubStepResults(java.util.Map.of(
                        "PROCESSING::step1", ActionResult.success(),
                        "PROCESSING::step2", ActionResult.success()
                ))
                .status(SnapshotStatus.RUNNING)
                .capturedAt(java.time.Instant.now())
                .build();
        repo.save("order-lazy", interruptedSnapshot);

        assertThatThrownBy(() -> manager.trigger("order-lazy", "APPROVE"))
                .isInstanceOf(IllegalTriggerStateException.class)
                .hasMessageContaining("RUNNING")
                .hasMessageContaining("resume()");

        assertThat(manager.eligibilityOf("order-lazy")).isEqualTo(TriggerEligibility.NEEDS_RESUME);

        assertThat(step1Count.get()).isEqualTo(0);
        assertThat(step2Count.get()).isEqualTo(0);
        assertThat(step3Count.get()).isEqualTo(0);

        ManagedTransitionResult<OrderContext> resumeResult = manager.resume("order-lazy");
        assertThat(resumeResult.getExecutionStatus()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(resumeResult.getToState()).isEqualTo("PROCESSING");

        assertThat(step1Count.get()).isEqualTo(0);
        assertThat(step2Count.get()).isEqualTo(0);
        assertThat(step3Count.get()).isEqualTo(1);

        assertThat(manager.eligibilityOf("order-lazy")).isEqualTo(TriggerEligibility.READY);

        ManagedTransitionResult<OrderContext> triggerResult = manager.trigger("order-lazy", "COMPLETE");
        assertThat(triggerResult.getToState()).isEqualTo("SHIPPED");
        assertThat(triggerResult.getExecutionStatus()).isEqualTo(ExecutionStatus.TERMINATED);
    }

    /**
     * New contract: triggering a different event on a RUNNING (interrupted) execution also throws.
     * The caller must resume() first (completing the in-flight transition), then trigger().
     */
    @Test
    void lazyResume_newEvent_onInterrupted_throwsAndRequiresExplicitResumeThenTrigger() {
        AtomicInteger step1Count = new AtomicInteger(0);
        AtomicInteger step2Count = new AtomicInteger(0);
        AtomicInteger step3Count = new AtomicInteger(0);

        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        StateMachineDefinition<OrderContext> definition = threeStepDefinition(
                step1Count, step2Count, step3Count,
                new AtomicInteger(), new AtomicInteger(), repo);
        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        ExecutionSnapshot interruptedSnapshot = new ExecutionSnapshot.Builder()
                .executionId("order-new-event")
                .machineDefinitionId("order")
                .currentStateName("PROCESSING")
                .lastTriggerEvent("APPROVE")
                .completedSubStepResults(java.util.Map.of("PROCESSING::step1", ActionResult.success()))
                .status(SnapshotStatus.RUNNING)
                .capturedAt(java.time.Instant.now())
                .build();
        repo.save("order-new-event", interruptedSnapshot);

        assertThatThrownBy(() -> manager.trigger("order-new-event", "COMPLETE"))
                .isInstanceOf(IllegalTriggerStateException.class)
                .hasMessageContaining("RUNNING")
                .hasMessageContaining("resume()");

        assertThat(manager.eligibilityOf("order-new-event")).isEqualTo(TriggerEligibility.NEEDS_RESUME);

        assertThat(step1Count.get()).isEqualTo(0);
        assertThat(step2Count.get()).isEqualTo(0);
        assertThat(step3Count.get()).isEqualTo(0);

        ManagedTransitionResult<OrderContext> resumeResult = manager.resume("order-new-event");
        assertThat(resumeResult.getExecutionStatus()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(resumeResult.getToState()).isEqualTo("PROCESSING");

        assertThat(step1Count.get()).isEqualTo(0);
        assertThat(step2Count.get()).isEqualTo(1);
        assertThat(step3Count.get()).isEqualTo(1);

        assertThat(manager.eligibilityOf("order-new-event")).isEqualTo(TriggerEligibility.READY);

        ManagedTransitionResult<OrderContext> result = manager.trigger("order-new-event", "COMPLETE");
        assertThat(result.getToState()).isEqualTo("SHIPPED");
        assertThat(result.getExecutionStatus()).isEqualTo(ExecutionStatus.TERMINATED);
    }

    @Test
    void resume_interruptedExecution_completes() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        AtomicInteger stepCount = new AtomicInteger(0);

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .state("PENDING")
                .on("GO").to("WORKING").end()
                .and()
                .state("WORKING")
                .subStep("doWork", ctx -> {
                    stepCount.incrementAndGet();
                    return ActionResult.success();
                })
                .on("DONE").to("FINISHED").end()
                .and()
                .state("FINISHED").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        ExecutionSnapshot interrupted = new ExecutionSnapshot.Builder()
                .executionId("exec-resume")
                .machineDefinitionId("order")
                .currentStateName("WORKING")
                .lastTriggerEvent("GO")
                .completedSubStepResults(java.util.Map.of())
                .status(SnapshotStatus.RUNNING)
                .capturedAt(java.time.Instant.now())
                .build();
        repo.save("exec-resume", interrupted);

        ManagedTransitionResult<OrderContext> result = manager.resume("exec-resume");

        assertThat(result.getExecutionStatus()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(result.getToState()).isEqualTo("WORKING");
        assertThat(stepCount.get()).isEqualTo(1);
    }

    @Test
    void resume_atRestRunning_isNoOp() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        AtomicInteger stepCount = new AtomicInteger(0);

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-noop")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .state("PENDING")
                .on("GO").to("WORKING").end()
                .and()
                .state("WORKING")
                .subStep("doWork", ctx -> {
                    stepCount.incrementAndGet();
                    return ActionResult.success();
                })
                .on("DONE").to("FINISHED").end()
                .and()
                .state("FINISHED").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        ExecutionSnapshot atRest = new ExecutionSnapshot.Builder()
                .executionId("exec-atrest")
                .machineDefinitionId("order-noop")
                .currentStateName("WORKING")
                .lastTriggerEvent("GO")
                .completedSubStepResults(java.util.Map.of("WORKING::doWork", ActionResult.success()))
                .status(SnapshotStatus.RUNNING)
                .capturedAt(java.time.Instant.now())
                .build();
        repo.save("exec-atrest", atRest);

        ManagedTransitionResult<OrderContext> result = manager.resume("exec-atrest");

        assertThat(result.getExecutionStatus()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(result.getToState()).isEqualTo("WORKING");
        assertThat(stepCount.get()).isEqualTo(0);
    }

    @Test
    void resume_onFailedExecution_throwsIllegalState() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-fail-test")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .state("PENDING")
                .subStep("bad", ctx -> { throw new RuntimeException("oops"); })
                .on("GO").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);
        manager.trigger("exec-fail2", "GO");  // creates FAILED snapshot

        assertThatThrownBy(() -> manager.resume("exec-fail2"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("FAILED")
                .hasMessageContaining("proceed()");
    }

    @Test
    void resume_onTerminatedExecution_throwsIllegalState() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-comp-test")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .state("PENDING")
                .on("GO").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);
        manager.trigger("exec-comp", "GO");  // TERMINATED

        assertThatThrownBy(() -> manager.resume("exec-comp"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("TERMINATED");
    }

    @Test
    void recoverInterruptedExecutions_noExecutor_throwsIllegalState() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-no-exec")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .state("PENDING")
                .on("GO").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        assertThatThrownBy(manager::recoverInterruptedExecutions)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recoveryExecutor");
    }

    @Test
    void recoverInterruptedExecutions_resumesOnlyInterruptedRows_returnsCount() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        AtomicInteger workCount = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-bulk")
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

        repo.save("interrupted-1", new ExecutionSnapshot.Builder()
                .executionId("interrupted-1").machineDefinitionId("order-bulk")
                .currentStateName("WORKING").lastTriggerEvent("GO")
                .completedSubStepResults(java.util.Map.of())
                .status(SnapshotStatus.RUNNING).capturedAt(java.time.Instant.now()).build());

        repo.save("interrupted-2", new ExecutionSnapshot.Builder()
                .executionId("interrupted-2").machineDefinitionId("order-bulk")
                .currentStateName("WORKING").lastTriggerEvent("GO")
                .completedSubStepResults(java.util.Map.of())
                .status(SnapshotStatus.RUNNING).capturedAt(java.time.Instant.now()).build());

        // WAITING = at-rest (non-terminal); should NOT be returned by listInterrupted
        repo.save("waiting-1", new ExecutionSnapshot.Builder()
                .executionId("waiting-1").machineDefinitionId("order-bulk")
                .currentStateName("WORKING").lastTriggerEvent("GO")
                .completedSubStepResults(java.util.Map.of("WORKING::doWork", ActionResult.success()))
                .status(SnapshotStatus.WAITING).capturedAt(java.time.Instant.now()).build());

        // FAILED: should be ignored
        repo.save("failed-1", new ExecutionSnapshot.Builder()
                .executionId("failed-1").machineDefinitionId("order-bulk")
                .currentStateName("WORKING").failedStateName("WORKING").failedSubStepName("doWork")
                .status(SnapshotStatus.FAILED).capturedAt(java.time.Instant.now()).build());

        int count = manager.recoverInterruptedExecutions();

        // Only the 2 interrupted ones should be submitted
        assertThat(count).isEqualTo(2);

        // Drain the executor and wait for completions
        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        // Each interrupted execution ran doWork once
        assertThat(workCount.get()).isEqualTo(2);

        // Verify the WAITING snapshot was not touched
        assertThat(repo.load("waiting-1")).isPresent()
                .hasValueSatisfying(s -> assertThat(s.getStatus()).isEqualTo(SnapshotStatus.WAITING));
    }

    @Test
    void recoverInterruptedExecutions_noInterrupted_returnsZero() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-empty")
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

        // No RUNNING snapshots at all
        assertThat(manager.recoverInterruptedExecutions()).isEqualTo(0);
    }

    @Test
    void recoverInterruptedExecutions_spansMultiplePages_recoversAll() throws Exception {
        // Regression test: with more interrupted rows than one keyset page (PAGE=100), the
        // sweep must keep paging until the set is exhausted. Uses UUID execution IDs to
        // confirm keyset pagination is correct for non-sequential keys (ordering relies only
        // on execution_id being a unique total order — the PK — not on any chronological meaning).
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        AtomicInteger workCount = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(4);

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-bulk-pages")
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

        // 250 interrupted executions = 2.5 keyset pages — exceeds PAGE (100) by more than 2x.
        int total = 250;
        for (int i = 0; i < total; i++) {
            String id = java.util.UUID.randomUUID().toString();
            repo.save(id, new ExecutionSnapshot.Builder()
                    .executionId(id).machineDefinitionId("order-bulk-pages")
                    .currentStateName("WORKING").lastTriggerEvent("GO")
                    .completedSubStepResults(java.util.Map.of())
                    .status(SnapshotStatus.RUNNING).capturedAt(java.time.Instant.now()).build());
        }

        int count = manager.recoverInterruptedExecutions();

        // All 250 must be submitted — not just the first page of 100.
        assertThat(count).isEqualTo(total);

        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        // Every interrupted execution ran its sub-step exactly once.
        assertThat(workCount.get()).isEqualTo(total);
    }

    @Test
    void recoverInterruptedExecutions_customPageSize_recoversAll() throws Exception {
        // A configured recoveryPageSize smaller than the backlog must still recover everything
        // (the sweep paginates with the configured page size).
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        AtomicInteger workCount = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(2);

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-page")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .recoveryExecutor(executor)
                .recoveryPageSize(7)   // small custom page size
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

        int total = 23; // 23 / 7 = 4 pages (7,7,7,2) — exercises pagination with the custom size
        for (int i = 0; i < total; i++) {
            String id = java.util.UUID.randomUUID().toString();
            repo.save(id, new ExecutionSnapshot.Builder()
                    .executionId(id).machineDefinitionId("order-page")
                    .currentStateName("WORKING").lastTriggerEvent("GO")
                    .completedSubStepResults(java.util.Map.of())
                    .status(SnapshotStatus.RUNNING).capturedAt(java.time.Instant.now()).build());
        }

        assertThat(manager.recoverInterruptedExecutions()).isEqualTo(total);
        executor.shutdown();
        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        assertThat(workCount.get()).isEqualTo(total);
    }

    @Test
    void recoveryPageSize_defaultsTo100_andRejectsNonPositive() {
        StateMachineDefinition<OrderContext> def = StateMachine.<OrderContext>define("order-default-page")
                .initial("PENDING")
                .state("PENDING").terminal().and()
                .build();
        assertThat(def.recoveryPageSize()).isEqualTo(100);

        assertThatThrownBy(() -> StateMachine.<OrderContext>define("bad")
                .initial("PENDING")
                .recoveryPageSize(0)
                .state("PENDING").terminal().and()
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recoveryPageSize");
    }

    // ------------------------------------------------------------------ listInterrupted tests

    @Test
    void listInterrupted_returnsOnlyRunningSnapshots_notWaitingOrTerminated() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();

        repo.save("r1", new ExecutionSnapshot.Builder()
                .executionId("r1").machineDefinitionId("m").currentStateName("S")
                .status(SnapshotStatus.RUNNING).capturedAt(java.time.Instant.now()).build());
        repo.save("r2", new ExecutionSnapshot.Builder()
                .executionId("r2").machineDefinitionId("m").currentStateName("S")
                .status(SnapshotStatus.RUNNING).capturedAt(java.time.Instant.now()).build());
        repo.save("f1", new ExecutionSnapshot.Builder()
                .executionId("f1").machineDefinitionId("m").currentStateName("S")
                .status(SnapshotStatus.FAILED).capturedAt(java.time.Instant.now()).build());
        repo.save("w1", new ExecutionSnapshot.Builder()
                .executionId("w1").machineDefinitionId("m").currentStateName("S")
                .status(SnapshotStatus.WAITING).capturedAt(java.time.Instant.now()).build());
        repo.save("t1", new ExecutionSnapshot.Builder()
                .executionId("t1").machineDefinitionId("m").currentStateName("S")
                .status(SnapshotStatus.TERMINATED).capturedAt(java.time.Instant.now()).build());

        java.util.List<ExecutionSnapshot> interrupted = repo.listInterrupted(10, null);
        assertThat(interrupted).hasSize(2);
        assertThat(interrupted).extracting(ExecutionSnapshot::getExecutionId)
                .containsExactlyInAnyOrder("r1", "r2");
    }

    @Test
    void listInterrupted_keysetPagination_walksAllRows() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        for (int i = 1; i <= 5; i++) {
            repo.save("exec-" + i, new ExecutionSnapshot.Builder()
                    .executionId("exec-" + i).machineDefinitionId("m").currentStateName("S")
                    .status(SnapshotStatus.RUNNING).capturedAt(java.time.Instant.now()).build());
        }
        // Also add a WAITING row — must not appear
        repo.save("waiting-1", new ExecutionSnapshot.Builder()
                .executionId("waiting-1").machineDefinitionId("m").currentStateName("S")
                .status(SnapshotStatus.WAITING).capturedAt(java.time.Instant.now()).build());

        // Page size = 2; walk through all pages
        java.util.List<String> seen = new java.util.ArrayList<>();
        String lastId = null;
        while (true) {
            java.util.List<ExecutionSnapshot> page = repo.listInterrupted(2, lastId);
            if (page.isEmpty()) break;
            page.forEach(s -> seen.add(s.getExecutionId()));
            if (page.size() < 2) break;
            lastId = page.get(page.size() - 1).getExecutionId();
        }
        assertThat(seen).hasSize(5);
        assertThat(seen).doesNotContain("waiting-1");
    }

    // ------------------------------------------------------------------ regression: new-state entry checkpoint

    /**
     * Regression test for the crash-recovery edge case:
     * when the process crashes during a new state's FIRST sub-step, the persisted snapshot
     * must reflect the NEW state so that recovery resumes at C, not B.
     * <p>
     * Two assertions are made:
     * 1. Direct: after a real trigger(B→C), the repository received a RUNNING checkpoint
     *    stamped with state C and empty C-sub-step results BEFORE the first per-sub-step
     *    checkpoint. This is the on-entry checkpoint added by the fix.
     * 2. Recovery: given a RUNNING snapshot at C with no completed sub-steps (mimicking
     *    a crash right after the on-entry checkpoint), resumeInterrupted+resume correctly
     *    runs C's sub-steps from scratch (both c1 and c2) and does NOT re-run B's sub-steps.
     */
    @Test
    void recoverInterrupted_afterTransition_resumesNewStateNotOld() {
        AtomicInteger bStepCount = new AtomicInteger(0);
        AtomicInteger c1Count = new AtomicInteger(0);
        AtomicInteger c2Count = new AtomicInteger(0);

        // Track every save in order, so we can assert the first RUNNING@C save comes before
        // the first per-sub-step RUNNING@C save.
        java.util.concurrent.CopyOnWriteArrayList<ExecutionSnapshot> allSaves =
                new java.util.concurrent.CopyOnWriteArrayList<>();

        InMemorySnapshotRepository base = InMemorySnapshotRepository.create();
        SnapshotRepository intercepting = new SnapshotRepository() {
            @Override
            public void save(String id, ExecutionSnapshot s) {
                allSaves.add(s);
                base.save(id, s);
            }

            @Override
            public java.util.Optional<ExecutionSnapshot> load(String id) {
                return base.load(id);
            }

            @Override
            public void delete(String id) {
                base.delete(id);
            }

            @Override
            public java.util.List<ExecutionSnapshot> listPendingRetries() {
                return base.listPendingRetries();
            }

            @Override
            public java.util.List<ExecutionSnapshot> listInterrupted(int limit, String afterExecutionId) {
                return base.listInterrupted(limit, afterExecutionId);
            }

            @Override
            public java.util.List<ExecutionSnapshot> listFailed(int limit, String afterExecutionId, int maxAttempts) {
                return base.listFailed(limit, afterExecutionId, maxAttempts);
            }
        };

        // B→C definition: B has its own sub-step (bStep), C has c1 and c2.
        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("bc-machine")
                .initial("B")
                .snapshotRepository(intercepting)
                .contextLoader(id -> new OrderContext(id))
                .state("B")
                .subStep("bStep", ctx -> {
                    bStepCount.incrementAndGet();
                    return ActionResult.success();
                })
                .on("GO").to("C").end()
                .and()
                .state("C")
                .subStep("c1", ctx -> {
                    c1Count.incrementAndGet();
                    return ActionResult.success();
                })
                .subStep("c2", ctx -> {
                    c2Count.incrementAndGet();
                    return ActionResult.success();
                })
                .on("FINISH").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        // --- Assertion 1: on-entry checkpoint ordering ---
        allSaves.clear();
        StateMachineInstance<OrderContext> instance =
                definition.newInstance(new OrderContext("bc-1"), "bc-1");
        // B has bStep → B gets an on-entry RUNNING save, then bStep RUNNING save, then WAITING save.
        // After trigger GO → C has c1,c2 → C gets on-entry RUNNING save (currentState=C, no C::*),
        // then c1 RUNNING save (C::c1 present), then c2 RUNNING save (C::c1+c2 present), then WAITING.
        allSaves.clear(); // reset before trigger so we only observe the C transition's saves
        instance.trigger("GO");

        // The first RUNNING save to state C must have no C sub-step results (the on-entry checkpoint).
        java.util.List<ExecutionSnapshot> cRunningSaves = new java.util.ArrayList<>();
        for (ExecutionSnapshot s : allSaves) {
            if (s.isRunning() && "C".equals(s.getCurrentStateName())) {
                cRunningSaves.add(s);
            }
        }
        // Expect: on-entry(C, {}), after-c1(C, {C::c1}), after-c2(C, {C::c1, C::c2})
        assertThat(cRunningSaves).as("should have 3 RUNNING@C saves: on-entry + c1 + c2").hasSize(3);
        ExecutionSnapshot onEntry = cRunningSaves.get(0);
        assertThat(onEntry.getCurrentStateName()).isEqualTo("C");
        assertThat(onEntry.getCompletedSubStepResults())
                .as("on-entry checkpoint must have no C sub-step results")
                .doesNotContainKey("C::c1")
                .doesNotContainKey("C::c2");
        assertThat(cRunningSaves.get(1).getCompletedSubStepResults())
                .as("after-c1 checkpoint must contain C::c1 but not C::c2")
                .containsKey("C::c1")
                .doesNotContainKey("C::c2");
        assertThat(cRunningSaves.get(2).getCompletedSubStepResults())
                .as("after-c2 checkpoint must contain both C::c1 and C::c2")
                .containsKey("C::c1")
                .containsKey("C::c2");

        // --- Assertion 2: recovery from the on-entry RUNNING@C snapshot ---
        // Simulate the crash by building the snapshot the new code produces on entry to C:
        // RUNNING, currentStateName=C, completedSubStepResults={} (no C sub-steps done yet).
        ExecutionSnapshot crashedAtCEntry = new ExecutionSnapshot.Builder()
                .executionId("bc-crashed")
                .machineDefinitionId("bc-machine")
                .currentStateName("C")
                .lastTriggerEvent("GO")
                .completedSubStepResults(java.util.Map.of())
                .status(SnapshotStatus.RUNNING)
                .capturedAt(java.time.Instant.now())
                .build();
        base.save("bc-crashed", crashedAtCEntry);

        // Reset counters — only the recovery run should increment them
        bStepCount.set(0);
        c1Count.set(0);
        c2Count.set(0);

        // Recover: resumeInterrupted positions the machine at C with no completed sub-steps,
        // then resume() runs c1 and c2.
        StateMachineInstance<OrderContext> recovered =
                definition.resumeInterrupted(new OrderContext("bc-crashed"), crashedAtCEntry, base);
        recovered.resume();

        // C's sub-steps must both run exactly once
        assertThat(c1Count.get()).as("c1 must run on recovery").isEqualTo(1);
        assertThat(c2Count.get()).as("c2 must run on recovery").isEqualTo(1);
        // B's sub-step must NOT have re-run
        assertThat(bStepCount.get()).as("bStep must NOT re-run on C-recovery").isEqualTo(0);
        // Machine is positioned at C (awaiting FINISH event)
        assertThat(recovered.currentState().name()).isEqualTo("C");
        assertThat(recovered.status()).isEqualTo(ExecutionStatus.RUNNING);
    }

    @Test
    void perSubStepCheckpoint_persistsProgressAfterEachStep() {
        // Verify that the repository receives a RUNNING checkpoint after each successful sub-step,
        // and a WAITING checkpoint at the end (at-rest, non-terminal state).
        AtomicInteger runningCount = new AtomicInteger(0);
        AtomicInteger waitingCount = new AtomicInteger(0);
        java.util.concurrent.CopyOnWriteArrayList<ExecutionSnapshot> runningSnapshots =
                new java.util.concurrent.CopyOnWriteArrayList<>();

        // Wrap in-memory repo to intercept saves
        InMemorySnapshotRepository base = InMemorySnapshotRepository.create();
        SnapshotRepository intercepting = new SnapshotRepository() {
            @Override
            public void save(String id, ExecutionSnapshot s) {
                if (s.isRunning()) {
                    runningCount.incrementAndGet();
                    runningSnapshots.add(s);
                } else if (s.isWaiting()) {
                    waitingCount.incrementAndGet();
                }
                base.save(id, s);
            }

            @Override
            public java.util.Optional<ExecutionSnapshot> load(String id) {
                return base.load(id);
            }

            @Override
            public void delete(String id) {
                base.delete(id);
            }

            @Override
            public java.util.List<ExecutionSnapshot> listPendingRetries() {
                return base.listPendingRetries();
            }

            @Override
            public java.util.List<ExecutionSnapshot> listInterrupted(int limit, String afterExecutionId) {
                return base.listInterrupted(limit, afterExecutionId);
            }

            @Override
            public java.util.List<ExecutionSnapshot> listFailed(int limit, String afterExecutionId, int maxAttempts) {
                return base.listFailed(limit, afterExecutionId, maxAttempts);
            }
        };

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("order-chk")
                .initial("PENDING")
                .snapshotRepository(intercepting)
                .contextLoader(id -> new OrderContext(id))
                .state("PENDING")
                .on("GO").to("MULTI").end()
                .and()
                .state("MULTI")
                .subStep("a", ctx -> ActionResult.success())
                .subStep("b", ctx -> ActionResult.success())
                .subStep("c", ctx -> ActionResult.success())
                .on("NEXT").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineInstance<OrderContext> instance =
                definition.newInstance(new OrderContext("chk-1"), "chk-1");
        // Initial state PENDING has no sub-steps → 1 WAITING save (at-rest in PENDING)
        assertThat(waitingCount.get()).isEqualTo(1);

        instance.trigger("GO");
        // After GO, one on-entry RUNNING checkpoint is written before the first sub-step,
        // then three per-sub-step RUNNING checkpoints (a, b, c) → 4 RUNNING saves total.
        // Then a final WAITING save (at-rest in MULTI, awaiting next event).
        assertThat(runningCount.get()).isEqualTo(4); // 1 on-entry + 3 per-sub-step
        assertThat(waitingCount.get()).isEqualTo(2); // PENDING + MULTI at-rest

        // Check progressive completedSubStepResults in RUNNING checkpoints
        assertThat(runningSnapshots).isNotEmpty();
        // After step 'a': snapshot should have {MULTI::a: success}
        boolean foundA = runningSnapshots.stream()
                .anyMatch(s -> s.getCompletedSubStepResults().containsKey("MULTI::a")
                        && !s.getCompletedSubStepResults().containsKey("MULTI::b"));
        assertThat(foundA).as("RUNNING checkpoint after step 'a' should exist").isTrue();

        boolean foundAB = runningSnapshots.stream()
                .anyMatch(s -> s.getCompletedSubStepResults().containsKey("MULTI::a")
                        && s.getCompletedSubStepResults().containsKey("MULTI::b")
                        && !s.getCompletedSubStepResults().containsKey("MULTI::c"));
        assertThat(foundAB).as("RUNNING checkpoint after step 'b' should exist").isTrue();
    }
}
