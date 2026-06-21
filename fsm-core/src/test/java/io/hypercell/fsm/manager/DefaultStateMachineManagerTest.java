package io.hypercell.fsm.manager;

import io.hypercell.fsm.OrderContext;
import io.hypercell.fsm.StateMachine;
import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.core.ExecutionStatus;
import io.hypercell.fsm.core.StateMachineDefinition;
import io.hypercell.fsm.exception.CompletedMachineException;
import io.hypercell.fsm.exception.IllegalTriggerStateException;
import io.hypercell.fsm.exception.StateMachineException;
import io.hypercell.fsm.resume.ExecutionSnapshot;
import io.hypercell.fsm.resume.SnapshotRepository;
import io.hypercell.fsm.resume.SnapshotStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultStateMachineManagerTest {

    private SnapshotRepository repo;
    private StateMachineDefinition<OrderContext> definition;
    private StateMachineManager<OrderContext> manager;

    @BeforeEach
    void setUp() {
        repo = StateMachine.inMemoryRepository();
        definition = StateMachine.<OrderContext>define("order")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderContext(id))
                .state("PENDING")
                .on("APPROVE").to("PROCESSING").end()
                .and()
                .state("PROCESSING")
                .subStep("reserve", ctx -> {
                    ctx.setReservationId("RSV-1");
                    return ActionResult.success();
                })
                .on("COMPLETE").to("SHIPPED").end()
                .and()
                .state("SHIPPED").terminal().and()
                .build();
        manager = StateMachine.manager(definition, repo);
    }


    @Test
    void initialize_createsExecutionInInitialState() {
        ManagedTransitionResult<OrderContext> result = manager.initialize("order-1");

        assertThat(result.getExecutionId()).isEqualTo("order-1");
        assertThat(result.getToState()).isEqualTo("PENDING");
        assertThat(result.getExecutionStatus()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(result.isRunning()).isTrue();
        assertThat(repo.load("order-1")).isPresent();
    }

    @Test
    void initialize_isIdempotent_secondCallReturnsCurrentState() {
        manager.initialize("order-1");
        manager.trigger("order-1", "APPROVE");
        ManagedTransitionResult<OrderContext> second = manager.initialize("order-1");

        assertThat(second.getToState()).isEqualTo("PROCESSING");
    }

    @Test
    void initialize_withContextOverride_usesProvidedContext() {
        OrderContext ctx = new OrderContext("order-override");
        ManagedTransitionResult<OrderContext> result = manager.initialize("order-override", ctx);
        assertThat(result.getExecutionStatus()).isEqualTo(ExecutionStatus.RUNNING);
    }

    @Test
    void initialize_throws_onCompletedExecution() {
        manager.initialize("order-done");
        manager.trigger("order-done", "APPROVE");
        manager.trigger("order-done", "COMPLETE");

        assertThatThrownBy(() -> manager.initialize("order-done"))
                .isInstanceOf(CompletedMachineException.class);
    }


    @Test
    void trigger_firstTime_noSnapshot_succeeds() {
        ManagedTransitionResult<OrderContext> result = manager.trigger("order-2", "APPROVE");

        assertThat(result.getFromState()).isEqualTo("PENDING");
        assertThat(result.getToState()).isEqualTo("PROCESSING");
        assertThat(result.getExecutionStatus()).isEqualTo(ExecutionStatus.RUNNING);
    }

    @Test
    void trigger_withExistingRunningSnapshot_reconstitutesAndTransitions() {
        manager.trigger("order-3", "APPROVE");
        ManagedTransitionResult<OrderContext> result = manager.trigger("order-3", "COMPLETE");

        assertThat(result.getToState()).isEqualTo("SHIPPED");
        assertThat(result.getExecutionStatus()).isEqualTo(ExecutionStatus.TERMINATED);
        assertThat(result.isTerminated()).isTrue();
    }

    @Test
    void trigger_onFailedSnapshot_throwsIllegalTriggerStateException() {
        AtomicBoolean fail = new AtomicBoolean(true);
        SnapshotRepository failRepo = StateMachine.inMemoryRepository();
        StateMachineDefinition<OrderContext> failDef = StateMachine.<OrderContext>define("order-fail")
                .initial("PENDING")
                .snapshotRepository(failRepo)
                .contextLoader(id -> new OrderContext(id))
                .state("PENDING")
                .subStep("flaky", ctx -> {
                    if (fail.get()) {
                        fail.set(false);
                        throw new RuntimeException("transient error");
                    }
                    return ActionResult.success();
                })
                .on("GO").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineManager<OrderContext> failManager = StateMachine.manager(failDef, failRepo);

        ManagedTransitionResult<OrderContext> r1 = failManager.trigger("exec-1", "GO");
        assertThat(r1.getExecutionStatus()).isEqualTo(ExecutionStatus.FAILED);
        assertThat(r1.getRootCause()).isNotNull().hasMessage("transient error");

        assertThatThrownBy(() -> failManager.trigger("exec-1", "GO"))
                .isInstanceOf(IllegalTriggerStateException.class)
                .hasMessageContaining("FAILED")
                .hasMessageContaining("proceed()");

        assertThat(failManager.eligibilityOf("exec-1")).isEqualTo(TriggerEligibility.NEEDS_PROCEED);

        ManagedTransitionResult<OrderContext> r2 = failManager.proceed("exec-1");
        assertThat(r2.getExecutionStatus()).isEqualTo(ExecutionStatus.RUNNING);

        assertThat(failManager.eligibilityOf("exec-1")).isEqualTo(TriggerEligibility.READY);

        ManagedTransitionResult<OrderContext> r3 = failManager.trigger("exec-1", "GO");
        assertThat(r3.getToState()).isEqualTo("DONE");
        assertThat(r3.getExecutionStatus()).isEqualTo(ExecutionStatus.TERMINATED);
    }

    @Test
    void trigger_onRunningInterruptedSnapshot_throwsIllegalTriggerStateException() {
        SnapshotRepository runningRepo = StateMachine.inMemoryRepository();
        StateMachineDefinition<OrderContext> def = StateMachine.<OrderContext>define("order-running")
                .initial("PENDING")
                .snapshotRepository(runningRepo)
                .contextLoader(id -> new OrderContext(id))
                .state("PENDING")
                .on("GO").to("WORKING").end()
                .and()
                .state("WORKING")
                .subStep("doWork", ctx -> ActionResult.success())
                .on("DONE").to("FINISHED").end()
                .and()
                .state("FINISHED").terminal().and()
                .build();

        StateMachineManager<OrderContext> m = StateMachine.manager(def, runningRepo);

        ExecutionSnapshot interrupted = new ExecutionSnapshot.Builder()
                .executionId("exec-run")
                .machineDefinitionId("order-running")
                .currentStateName("WORKING")
                .lastTriggerEvent("GO")
                .completedSubStepResults(java.util.Map.of())
                .status(SnapshotStatus.RUNNING)
                .capturedAt(java.time.Instant.now())
                .build();
        runningRepo.save("exec-run", interrupted);

        assertThatThrownBy(() -> m.trigger("exec-run", "DONE"))
                .isInstanceOf(IllegalTriggerStateException.class)
                .hasMessageContaining("RUNNING")
                .hasMessageContaining("resume()");

        assertThat(m.eligibilityOf("exec-run")).isEqualTo(TriggerEligibility.NEEDS_RESUME);

        ManagedTransitionResult<OrderContext> resumeResult = m.resume("exec-run");
        assertThat(resumeResult.getExecutionStatus()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(resumeResult.getToState()).isEqualTo("WORKING");

        assertThat(m.eligibilityOf("exec-run")).isEqualTo(TriggerEligibility.READY);

        ManagedTransitionResult<OrderContext> r = m.trigger("exec-run", "DONE");
        assertThat(r.getToState()).isEqualTo("FINISHED");
        assertThat(r.getExecutionStatus()).isEqualTo(ExecutionStatus.TERMINATED);
    }

    @Test
    void eligibilityOf_returnsReadyForAbsentAndWaiting() {
        assertThat(manager.eligibilityOf("never-seen")).isEqualTo(TriggerEligibility.READY);

        manager.trigger("order-elig", "APPROVE");
        assertThat(manager.eligibilityOf("order-elig")).isEqualTo(TriggerEligibility.READY);
    }

    @Test
    void eligibilityOf_returnsTerminatedAfterCompletion() {
        manager.trigger("order-term", "APPROVE");
        manager.trigger("order-term", "COMPLETE");
        assertThat(manager.eligibilityOf("order-term")).isEqualTo(TriggerEligibility.TERMINATED);
    }

    @Test
    void currentState_returnsStateName_andEmptyForAbsent() {
        assertThat(manager.currentState("never-seen")).isEmpty();

        manager.trigger("order-cs", "APPROVE");
        assertThat(manager.currentState("order-cs")).hasValue("PROCESSING");

        manager.trigger("order-cs", "COMPLETE");
        assertThat(manager.currentState("order-cs")).hasValue("SHIPPED");
    }

    @Test
    void trigger_throws_onCompletedExecution() {
        manager.trigger("order-4", "APPROVE");
        manager.trigger("order-4", "COMPLETE");

        assertThatThrownBy(() -> manager.trigger("order-4", "COMPLETE"))
                .isInstanceOf(CompletedMachineException.class);
    }

    @Test
    void trigger_failedSubStep_returnsFailedResultWithRootCause() {
        SnapshotRepository failRepo = StateMachine.inMemoryRepository();
        StateMachineDefinition<OrderContext> failDef = StateMachine.<OrderContext>define("order-fail2")
                .initial("PENDING")
                .snapshotRepository(failRepo)
                .contextLoader(id -> new OrderContext(id))
                .state("PENDING")
                .subStep("bad-step", ctx -> {
                    throw new IllegalStateException("invalid state");
                })
                .on("GO").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineManager<OrderContext> failManager = StateMachine.manager(failDef, failRepo);
        ManagedTransitionResult<OrderContext> result = failManager.trigger("exec-fail", "GO");

        assertThat(result.isFailed()).isTrue();
        assertThat(result.getFailedSubStepName()).isEqualTo("bad-step");
        assertThat(result.getRootCause()).isNotNull()
                .isInstanceOf(RuntimeException.class)
                .hasMessage("invalid state");
    }


    @Test
    void proceed_fromFailedState_retries() {
        AtomicInteger attempts = new AtomicInteger(0);
        SnapshotRepository proceedRepo = StateMachine.inMemoryRepository();
        StateMachineDefinition<OrderContext> proceedDef = StateMachine.<OrderContext>define("order-proceed")
                .initial("PENDING")
                .snapshotRepository(proceedRepo)
                .contextLoader(id -> new OrderContext(id))
                .state("PENDING")
                .subStep("flaky-step", ctx -> {
                    if (attempts.incrementAndGet() < 2) {
                        throw new RuntimeException("first fail");
                    }
                    return ActionResult.success();
                })
                .on("GO").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineManager<OrderContext> proceedManager = StateMachine.manager(proceedDef, proceedRepo);
        proceedManager.trigger("exec-p", "GO");

        ManagedTransitionResult<OrderContext> result = proceedManager.proceed("exec-p");
        assertThat(result.getExecutionStatus()).isEqualTo(ExecutionStatus.RUNNING);
    }

    @Test
    void isInitialState_delegatesToDefinition() {
        assertThat(manager.isInitialState("PENDING")).isTrue();
        assertThat(manager.isInitialState("PROCESSING")).isFalse();
    }

    @Test
    void isTerminal_delegatesToDefinition() {
        assertThat(manager.isTerminal("SHIPPED")).isTrue();
        assertThat(manager.isTerminal("PENDING")).isFalse();
    }

    @Test
    void resolveContext_checkedExceptionFromLoader_wrappedInStateMachineException() {
        StateMachineDefinition<OrderContext> checkDef = StateMachine.<OrderContext>define("order-check")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> {
                    throw new java.io.IOException("db down");
                })
                .state("PENDING").on("GO").to("DONE").end().and()
                .state("DONE").terminal().and()
                .build();

        StateMachineManager<OrderContext> checkManager = StateMachine.manager(checkDef, repo);
        assertThatThrownBy(() -> checkManager.trigger("exec-c", "GO"))
                .isInstanceOf(StateMachineException.class)
                .hasMessageContaining("exec-c")
                .hasCauseInstanceOf(java.io.IOException.class);
    }

    @Test
    void withContextLoader_createsManagerWithOverride() {
        StateMachineManager<OrderContext> overrideManager = manager.withContextLoader(
                id -> new OrderContext("override-" + id));
        ManagedTransitionResult<OrderContext> result = overrideManager.trigger("exec-ov", "APPROVE");
        assertThat(result.getExecutionStatus()).isEqualTo(ExecutionStatus.RUNNING);
    }

    @Test
    void snapshotOf_returnsCurrentSnapshot() {
        manager.trigger("order-snap", "APPROVE");
        assertThat(manager.snapshotOf("order-snap")).isPresent()
                .hasValueSatisfying(s -> assertThat(s.getCurrentStateName()).isEqualTo("PROCESSING"));
    }

    @Test
    void snapshotOf_returnsEmpty_whenNoExecution() {
        assertThat(manager.snapshotOf("nonexistent")).isEmpty();
    }
}