package io.hypercell.fsm.integration;

import io.hypercell.fsm.OrderContext;
import io.hypercell.fsm.StateMachine;
import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.core.ExecutionStatus;
import io.hypercell.fsm.core.StateMachineDefinition;
import io.hypercell.fsm.manager.ManagedTransitionResult;
import io.hypercell.fsm.manager.StateMachineManager;
import io.hypercell.fsm.resume.SnapshotRepository;
import io.hypercell.fsm.resume.SnapshotStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class ManagerLifecycleIT {

    private SnapshotRepository repo;
    private final Map<String, OrderContext> store = new HashMap<>();
    private StateMachineDefinition<OrderContext> definition;
    private StateMachineManager<OrderContext> manager;

    @BeforeEach
    void setUp() {
        repo = StateMachine.inMemoryRepository();

        definition = StateMachine.<OrderContext>define("order")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> {
                    OrderContext ctx = store.getOrDefault(id, new OrderContext(id));
                    return ctx;
                })
                .state("PENDING")
                .on("APPROVE").to("PROCESSING").end()
                .and()
                .state("PROCESSING")
                .subStep("reserve", ctx -> {
                    ctx.setReservationId("RSV-" + ctx.getOrderId());
                    store.put(ctx.getOrderId(), ctx);
                    return ActionResult.success();
                })
                .subStep("charge", ctx -> {
                    ctx.setPaymentCharged(true);
                    store.put(ctx.getOrderId(), ctx);
                    return ActionResult.success();
                })
                .on("COMPLETE").to("SHIPPED").end()
                .and()
                .state("SHIPPED").terminal().and()
                .build();

        manager = StateMachine.manager(definition, repo);
    }

    @Test
    void fullLifecycle_initialize_trigger_complete() {
        ManagedTransitionResult<OrderContext> initResult = manager.initialize("order-1");
        assertThat(initResult.getToState()).isEqualTo("PENDING");
        assertThat(initResult.getExecutionStatus()).isEqualTo(ExecutionStatus.RUNNING);

        ManagedTransitionResult<OrderContext> approveResult = manager.trigger("order-1", "APPROVE");
        assertThat(approveResult.getToState()).isEqualTo("PROCESSING");
        assertThat(approveResult.getExecutionStatus()).isEqualTo(ExecutionStatus.RUNNING);

        ManagedTransitionResult<OrderContext> completeResult = manager.trigger("order-1", "COMPLETE");
        assertThat(completeResult.getToState()).isEqualTo("SHIPPED");
        assertThat(completeResult.getExecutionStatus()).isEqualTo(ExecutionStatus.TERMINATED);
        assertThat(completeResult.isTerminated()).isTrue();

        assertThat(repo.load("order-1")).isPresent()
                .hasValueSatisfying(s -> assertThat(s.getStatus()).isEqualTo(SnapshotStatus.TERMINATED));
    }

    @Test
    void failureRecovery_manualProceed_thenComplete() {
        AtomicBoolean shouldFail = new AtomicBoolean(true);

        SnapshotRepository failRepo = StateMachine.inMemoryRepository();
        StateMachineDefinition<OrderContext> failDef = StateMachine.<OrderContext>define("order-recover")
                .initial("PENDING")
                .snapshotRepository(failRepo)
                .contextLoader(id -> new OrderContext(id))
                .state("PENDING")
                .subStep("flaky", ctx -> {
                    if (shouldFail.getAndSet(false)) {
                        throw new RuntimeException("first failure");
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
        assertThat(r1.getFailedSubStepName()).isEqualTo("flaky");

        ManagedTransitionResult<OrderContext> r2 = failManager.proceed("exec-1");
        assertThat(r2.getExecutionStatus()).isEqualTo(ExecutionStatus.RUNNING);

        ManagedTransitionResult<OrderContext> r3 = failManager.trigger("exec-1", "GO");
        assertThat(r3.getToState()).isEqualTo("DONE");
        assertThat(r3.getExecutionStatus()).isEqualTo(ExecutionStatus.TERMINATED);
    }

    @Test
    void snapshotOf_returnsCorrectState_afterEachTransition() {
        manager.initialize("order-2");
        assertThat(manager.snapshotOf("order-2"))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getCurrentStateName()).isEqualTo("PENDING"));

        manager.trigger("order-2", "APPROVE");
        assertThat(manager.snapshotOf("order-2"))
                .isPresent()
                .hasValueSatisfying(s -> assertThat(s.getCurrentStateName()).isEqualTo("PROCESSING"));
    }

    @Test
    void stateValidation_delegatesCorrectly() {
        assertThat(manager.isInitialState("PENDING")).isTrue();
        assertThat(manager.isInitialState("PROCESSING")).isFalse();
        assertThat(manager.isTerminal("SHIPPED")).isTrue();
        assertThat(manager.isTerminal("PENDING")).isFalse();
    }
}