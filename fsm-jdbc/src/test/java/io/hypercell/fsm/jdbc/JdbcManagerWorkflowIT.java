package io.hypercell.fsm.jdbc;

import io.hypercell.fsm.StateMachine;
import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.core.ExecutionStatus;
import io.hypercell.fsm.core.StateMachineDefinition;
import io.hypercell.fsm.exception.IllegalTriggerStateException;
import io.hypercell.fsm.manager.ManagedTransitionResult;
import io.hypercell.fsm.manager.StateMachineManager;
import io.hypercell.fsm.manager.TriggerEligibility;
import io.hypercell.fsm.resume.SnapshotStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that JdbcSnapshotRepository works end-to-end with the FSM manager over H2.
 */
class JdbcManagerWorkflowIT {

    /**
     * Simulates cross-process persistence by sharing the same DataSource across two managers.
     */
    private DataSource sharedDataSource;

    private final Map<String, OrderCtx> store = new HashMap<>();

    record OrderCtx(String orderId, String reservationId, boolean paid) {
    }

    @BeforeEach
    void setUp() {
        sharedDataSource = H2DataSourceHelper.newInMemoryDataSource();
    }

    private StateMachineDefinition<OrderCtx> buildDefinition(JdbcSnapshotRepository repo) {
        return StateMachine.<OrderCtx>define("order")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> store.getOrDefault(id, new OrderCtx(id, null, false)))
                .state("PENDING")
                .on("APPROVE").to("PROCESSING").end()
                .and()
                .state("PROCESSING")
                .subStep("reserve", ctx -> {
                    OrderCtx updated = new OrderCtx(ctx.orderId(), "RSV-" + ctx.orderId(), false);
                    store.put(ctx.orderId(), updated);
                    return ActionResult.success();
                })
                .subStep("charge", ctx -> {
                    OrderCtx current = store.getOrDefault(ctx.orderId(), ctx);
                    store.put(ctx.orderId(), new OrderCtx(current.orderId(), current.reservationId(), true));
                    return ActionResult.success();
                })
                .on("COMPLETE").to("SHIPPED").end()
                .and()
                .state("SHIPPED").terminal().and()
                .build();
    }

    @Test
    void fullWorkflow_persistsAcrossTwoManagerInstances() {
        JdbcSnapshotRepository repo1 = new JdbcSnapshotRepository(sharedDataSource, new TestH2Dialect());
        StateMachineManager<OrderCtx> manager1 = StateMachine.manager(buildDefinition(repo1), repo1);

        manager1.initialize("order-1");
        manager1.trigger("order-1", "APPROVE");

        assertThat(repo1.load("order-1")).isPresent()
                .hasValueSatisfying(s -> assertThat(s.getCurrentStateName()).isEqualTo("PROCESSING"));

        JdbcSnapshotRepository repo2 = new JdbcSnapshotRepository(sharedDataSource, new TestH2Dialect());
        StateMachineManager<OrderCtx> manager2 = StateMachine.manager(buildDefinition(repo2), repo2);

        ManagedTransitionResult<OrderCtx> result = manager2.trigger("order-1", "COMPLETE");
        assertThat(result.getToState()).isEqualTo("SHIPPED");
        assertThat(result.getExecutionStatus()).isEqualTo(ExecutionStatus.TERMINATED);

        assertThat(repo2.load("order-1")).isPresent()
                .hasValueSatisfying(s -> assertThat(s.getStatus()).isEqualTo(SnapshotStatus.TERMINATED));
    }

    @Test
    void failureRecovery_withJdbc_strictTrigger_requiresExplicitProceed() {
        AtomicBoolean shouldFail = new AtomicBoolean(true);

        JdbcSnapshotRepository repo = new JdbcSnapshotRepository(sharedDataSource, new TestH2Dialect());
        StateMachineDefinition<OrderCtx> failDef = StateMachine.<OrderCtx>define("order-fail")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(id -> new OrderCtx(id, null, false))
                .state("PENDING")
                .subStep("flaky", ctx -> {
                    if (shouldFail.getAndSet(false)) {
                        throw new RuntimeException("transient");
                    }
                    return ActionResult.success();
                })
                .on("GO").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineManager<OrderCtx> manager = StateMachine.manager(failDef, repo);

        ManagedTransitionResult<OrderCtx> r1 = manager.trigger("exec-f", "GO");
        assertThat(r1.getExecutionStatus()).isEqualTo(ExecutionStatus.FAILED);
        assertThat(repo.load("exec-f")).isPresent()
                .hasValueSatisfying(s -> assertThat(s.getStatus()).isEqualTo(SnapshotStatus.FAILED));

        assertThatThrownBy(() -> manager.trigger("exec-f", "GO"))
                .isInstanceOf(IllegalTriggerStateException.class)
                .hasMessageContaining("FAILED")
                .hasMessageContaining("proceed()");

        assertThat(manager.eligibilityOf("exec-f")).isEqualTo(TriggerEligibility.NEEDS_PROCEED);

        // Explicit recover-then-trigger: proceed() retries failed sub-step
        ManagedTransitionResult<OrderCtx> r2 = manager.proceed("exec-f");
        assertThat(r2.getExecutionStatus()).isEqualTo(ExecutionStatus.RUNNING);

        // Now WAITING → READY
        assertThat(manager.eligibilityOf("exec-f")).isEqualTo(TriggerEligibility.READY);

        // trigger() can now apply the next event
        ManagedTransitionResult<OrderCtx> r3 = manager.trigger("exec-f", "GO");
        assertThat(r3.getToState()).isEqualTo("DONE");
        assertThat(r3.getExecutionStatus()).isEqualTo(ExecutionStatus.TERMINATED);
    }
}