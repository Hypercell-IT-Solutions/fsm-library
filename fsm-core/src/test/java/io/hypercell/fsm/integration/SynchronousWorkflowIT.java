package io.hypercell.fsm.integration;

import io.hypercell.fsm.OrderContext;
import io.hypercell.fsm.StateMachine;
import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.core.ExecutionStatus;
import io.hypercell.fsm.core.StateMachineDefinition;
import io.hypercell.fsm.core.StateMachineInstance;
import io.hypercell.fsm.exception.SubStepExecutionException;
import io.hypercell.fsm.resume.SnapshotRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SynchronousWorkflowIT {

    private SnapshotRepository repo;
    private StateMachineDefinition<OrderContext> definition;
    private List<String> executedSteps;

    @BeforeEach
    void setUp() {
        repo = StateMachine.inMemoryRepository();
        executedSteps = new ArrayList<>();

        definition = StateMachine.<OrderContext>define("order-workflow")
                .initial("PENDING")
                .snapshotRepository(repo)
                .state("PENDING")
                .on("APPROVE").to("PROCESSING").end()
                .on("CANCEL").to("CANCELLED").end()
                .and()
                .state("PROCESSING")
                .subStep("reserve", ctx -> {
                    executedSteps.add("reserve");
                    ctx.setReservationId("RSV-1");
                    return ActionResult.success();
                })
                .subStep("charge", ctx -> {
                    executedSteps.add("charge");
                    ctx.setPaymentCharged(true);
                    return ActionResult.success();
                })
                .subStep("notify", ctx -> {
                    executedSteps.add("notify");
                    ctx.setConfirmationSent(true);
                    return ActionResult.success();
                })
                .on("COMPLETE").to("SHIPPED").end()
                .and()
                .state("SHIPPED").terminal().and()
                .state("CANCELLED").terminal().and()
                .build();
    }

    @Test
    void happyPath_PENDING_to_SHIPPED() {
        StateMachineInstance<OrderContext> instance = definition.newInstance(new OrderContext("o1"), "o1");
        assertThat(instance.currentState().name()).isEqualTo("PENDING");
        assertThat(instance.isInInitialState()).isTrue();

        instance.trigger("APPROVE");
        assertThat(instance.currentState().name()).isEqualTo("PROCESSING");
        assertThat(executedSteps).containsExactly("reserve", "charge", "notify");
        assertThat(instance.context().getReservationId()).isEqualTo("RSV-1");
        assertThat(instance.context().isPaymentCharged()).isTrue();
        assertThat(instance.context().isConfirmationSent()).isTrue();

        instance.trigger("COMPLETE");
        assertThat(instance.currentState().name()).isEqualTo("SHIPPED");
        assertThat(instance.status()).isEqualTo(ExecutionStatus.TERMINATED);
        assertThat(instance.isInTerminalState()).isTrue();
    }

    @Test
    void cancellationPath_PENDING_to_CANCELLED() {
        StateMachineInstance<OrderContext> instance = definition.newInstance(new OrderContext("o1"), "o1");
        instance.trigger("CANCEL");
        assertThat(instance.currentState().name()).isEqualTo("CANCELLED");
        assertThat(instance.status()).isEqualTo(ExecutionStatus.TERMINATED);
    }

    @Test
    void subStepFailure_snapshotsAndThrows() {
        StateMachineDefinition<OrderContext> failDef = StateMachine.<OrderContext>define("order-fail")
                .initial("PENDING")
                .snapshotRepository(repo)
                .state("PENDING")
                .subStep("step-ok", ctx -> {
                    executedSteps.add("step-ok");
                    return ActionResult.success();
                })
                .subStep("step-fail", ctx -> {
                    executedSteps.add("step-fail");
                    throw new RuntimeException("external service down");
                })
                .on("GO").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        assertThatThrownBy(() -> failDef.newInstance(new OrderContext("o1"), "fail-exec"))
                .isInstanceOf(SubStepExecutionException.class)
                .satisfies(e -> {
                    SubStepExecutionException ex = (SubStepExecutionException) e;
                    assertThat(ex.getSubStepName()).isEqualTo("step-fail");
                });

        assertThat(executedSteps).containsExactly("step-ok", "step-fail");
        assertThat(repo.load("fail-exec")).isPresent()
                .hasValueSatisfying(s -> {
                    assertThat(s.isFailed()).isTrue();
                    assertThat(s.getFailedSubStepName()).isEqualTo("step-fail");
                    assertThat(s.getCompletedSubStepResults()).containsKey("PENDING::step-ok");
                });
    }

    @Test
    void proceed_skipsCompletedSteps() {
        List<String> executedOnResume = new ArrayList<>();
        // Tracks whether step-fail has already been attempted once (survives the executedOnResume.clear() below).
        AtomicBoolean stepFailAttempted = new AtomicBoolean(false);
        StateMachineDefinition<OrderContext> resumeDef = StateMachine.<OrderContext>define("order-resume")
                .initial("PENDING")
                .snapshotRepository(repo)
                .state("PENDING")
                .subStep("step-ok", ctx -> {
                    executedOnResume.add("step-ok");
                    return ActionResult.success();
                })
                .subStep("step-fail", ctx -> {
                    executedOnResume.add("step-fail");
                    if (stepFailAttempted.getAndSet(true)) return ActionResult.success();
                    throw new RuntimeException("first failure");
                })
                .on("GO").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineInstance<OrderContext> instance;
        try {
            instance = resumeDef.newInstance(new OrderContext("o1"), "resume-exec");
        } catch (SubStepExecutionException e) {
            instance = resumeDef.resume(new OrderContext("o1"), repo.load("resume-exec").orElseThrow());
        }

        executedOnResume.clear();

        instance.proceed();

        assertThat(executedOnResume).doesNotContain("step-ok");
        assertThat(executedOnResume).contains("step-fail");
        assertThat(instance.status()).isEqualTo(ExecutionStatus.RUNNING);
    }
}