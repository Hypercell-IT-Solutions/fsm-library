package io.hypercell.fsm.execution;

import io.hypercell.fsm.OrderContext;
import io.hypercell.fsm.StateMachine;
import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.core.ExecutionStatus;
import io.hypercell.fsm.core.StateMachineDefinition;
import io.hypercell.fsm.core.StateMachineInstance;
import io.hypercell.fsm.exception.InvalidEventException;
import io.hypercell.fsm.exception.SubStepExecutionException;
import io.hypercell.fsm.resume.SnapshotRepository;
import io.hypercell.fsm.resume.SnapshotStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultStateMachineInstanceTest {

    private SnapshotRepository repo;
    private StateMachineDefinition<OrderContext> definition;

    @BeforeEach
    void setUp() {
        repo = StateMachine.inMemoryRepository();
        definition = StateMachine.<OrderContext>define("order")
                .initial("PENDING")
                .snapshotRepository(repo)
                .state("PENDING")
                .on("APPROVE").to("PROCESSING").end()
                .on("CANCEL").to("CANCELLED").end()
                .and()
                .state("PROCESSING")
                .subStep("reserve", ctx -> {
                    ctx.setReservationId("RSV-1");
                    return ActionResult.success();
                })
                .subStep("charge", ctx -> {
                    ctx.setPaymentCharged(true);
                    return ActionResult.success();
                })
                .on("COMPLETE").to("SHIPPED").end()
                .and()
                .state("SHIPPED").terminal().and()
                .state("CANCELLED").terminal().and()
                .build();
    }

    @Test
    void newInstance_startsInInitialStateRunning() {
        StateMachineInstance<OrderContext> instance = definition.newInstance(new OrderContext("o1"), "o1");
        assertThat(instance.currentState().name()).isEqualTo("PENDING");
        assertThat(instance.status()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(instance.isRunning()).isTrue();
        assertThat(instance.isInInitialState()).isTrue();
        assertThat(instance.isInTerminalState()).isFalse();
    }

    @Test
    void trigger_movesToNextState() {
        StateMachineInstance<OrderContext> instance = definition.newInstance(new OrderContext("o1"), "o1");
        instance.trigger("APPROVE");
        assertThat(instance.currentState().name()).isEqualTo("PROCESSING");
        assertThat(instance.isInInitialState()).isFalse();
    }

    @Test
    void trigger_executesSubSteps() {
        StateMachineInstance<OrderContext> instance = definition.newInstance(new OrderContext("o1"), "o1");
        instance.trigger("APPROVE");
        assertThat(instance.context().getReservationId()).isEqualTo("RSV-1");
        assertThat(instance.context().isPaymentCharged()).isTrue();
    }

    @Test
    void trigger_toTerminalState_completesInstance() {
        StateMachineInstance<OrderContext> instance = definition.newInstance(new OrderContext("o1"), "o1");
        instance.trigger("APPROVE");
        instance.trigger("COMPLETE");
        assertThat(instance.status()).isEqualTo(ExecutionStatus.TERMINATED);
        assertThat(instance.isTerminated()).isTrue();
        assertThat(instance.isInTerminalState()).isTrue();
    }

    @Test
    void trigger_unknownEvent_throwsInvalidEventException() {
        StateMachineInstance<OrderContext> instance = definition.newInstance(new OrderContext("o1"), "o1");
        assertThatThrownBy(() -> instance.trigger("NONEXISTENT"))
                .isInstanceOf(InvalidEventException.class);
    }

    @Test
    void trigger_onFailedStatus_throws() {
        StateMachineDefinition<OrderContext> failDef = StateMachine.<OrderContext>define("order-fail")
                .initial("PENDING")
                .snapshotRepository(repo)
                .state("PENDING")
                .subStep("failStep", ctx -> {
                    throw new RuntimeException("boom");
                })
                .on("GO").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        StateMachineInstance<OrderContext> instance;
        try {
            instance = failDef.newInstance(new OrderContext("o1"), "o1");
        } catch (SubStepExecutionException e) {
            instance = failDef.resume(new OrderContext("o1"),
                    repo.load("o1").orElseThrow());
        }
        final StateMachineInstance<OrderContext> failed = instance;
        assertThat(failed.isFailed()).isTrue();
        assertThatThrownBy(() -> failed.trigger("GO"))
                .isInstanceOf(InvalidEventException.class);
    }

    @Test
    void subStepFailure_savesSnapshotAndThrows() {
        StateMachineDefinition<OrderContext> failDef = StateMachine.<OrderContext>define("order-fail2")
                .initial("PENDING")
                .snapshotRepository(repo)
                .state("PENDING")
                .subStep("fail-step", ctx -> ActionResult.failed("intentional"))
                .on("GO").to("DONE").end()
                .and()
                .state("DONE").terminal().and()
                .build();

        assertThatThrownBy(() -> failDef.newInstance(new OrderContext("o1"), "exec-fail"))
                .isInstanceOf(SubStepExecutionException.class)
                .satisfies(ex -> {
                    SubStepExecutionException e = (SubStepExecutionException) ex;
                    assertThat(e.getSubStepName()).isEqualTo("fail-step");
                });
        assertThat(repo.load("exec-fail")).isPresent()
                .hasValueSatisfying(s -> assertThat(s.isFailed()).isTrue());
    }

    @Test
    void guard_selectsCorrectTransition() {
        StateMachineDefinition<OrderContext> guardDef = StateMachine.<OrderContext>define("guard-order")
                .initial("PENDING")
                .state("PENDING")
                .on("GO").when(ctx -> ctx.getReservationId() != null).to("VIP").end()
                .on("GO").to("NORMAL").end()
                .and()
                .state("VIP").terminal().and()
                .state("NORMAL").terminal().and()
                .build();

        OrderContext ctxWithReservation = new OrderContext("o1");
        ctxWithReservation.setReservationId("RSV-1");
        StateMachineInstance<OrderContext> instance = guardDef.newInstance(ctxWithReservation, "o1");
        instance.trigger("GO");
        assertThat(instance.currentState().name()).isEqualTo("VIP");
    }

    @Test
    void checkpointSaved_afterSuccessfulTrigger() {
        StateMachineInstance<OrderContext> instance = definition.newInstance(new OrderContext("o1"), "o1-chk");
        instance.trigger("APPROVE");
        assertThat(repo.load("o1-chk")).isPresent()
                .hasValueSatisfying(s -> {
                    assertThat(s.getCurrentStateName()).isEqualTo("PROCESSING");
                    assertThat(s.getStatus()).isEqualTo(SnapshotStatus.WAITING);
                });
    }

    @Test
    void onEntryOnExit_executedInCorrectOrder() {
        List<String> events = new ArrayList<>();
        StateMachineDefinition<OrderContext> hookDef = StateMachine.<OrderContext>define("hook-order")
                .initial("A")
                .state("A")
                .onEntry(ctx -> events.add("entry-A"))
                .onExit(ctx -> events.add("exit-A"))
                .on("GO").to("B").end()
                .and()
                .state("B")
                .onEntry(ctx -> events.add("entry-B"))
                .terminal()
                .and()
                .build();

        StateMachineInstance<OrderContext> instance = hookDef.newInstance(new OrderContext("o1"), "o1");
        assertThat(events).containsExactly("entry-A");
        instance.trigger("GO");
        assertThat(events).containsExactly("entry-A", "exit-A", "entry-B");
    }
}