package io.hypercell.fsm.builder;

import io.hypercell.fsm.OrderContext;
import io.hypercell.fsm.StateMachine;
import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.core.StateMachineDefinition;
import io.hypercell.fsm.core.SubStepHandler;
import io.hypercell.fsm.exception.StateMachineConfigurationException;
import io.hypercell.fsm.failure.FailureDisposition;
import io.hypercell.fsm.failure.FailurePolicy;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StateMachineBuilderTest {

    @Test
    void build_succeeds_withMinimalDefinition() {
        StateMachineDefinition<OrderContext> def = StateMachine.<OrderContext>define("order")
                .initial("PENDING")
                .state("PENDING").terminal().and()
                .build();
        assertThat(def.id()).isEqualTo("order");
        assertThat(def.initialState().name()).isEqualTo("PENDING");
    }

    @Test
    void build_throws_whenNoStatesDefined() {
        assertThatThrownBy(() ->
                StateMachine.<OrderContext>define("order").initial("PENDING").build()
        ).isInstanceOf(StateMachineConfigurationException.class)
                .hasMessageContaining("No states defined");
    }

    @Test
    void build_throws_whenNoInitialState() {
        assertThatThrownBy(() ->
                StateMachine.<OrderContext>define("order")
                        .state("PENDING").terminal().and()
                        .build()
        ).isInstanceOf(StateMachineConfigurationException.class)
                .hasMessageContaining("No initial state");
    }

    @Test
    void build_throws_whenInitialStateUndefined() {
        assertThatThrownBy(() ->
                StateMachine.<OrderContext>define("order")
                        .initial("MISSING")
                        .state("PENDING").terminal().and()
                        .build()
        ).isInstanceOf(StateMachineConfigurationException.class)
                .hasMessageContaining("MISSING");
    }

    @Test
    void build_throws_forDuplicateStateName() {
        assertThatThrownBy(() ->
                StateMachine.<OrderContext>define("order")
                        .initial("PENDING")
                        .state("PENDING").terminal().and()
                        .state("PENDING").terminal().and()
                        .build()
        ).isInstanceOf(StateMachineConfigurationException.class)
                .hasMessageContaining("Duplicate state name");
    }

    @Test
    void build_throws_whenTerminalStateHasTransitions() {
        assertThatThrownBy(() ->
                StateMachine.<OrderContext>define("order")
                        .initial("DONE")
                        .state("DONE").terminal()
                        .on("EVENT").to("DONE").end()
                        .and()
                        .build()
        ).isInstanceOf(StateMachineConfigurationException.class);
    }

    @Test
    void build_throws_whenTransitionTargetsUnknownState() {
        assertThatThrownBy(() ->
                StateMachine.<OrderContext>define("order")
                        .initial("PENDING")
                        .state("PENDING")
                        .on("APPROVE").to("NONEXISTENT").end()
                        .and()
                        .build()
        ).isInstanceOf(StateMachineConfigurationException.class)
                .hasMessageContaining("NONEXISTENT");
    }

    @Test
    void build_throws_whenStateNameContainsReservedSeparator() {
        assertThatThrownBy(() ->
                StateMachine.<OrderContext>define("order")
                        .initial("PEND::ING")
                        .state("PEND::ING").terminal().and()
                        .build()
        ).isInstanceOf(StateMachineConfigurationException.class)
                .hasMessageContaining("::");
    }

    @Test
    void build_throws_retryPolicyWithoutRepository() {
        assertThatThrownBy(() ->
                StateMachine.<OrderContext>define("order")
                        .initial("PENDING")
                        .retryPolicy(StateMachine.fixedDelay(3, java.time.Duration.ofSeconds(1)))
                        .contextLoader(id -> new OrderContext(id))
                        .state("PENDING").terminal().and()
                        .build()
        ).isInstanceOf(StateMachineConfigurationException.class)
                .hasMessageContaining("snapshotRepository");
    }

    @Test
    void build_throws_retryPolicyWithoutContextLoader() {
        assertThatThrownBy(() ->
                StateMachine.<OrderContext>define("order")
                        .initial("PENDING")
                        .snapshotRepository(StateMachine.inMemoryRepository())
                        .retryPolicy(StateMachine.fixedDelay(3, java.time.Duration.ofSeconds(1)))
                        .state("PENDING").terminal().and()
                        .build()
        ).isInstanceOf(StateMachineConfigurationException.class)
                .hasMessageContaining("contextLoader");
    }

    @Test
    void contextLoader_storedOnDefinition() {
        StateMachineDefinition<OrderContext> def = StateMachine.<OrderContext>define("order")
                .initial("PENDING")
                .contextLoader(id -> new OrderContext(id))
                .state("PENDING").terminal().and()
                .build();
        assertThat(def.contextLoader()).isNotNull();
    }

    @Test
    void multipleTransitionsPerEvent_registeredInOrder() {
        StateMachineDefinition<OrderContext> def = StateMachine.<OrderContext>define("order")
                .initial("PENDING")
                .state("PENDING")
                .on("APPROVE").when(ctx -> ctx.getReservationId() != null).to("VIP").end()
                .on("APPROVE").to("NORMAL").end()
                .and()
                .state("VIP").terminal().and()
                .state("NORMAL").terminal().and()
                .build();
        assertThat(def.transitionsFrom("PENDING")).hasSize(2);
        assertThat(def.transitionsFrom("PENDING").get(0).targetState()).isEqualTo("VIP");
    }

    @Test
    void isInitialState_and_isTerminal_correctAfterBuild() {
        StateMachineDefinition<OrderContext> def = StateMachine.<OrderContext>define("order")
                .initial("PENDING")
                .state("PENDING").on("GO").to("DONE").end().and()
                .state("DONE").terminal().and()
                .build();
        assertThat(def.isInitialState("PENDING")).isTrue();
        assertThat(def.isInitialState("DONE")).isFalse();
        assertThat(def.isTerminal("DONE")).isTrue();
        assertThat(def.isTerminal("PENDING")).isFalse();
    }

    // ------------------------------------------------------- subStep(SubStepHandler[, policy])

    /** Minimal handler; these tests only care about registration, never execution. */
    private static SubStepHandler<OrderContext> handler(String name,
                                                        FailurePolicy<OrderContext> declared) {
        return new SubStepHandler<>() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public ActionResult execute(OrderContext ctx) {
                return ActionResult.success();
            }

            @Override
            public FailurePolicy<OrderContext> failurePolicy() {
                return declared;
            }
        };
    }

    @Test
    void subStepHandlerOverload_attachesTheGivenPolicyInsteadOfTheHandlers() {
        FailurePolicy<OrderContext> declared = FailurePolicy.always(FailureDisposition.MANUAL);
        FailurePolicy<OrderContext> override = FailurePolicy.always(FailureDisposition.ABORT);

        StateMachineDefinition<OrderContext> def = StateMachine.<OrderContext>define("order")
                .initial("PENDING")
                .state("PENDING")
                .subStep(handler("declared", declared))
                .subStep(handler("overridden", declared), override)
                .terminal().and()
                .build();

        var subSteps = def.stateByName("PENDING").subSteps();
        assertThat(subSteps.get(0).failurePolicy()).isSameAs(declared);
        assertThat(subSteps.get(1).failurePolicy()).isSameAs(override);
    }

    @Test
    void subStepHandlerOverload_rejectsNullPolicy() {
        assertThatThrownBy(() ->
                StateMachine.<OrderContext>define("order")
                        .initial("PENDING")
                        .state("PENDING")
                        .subStep(handler("reserve", null), null)
        ).isInstanceOf(StateMachineConfigurationException.class)
                .hasMessageContaining("reserve")
                .hasMessageContaining("PENDING");
    }

    @Test
    void subStepHandlerOverload_stillRejectsDuplicateNames() {
        assertThatThrownBy(() ->
                StateMachine.<OrderContext>define("order")
                        .initial("PENDING")
                        .state("PENDING")
                        .subStep("reserve", ctx -> ActionResult.success())
                        .subStep(handler("reserve", null),
                                FailurePolicy.always(FailureDisposition.ABORT))
        ).isInstanceOf(StateMachineConfigurationException.class)
                .hasMessageContaining("Duplicate sub-step 'reserve'");
    }

    @Test
    void subStepHandlerOverload_stillRejectsReservedSeparator() {
        assertThatThrownBy(() ->
                StateMachine.<OrderContext>define("order")
                        .initial("PENDING")
                        .state("PENDING")
                        .subStep(handler("re::serve", null),
                                FailurePolicy.always(FailureDisposition.ABORT))
        ).isInstanceOf(StateMachineConfigurationException.class)
                .hasMessageContaining("::");
    }
}
