package io.hypercell.fsm.builder;

import io.hypercell.fsm.core.*;
import io.hypercell.fsm.exception.StateMachineConfigurationException;
import io.hypercell.fsm.failure.FailurePolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Fluent builder for defining a single state.
 * <p>
 * Obtained from {@link StateMachineBuilder#state(String)}. Chain calls to add
 * sub-steps, hooks, and transitions, then call {@link #and()} to register the state
 * and return to the machine builder.
 * <p>
 * Example:
 * <pre>{@code
 * .state("PROCESSING")
 *     .onEntry(ctx -> ctx.setStartedAt(Instant.now()))
 *     .subStep("reserve-stock",  ctx -> reserveStock(ctx))
 *     .subStep("charge-payment", ctx -> chargePayment(ctx))
 *     .on("COMPLETE").to("SHIPPED").end()
 *     .and()
 * }</pre>
 *
 * @param <C> the context type flowing through the machine
 */
public class StateBuilder<C> {
    private final StateMachineBuilder<C> parent;
    private final String name;
    private boolean terminal = false;
    private final List<SubStepDefinition<C>> subSteps = new ArrayList<>();
    private StateHook<C> compositeHook;
    private FailurePolicy<C> failurePolicy;
    final List<Object[]> transitionData = new ArrayList<>();

    StateBuilder(StateMachineBuilder<C> parent, String name) {
        this.parent = parent;
        this.name = name;
    }

    /**
     * Register a callback invoked when the machine enters this state, after the
     * transition action and before sub-steps run.
     * <p>
     * Multiple calls compose: all registered functions run in registration order.
     * Hooks are NOT tracked for retry — they re-run on every entry, including resumes.
     * Keep them idempotent or side-effect-free; use sub-steps for non-idempotent work.
     */
    public StateBuilder<C> onEntry(Consumer<C> fn) {
        StateHook<C> existing = compositeHook;
        compositeHook = new StateHook<C>() {
            @Override
            public void onEntry(C ctx) {
                if (existing != null) existing.onEntry(ctx);
                fn.accept(ctx);
            }

            @Override
            public void onExit(C ctx) {
                if (existing != null) existing.onExit(ctx);
            }
        };
        return this;
    }

    /**
     * Register a callback invoked before the machine leaves this state (when a
     * transition fires). Not called if the machine fails mid-state.
     * <p>
     * Multiple calls compose: all registered functions run in registration order.
     * Same idempotency guidance as {@link #onEntry}.
     */
    public StateBuilder<C> onExit(Consumer<C> fn) {
        StateHook<C> existing = compositeHook;
        compositeHook = new StateHook<C>() {
            @Override
            public void onEntry(C ctx) {
                if (existing != null) existing.onEntry(ctx);
            }

            @Override
            public void onExit(C ctx) {
                if (existing != null) existing.onExit(ctx);
                fn.accept(ctx);
            }
        };
        return this;
    }

    /**
     * Set the failure policy for this state — consulted whenever one of its sub-steps fails and
     * that sub-step had no policy of its own.
     * <p>
     * This is usually the right level to configure: the policy receives the failing sub-step's
     * name and position in its {@link io.hypercell.fsm.failure.FailureContext}, so a single
     * state-level policy can classify every sub-step of the state.
     * <pre>{@code
     * .state("PERFORM_SIM_SWAP")
     *     .failurePolicy(FailurePolicy.onFirstSubStep(FailureDisposition.REWIND))
     *     .subStep("reserve-msisdn", ctx -> reserve(ctx))   // fails → whole transition rewound
     *     .subStep("activate-sim",   ctx -> activate(ctx))  // fails → normal retry from here
     * }</pre>
     * Returning {@code null} from the policy defers to the machine-level policy, and then to
     * {@link io.hypercell.fsm.failure.FailureDisposition#RETRY}.
     *
     * @param policy the policy; must not be {@code null}
     */
    public StateBuilder<C> failurePolicy(FailurePolicy<C> policy) {
        if (policy == null) throw new StateMachineConfigurationException(
                "failurePolicy must not be null for state '" + name + "'.");
        this.failurePolicy = policy;
        return this;
    }

    /**
     * Register a sub-step from a handler class.
     * Equivalent to: .subStep(handler.name(), handler::execute), also picking up the handler's
     * {@link SubStepHandler#failurePolicy()} if it declares one.
     */
    public StateBuilder<C> subStep(SubStepHandler<C> handler) {
        return subStep(handler.name(), handler::execute, handler.failurePolicy());
    }

    /**
     * Add a named sub-step with an inline action lambda.
     * <p>
     * Sub-steps are executed in the order they are added. On resume after failure,
     * completed sub-steps are skipped; only the failed step and those after it re-run.
     *
     * @param name   stable snapshot key — treat like a DB column name; renaming breaks
     *               existing snapshots
     * @param action the work to perform; may read and write the ctx
     * @throws StateMachineConfigurationException if the name is a duplicate or contains {@code ::}
     */
    public StateBuilder<C> subStep(String name, Action<C> action) {
        return subStep(name, action, null);
    }

    /**
     * Add a named sub-step with its own failure policy.
     * <p>
     * The sub-step policy is the most specific level of the chain — it is consulted before the
     * state's and the machine's. Use this when one step inside a state needs recovery behaviour
     * that the rest of the state does not share; otherwise prefer {@link #failurePolicy} at the
     * state level.
     *
     * @param name   stable snapshot key — treat like a DB column name; renaming breaks
     *               existing snapshots
     * @param action the work to perform; may read and write the ctx
     * @param policy the failure policy for this step; {@code null} means "no opinion", deferring
     *               to the state policy
     * @throws StateMachineConfigurationException if the name is a duplicate or contains {@code ::}
     */
    public StateBuilder<C> subStep(String name, Action<C> action, FailurePolicy<C> policy) {
        if (subSteps.stream().anyMatch(s -> s.name().equals(name)))
            throw new StateMachineConfigurationException(
                    "Duplicate sub-step '" + name + "' in state '" + this.name + "'.");
        if (name.contains("::"))
            throw new StateMachineConfigurationException(
                    "Sub-step name '" + name + "' contains reserved separator '::'.");
        subSteps.add(new SimpleSubStep<>(name, action, policy));
        return this;
    }

    /**
     * Begin defining a transition from this state for the given event name.
     * Multiple calls with the same event name add multiple transitions; they are
     * evaluated in definition order and the first whose guard returns {@code true} fires.
     *
     * @return a {@link TransitionBuilder}; call {@code .to(...).end()} to close it
     */
    public TransitionBuilder<C> on(String event) {
        return new TransitionBuilder<>(this, event);
    }

    /**
     * Mark this state as terminal (the end of the workflow).
     * When the machine enters a terminal state and all its sub-steps complete,
     * the instance status becomes {@code COMPLETED}. Terminal states must have
     * no outgoing transitions; {@link StateMachineBuilder#build()} enforces this.
     */
    public StateBuilder<C> terminal() {
        this.terminal = true;
        return this;
    }

    /**
     * Register this state with the parent builder and return to it.
     * Must be called to close the state definition — otherwise the state is silently dropped.
     */
    public StateMachineBuilder<C> and() {
        parent.registerState(this);
        return parent;
    }

    void addTransition(String event, String target, Guard<C> guard, Action<C> action) {
        transitionData.add(new Object[]{event, target, guard, action});
    }

    String getName() {
        return name;
    }

    boolean isTerminal() {
        return terminal;
    }

    List<SubStepDefinition<C>> getSubSteps() {
        return subSteps;
    }

    StateHook<C> getCompositeHook() {
        return compositeHook;
    }

    FailurePolicy<C> getFailurePolicy() {
        return failurePolicy;
    }

    private record SimpleSubStep<C>(String n, Action<C> a, FailurePolicy<C> p)
            implements SubStepDefinition<C> {
        @Override
        public String name() {
            return n;
        }

        @Override
        public Action<C> action() {
            return a;
        }

        @Override
        public FailurePolicy<C> failurePolicy() {
            return p;
        }
    }
}
