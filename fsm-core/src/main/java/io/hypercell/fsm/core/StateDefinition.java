package io.hypercell.fsm.core;

import io.hypercell.fsm.failure.FailurePolicy;

import java.util.List;
import java.util.Optional;

/**
 * Defines a single state in the state machine graph.
 * <p>
 * A state is a stable condition that the machine can be in. While in a state,
 * the machine can be waiting for an event (between sub-steps are complete) or
 * actively executing sub-steps.
 * <p>
 * TERMINAL STATES:
 * A terminal state is the end of the workflow. When the machine enters a terminal
 * state and completes its sub-steps, its status becomes COMPLETED. No further
 * events can be triggered. Terminal states can have sub-steps and hooks — they
 * just have no outgoing transitions.
 *
 * @param <C> the context type flowing through the machine
 */
public interface StateDefinition<C> {

    /**
     * Unique name for this state within the machine.
     */
    String name();

    /**
     * Whether this is an end state. When the machine enters a terminal state
     * and all its sub-steps complete, the instance status becomes COMPLETED.
     */
    boolean isTerminal();

    /**
     * Ordered list of sub-steps to execute upon entering this state.
     * Executed sequentially. If any fails, the machine enters FAILED status.
     * May be empty for states that only serve as routing/coordination points.
     */
    List<SubStepDefinition<C>> subSteps();

    /**
     * Optional entry/exit lifecycle callbacks.
     * Present if the builder called .onEntry() or .onExit() for this state.
     */
    Optional<StateHook<C>> hook();

    /**
     * The state-level {@link FailurePolicy}, consulted when a sub-step of this state fails and
     * the sub-step itself had no opinion. Present if the builder called
     * {@code .failurePolicy(...)} for this state.
     * <p>
     * This is where most policies live in practice: the policy receives the sub-step name in its
     * {@link io.hypercell.fsm.failure.FailureContext}, so one state-level policy can classify
     * every sub-step of the state.
     */
    Optional<FailurePolicy<C>> failurePolicy();
}
