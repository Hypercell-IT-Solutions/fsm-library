package io.hypercell.fsm.execution;

import io.hypercell.fsm.core.*;
import io.hypercell.fsm.failure.FailurePolicy;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Default, immutable implementation of SubStepDefinition.
 * Built by StateBuilder and stored in DefaultStateDefinition.
 */
final class DefaultSubStepDefinition<C> implements SubStepDefinition<C> {

    private final String name;
    private final Action<C> action;
    private final FailurePolicy<C> failurePolicy;

    DefaultSubStepDefinition(String name, Action<C> action) {
        this(name, action, null);
    }

    DefaultSubStepDefinition(String name, Action<C> action, FailurePolicy<C> failurePolicy) {
        this.name = name;
        this.action = action;
        this.failurePolicy = failurePolicy;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Action<C> action() {
        return action;
    }

    @Override
    public FailurePolicy<C> failurePolicy() {
        return failurePolicy;
    }
}

/**
 * Default, immutable implementation of StateDefinition.
 * Built by StateBuilder and registered in DefaultStateMachineDefinition.
 */
final class DefaultStateDefinition<C> implements StateDefinition<C> {

    private final String name;
    private final boolean terminal;
    private final List<SubStepDefinition<C>> subSteps;
    private final StateHook<C> hook;
    private final FailurePolicy<C> failurePolicy;

    DefaultStateDefinition(String name, boolean terminal,
                           List<SubStepDefinition<C>> subSteps,
                           StateHook<C> hook) {
        this(name, terminal, subSteps, hook, null);
    }

    DefaultStateDefinition(String name, boolean terminal,
                           List<SubStepDefinition<C>> subSteps,
                           StateHook<C> hook,
                           FailurePolicy<C> failurePolicy) {
        this.name = name;
        this.terminal = terminal;
        this.subSteps = Collections.unmodifiableList(subSteps);
        this.hook = hook;
        this.failurePolicy = failurePolicy;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean isTerminal() {
        return terminal;
    }

    @Override
    public List<SubStepDefinition<C>> subSteps() {
        return subSteps;
    }

    @Override
    public Optional<StateHook<C>> hook() {
        return Optional.ofNullable(hook);
    }

    @Override
    public Optional<FailurePolicy<C>> failurePolicy() {
        return Optional.ofNullable(failurePolicy);
    }
}

/**
 * Default, immutable implementation of TransitionDefinition.
 * Built by TransitionBuilder and registered in DefaultStateMachineDefinition.
 */
final class DefaultTransitionDefinition<C> implements TransitionDefinition<C> {

    private final String event;
    private final String sourceState;
    private final String targetState;
    private final Guard<C> guard;
    private final Action<C> action;

    DefaultTransitionDefinition(String event, String sourceState, String targetState,
                                Guard<C> guard, Action<C> action) {
        this.event = event;
        this.sourceState = sourceState;
        this.targetState = targetState;
        this.guard = guard;
        this.action = action;
    }

    @Override
    public String event() {
        return event;
    }

    @Override
    public String sourceState() {
        return sourceState;
    }

    @Override
    public String targetState() {
        return targetState;
    }

    @Override
    public Optional<Guard<C>> guard() {
        return Optional.ofNullable(guard);
    }

    @Override
    public Optional<Action<C>> action() {
        return Optional.ofNullable(action);
    }
}
