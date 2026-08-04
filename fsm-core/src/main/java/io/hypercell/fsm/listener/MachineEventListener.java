package io.hypercell.fsm.listener;

/**
 * Listener interface for state machine lifecycle events.
 * <p>
 * DESIGN: TWO WAYS TO LISTEN
 * <p>
 * Option 1 — handle all events in one method (good for generic logging):
 * <pre>{@code
 * machine.addListener(event -> log(event));
 * }</pre>
 * Option 2 — override only the events you care about (good for specific reactions):
 * <pre>{@code
 * machine.addListener(new MachineEventListener<OrderContext>() {
 *     @Override
 *     public void onSubStepFailed(MachineEvent.SubStepFailedEvent<OrderContext> e) {
 *         alertOps("Payment step failed: " + e.getErrorMessage());
 *     }
 *     @Override
 *     public void onMachineCompleted(MachineEvent.MachineCompletedEvent<OrderContext> e) {
 *         metrics.increment("orders.completed");
 *     }
 * });
 * }</pre>
 * THREADING:
 * Listeners are called synchronously on the same thread as the machine execution.
 * Keep listeners fast — don't block inside them. If you need to do heavy work
 * (send a webhook, write to a slow DB), publish to a queue and process async.
 * <p>
 * EXCEPTION HANDLING:
 * Exceptions thrown by a listener are caught and logged but do NOT propagate —
 * a bad listener should not kill the machine execution.
 *
 * @param <C> the context type of the machine being observed
 */
public interface MachineEventListener<C> {

    /**
     * Called for every event. Override this for generic handling.
     * The default implementation delegates to the specific typed methods below.
     */
    default void onEvent(MachineEvent<C> event) {
        if (event instanceof MachineEvent.TransitionFiredEvent<C> e) onTransitionFired(e);
        else if (event instanceof MachineEvent.StateEnteredEvent<C> e) onStateEntered(e);
        else if (event instanceof MachineEvent.StateExitedEvent<C> e) onStateExited(e);
        else if (event instanceof MachineEvent.SubStepCompletedEvent<C> e) onSubStepCompleted(e);
        else if (event instanceof MachineEvent.SubStepSkippedEvent<C> e) onSubStepSkipped(e);
        else if (event instanceof MachineEvent.SubStepFailedEvent<C> e) onSubStepFailed(e);
        else if (event instanceof MachineEvent.MachineCompletedEvent<C> e) onMachineCompleted(e);
        else if (event instanceof MachineEvent.MachineFailedEvent<C> e) onMachineFailed(e);
        else if (event instanceof MachineEvent.MachineResumedEvent<C> e) onMachineResumed(e);
        else if (event instanceof MachineEvent.MachineRewoundEvent<C> e) onMachineRewound(e);
    }

    /** Fired when a transition successfully moves the machine from one state to another. */
    default void onTransitionFired(MachineEvent.TransitionFiredEvent<C> event) {
    }

    /** Fired after the machine enters a state and its {@code onEntry} hook completes. */
    default void onStateEntered(MachineEvent.StateEnteredEvent<C> event) {
    }

    /** Fired after the machine's {@code onExit} hook completes (before it moves). */
    default void onStateExited(MachineEvent.StateExitedEvent<C> event) {
    }

    /** Fired when a sub-step executes and returns {@code SUCCESS}. */
    default void onSubStepCompleted(MachineEvent.SubStepCompletedEvent<C> event) {
    }

    /**
     * Fired when a sub-step is skipped during a resume because it was already
     * recorded as completed in a previous run. Never fired on fresh executions.
     */
    default void onSubStepSkipped(MachineEvent.SubStepSkippedEvent<C> event) {
    }

    /**
     * Fired when a sub-step returns {@code FAILED} or throws.
     * The machine enters {@code FAILED} status immediately after this event.
     * Use this for alerting, dashboards, and audit trails.
     */
    default void onSubStepFailed(MachineEvent.SubStepFailedEvent<C> event) {
    }

    /**
     * Fired when the machine reaches a terminal state and all sub-steps complete.
     * This is the happy-path completion event.
     */
    default void onMachineCompleted(MachineEvent.MachineCompletedEvent<C> event) {
    }

    /**
     * Fired when the machine enters {@code FAILED} status — after the snapshot is
     * saved and before {@code SubStepExecutionException} is thrown to the caller.
     */
    default void onMachineFailed(MachineEvent.MachineFailedEvent<C> event) {
    }

    /**
     * Fired at the start of a resume ({@code proceed()}) before any sub-steps run.
     * Carries the state and sub-step name the resume will re-run from.
     */
    default void onMachineResumed(MachineEvent.MachineResumedEvent<C> event) {
    }

    /**
     * Fired when a failure was classified {@link io.hypercell.fsm.failure.FailureDisposition#REWIND}
     * and the in-flight transition was abandoned — the execution is parked back at its source
     * state and can be re-triggered.
     * <p>
     * Always preceded by an {@link #onMachineFailed} carrying the same disposition.
     */
    default void onMachineRewound(MachineEvent.MachineRewoundEvent<C> event) {
    }
}
