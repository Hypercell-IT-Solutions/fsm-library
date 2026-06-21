package io.hypercell.fsm.core;

import io.hypercell.fsm.exception.InvalidEventException;
import io.hypercell.fsm.exception.StateMachineException;
import io.hypercell.fsm.exception.SubStepExecutionException;
import io.hypercell.fsm.execution.ExecutionRecord;
import io.hypercell.fsm.resume.ExecutionSnapshot;

/**
 * A live, running instance of a state machine — one specific workflow execution.
 * <p>
 * RELATIONSHIP TO DEFINITION:
 * StateMachineDefinition is the blueprint (shared, immutable, reusable).
 * StateMachineInstance is one execution of that blueprint (mutable, per-workflow).
 * Think of it like a Class vs an Object instance.
 * <p>
 * THREAD SAFETY:
 * An instance is NOT thread safe. Each instance should be used by one thread at
 * a time. The RetryCoordinator handles concurrent retry protection at a higher level.
 * <p>
 * LIFECYCLE:
 * newInstance()  → status = RUNNING
 * trigger(event) → runs transition + sub-steps → may stay RUNNING or become FAILED/TERMINATED
 * proceed()      → resumes from failure → may go back to RUNNING or fail again
 *
 * @param <C> the context type flowing through the machine
 */
public interface StateMachineInstance<C> {

    /**
     * Unique identifier for this execution. Used as the snapshot storage key.
     * Either a generated UUID or the value passed to {@code newInstance(ctx, executionId)}.
     */
    String executionId();

    /**
     * The state the machine is currently positioned in.
     * Never {@code null}; starts at the initial state.
     */
    StateDefinition<C> currentState();

    /**
     * The current lifecycle status: {@code RUNNING}, {@code TERMINATED}, or {@code FAILED}.
     */
    ExecutionStatus status();

    /**
     * The full live execution record — all steps taken so far, including skipped ones.
     * Primarily used internally; exposed for monitoring and debugging.
     */
    ExecutionRecord executionRecord();

    /**
     * The mutable context object being passed through every guard, action, and sub-step.
     * Actions and sub-steps may modify this object.
     */
    C context();

    /**
     * Trigger a transition via an event name.
     * <p>
     * Executes: onExit → transition action → onEntry → all sub-steps of new state.
     * <p>
     * On sub-step failure:
     * - Status becomes FAILED
     * - Snapshot is saved (if a repository is configured)
     * - SubStepExecutionException is thrown
     * <p>
     * On success:
     * - Returns the new current state
     * - Status is RUNNING (or TERMINATED if the new state is terminal)
     *
     * @throws InvalidEventException     if no valid transition exists for this event
     * @throws SubStepExecutionException if a sub-step fails
     * @throws StateMachineException     if the machine is not in RUNNING status
     */
    StateDefinition<C> trigger(String event);

    /**
     * Continue execution from the failed sub-step.
     * <p>
     * Only valid when status is FAILED. Re-runs the sub-steps of the current state,
     * skipping the ones that already completed in the previous attempt.
     * <p>
     * This is called:
     * - By RetryCoordinator automatically (after backoff delay)
     * - By the developer manually via coordinator.manualRetry()
     *
     * @throws InvalidEventException     if called when status is not FAILED
     * @throws SubStepExecutionException if the sub-step fails again
     */
    StateDefinition<C> proceed();

    /**
     * Resume an interrupted {@code RUNNING} execution by completing the remaining sub-steps
     * of the current state, skipping those that have already been checkpointed.
     * <p>
     * Use this after {@link io.hypercell.fsm.core.StateMachineDefinition#resumeInterrupted}
     * when the process previously crashed mid-transition. This method does <em>not</em>
     * re-run the transition action or entry/exit hooks — those already executed before
     * the crash. It simply completes the remaining sub-step work.
     *
     * @return the current state after all remaining sub-steps have been processed
     * @throws InvalidEventException     if called when status is not {@code RUNNING}
     * @throws SubStepExecutionException if a remaining sub-step fails
     */
    StateDefinition<C> resume();

    /**
     * Take a serializable snapshot of the current execution state.
     * <p>
     * Called internally on failure (and by the RetryCoordinator), but also
     * available to callers who want to checkpoint a long-running machine.
     *
     * @param pendingEvent the event that was being processed when this snapshot
     *                     is taken; null if called outside a trigger() ctx
     */
    ExecutionSnapshot takeSnapshot(String pendingEvent);

    ExecutionSnapshot takeCheckpoint();

    /**
     * Check if the current state is the initial state.
     * Useful for validation after initialization or recovery.
     * Delegates to the machine definition.
     *
     * @return true if currently in the initial state, false otherwise
     */
    boolean isInInitialState();

    /**
     * Check if the current state is terminal.
     * Equivalent to {@code currentState().isTerminal()}.
     *
     * @return true if the current state is terminal, false otherwise
     */
    boolean isInTerminalState();

    /**
     * {@code true} when status is {@code TERMINATED}.
     */
    boolean isTerminated();

    /**
     * {@code true} when status is {@code FAILED}.
     */
    boolean isFailed();

    /**
     * {@code true} when status is {@code RUNNING}.
     */
    boolean isRunning();
}
