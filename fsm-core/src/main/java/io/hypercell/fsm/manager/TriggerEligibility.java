package io.hypercell.fsm.manager;

/**
 * Advisory indication of whether {@link StateMachineManager#trigger(String, String)} may be
 * called on a given execution without throwing
 * {@link io.hypercell.fsm.exception.IllegalTriggerStateException}.
 * <p>
 * Obtain via {@link StateMachineManager#eligibilityOf(String)}.
 * <p>
 * <strong>These values are advisory hints only.</strong> A concurrent operation may change the
 * execution's status between the call to {@code eligibilityOf()} and the subsequent
 * {@code trigger()} call. {@code trigger()} always re-validates the status authoritatively
 * under its per-execution lock and will throw if the execution is not eligible at that point.
 * Use {@code eligibilityOf()} for user-facing checks, logging, or early-exit guards, not as a
 * substitute for the lock-protected validation inside {@code trigger()}.
 */
public enum TriggerEligibility {

    /**
     * The execution is ready to accept a new event.
     * <p>
     * Covers two cases:
     * <ul>
     *   <li>No snapshot exists yet — the execution has never been started; this will be
     *       the first event and a new instance will be created.</li>
     *   <li>Snapshot status is {@code WAITING} — the execution is parked at a non-terminal
     *       state, awaiting the next event.</li>
     * </ul>
     */
    READY,

    /**
     * The execution has a {@code FAILED} or {@code RETRY_SCHEDULED} snapshot.
     * <p>
     * Call {@link StateMachineManager#proceed(String)} to retry the failed sub-steps first.
     * Once {@code proceed()} succeeds and the snapshot transitions to {@code WAITING}, the
     * execution will be {@code READY} for a new {@code trigger()} call.
     */
    NEEDS_PROCEED,

    /**
     * The execution has a {@code RUNNING} snapshot — the process crashed mid-transition.
     * <p>
     * Call {@link StateMachineManager#resume(String)} to complete the in-flight transition
     * first. Once {@code resume()} succeeds and the snapshot transitions to {@code WAITING},
     * the execution will be {@code READY} for a new {@code trigger()} call.
     */
    NEEDS_RESUME,

    /**
     * The execution has reached a terminal state.
     * <p>
     * Calling {@code trigger()} will throw
     * {@link io.hypercell.fsm.exception.CompletedMachineException}. No further events can
     * be applied. Call {@code repository.delete(executionId)} to clean up the snapshot.
     */
    TERMINATED
}
