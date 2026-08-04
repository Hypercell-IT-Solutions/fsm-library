package io.hypercell.fsm.failure;

/**
 * How a sub-step failure should be handled once it has been recorded.
 * <p>
 * This is deliberately separate from
 * {@link io.hypercell.fsm.resume.SnapshotStatus}: the status says <em>where in its lifecycle</em>
 * an execution is at rest, while the disposition says <em>how this particular failure should be
 * treated</em>. Two executions can both be {@code FAILED} and still need completely different
 * recovery, which is exactly what this enum expresses.
 * <p>
 * The disposition is decided at failure time by the resolved {@link FailurePolicy} and then
 * persisted on the snapshot, so recovery sweeps running in a different process (or days later)
 * see the same decision.
 * <p>
 * <strong>Behaviour summary</strong>
 * <table border="1">
 *   <caption>Effect of each disposition</caption>
 *   <tr><th>Disposition</th><th>Status</th><th>Positioned at</th><th>Swept</th>
 *       <th>Auto-retry</th><th>{@code proceed()}</th><th>{@code trigger()}</th></tr>
 *   <tr><td>{@link #RETRY}</td><td>FAILED / RETRY_SCHEDULED</td><td>failed state</td><td>yes</td>
 *       <td>yes</td><td>yes</td><td>throws</td></tr>
 *   <tr><td>{@link #MANUAL}</td><td>FAILED</td><td>failed state</td><td>no</td>
 *       <td>no</td><td>yes</td><td>throws</td></tr>
 *   <tr><td>{@link #REWIND}</td><td>WAITING</td><td>source state</td><td>no</td>
 *       <td>no</td><td>no-op</td><td><strong>yes</strong></td></tr>
 *   <tr><td>{@link #ABORT}</td><td>FAILED</td><td>failed state</td><td>no</td>
 *       <td>no</td><td>throws</td><td>throws</td></tr>
 * </table>
 *
 * @see FailurePolicy
 * @see FailureContext
 */
public enum FailureDisposition {

    /**
     * The default and the library's historical behaviour: the failure is transient and a
     * blind retry from the failure point may fix it.
     * <p>
     * The snapshot stays {@code FAILED} (or moves to {@code RETRY_SCHEDULED} if an auto-retry
     * policy is configured), the {@link io.hypercell.fsm.retry.RetryCoordinator} is consulted,
     * and the execution is picked up by
     * {@link io.hypercell.fsm.manager.StateMachineManager#recoverFailedExecutions(int)}.
     */
    RETRY,

    /**
     * The failure needs a human or an external system to decide what happens next.
     * <p>
     * The snapshot stays {@code FAILED} and remains resumable from the failure point via
     * {@link io.hypercell.fsm.manager.StateMachineManager#proceed(String)}, but it is invisible
     * to every automatic recovery path: no auto-retry is scheduled and
     * {@code recoverFailedExecutions(int)} skips it.
     */
    MANUAL,

    /**
     * Nothing was committed, so the in-flight transition is abandoned entirely and the execution
     * is parked back at the state it came from.
     * <p>
     * The snapshot is saved as {@code WAITING} positioned at the <em>source</em> state, which
     * makes the execution eligible for {@code trigger(executionId, event)} again — the caller
     * re-fires the same event rather than resuming from the failure point. Use this for the
     * first sub-step of a state, where a failure means the state was never really entered.
     * <p>
     * <strong>Requirements.</strong> A rewind is only sound when no sub-step of the state has
     * committed. The library enforces this: if any sub-step of the current state already
     * succeeded (or was skipped as already-complete from a prior attempt), or there is no source
     * state to return to, the disposition is downgraded to {@link #MANUAL} and a warning is
     * logged.
     * <p>
     * <strong>Idempotency.</strong> The transition action and the target state's {@code onEntry}
     * hook have already run and will run again on re-trigger. No compensating {@code onExit} is
     * invoked. Keep both idempotent, as the general hook guidance already advises.
     */
    REWIND,

    /**
     * The failure is permanent and no amount of retrying will help — a business rule was
     * violated, an input was invalid, the entity is ineligible.
     * <p>
     * The snapshot stays {@code FAILED} for auditing, but every recovery path refuses it:
     * no auto-retry, no sweep, and
     * {@link io.hypercell.fsm.manager.StateMachineManager#proceed(String)} throws
     * {@link io.hypercell.fsm.exception.ExecutionAbortedException}. Delete the snapshot or
     * start a new execution to move on.
     */
    ABORT
}
