package io.hypercell.fsm.resume;

/**
 * The persisted status of an execution snapshot.
 * <p>
 * This is separate from {@code ExecutionStatus} (which is the live runtime status)
 * because the snapshot lives in storage between request boundaries. The persisted
 * status encodes exactly where in its lifecycle an execution is at rest.
 * <p>
 * Status transition diagram:
 * <pre>
 *   (new execution)
 *        │ initial state entered / sub-steps run
 *        ▼
 *     RUNNING ──── sub-steps done, non-terminal ──────────────────────► WAITING
 *        │                                                                  │
 *        │ sub-step fails                                          next event arrives
 *        ▼                                                                  │
 *      FAILED ◄──────────────── retry also fails ──────────────── RUNNING (retry)
 *        │                                                                  │
 *        │ RetryPolicy.shouldRetry() == true                        retry succeeds
 *        ▼                                                                  │
 *  RETRY_SCHEDULED ─── scheduled retry fires ──────────────────────────────┤
 *                                                                           │
 *                                              terminal state reached       │
 *                                                      ▼                   │
 *                                                  TERMINATED ◄────────────┘
 * </pre>
 * <p>
 * Precise meanings:
 * <ul>
 *   <li>{@code RUNNING} — the execution is actively processing sub-steps. A crash
 *       leaves the snapshot in this state; the startup sweep and lazy-resume path
 *       detect and complete such executions.</li>
 *   <li>{@code WAITING} — sub-steps completed; the machine is parked at a non-terminal
 *       state awaiting the next event. This is the normal at-rest state between two
 *       consecutive transitions.</li>
 *   <li>{@code TERMINATED} — a terminal state was reached successfully. The snapshot is
 *       retained so that subsequent {@code trigger()} or {@code proceed()} calls correctly
 *       throw {@code CompletedMachineException}. Call {@code repository.delete(executionId)}
 *       to clean up when the record is no longer needed.</li>
 *   <li>{@code FAILED} — a sub-step failed; the execution is waiting for a manual or
 *       scheduled retry.</li>
 *   <li>{@code RETRY_SCHEDULED} — an automatic retry has been scheduled. Do not call
 *       {@code manualRetry()} while here; the scheduled retry will fire. Cancel the
 *       scheduled retry first if you must force an immediate retry.</li>
 * </ul>
 */
public enum SnapshotStatus {

    /**
     * Execution failed. Waiting for retry (manual or scheduled).
     */
    FAILED,

    /**
     * An automatic retry has been scheduled. A RETRY_SCHEDULED snapshot
     * should not be manually retried until the scheduled retry completes or
     * is canceled — doing so risks double-execution.
     */
    RETRY_SCHEDULED,

    /**
     * The execution is actively processing sub-steps. A crash leaves the snapshot
     * here; the startup sweep queries {@code WHERE status = 'RUNNING'} to find
     * interrupted executions efficiently via the status index.
     */
    RUNNING,

    /**
     * Sub-steps completed; the machine is parked at a non-terminal state
     * awaiting the next event. This is the normal at-rest status between transitions.
     * Executions in this state are NOT interrupted and are NOT returned by
     * {@code listInterrupted()}.
     */
    WAITING,

    /**
     * Execution reached a terminal state successfully. The snapshot is retained
     * with this status so that subsequent {@code trigger()} or {@code proceed()}
     * calls correctly throw {@code CompletedMachineException}.
     */
    TERMINATED
}
