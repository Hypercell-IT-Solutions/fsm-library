package io.hypercell.fsm.core;

/**
 * The current lifecycle status of a StateMachineInstance.
 * <p>
 * Valid transitions between statuses:
 * <p>
 * RUNNING → TERMINATED (terminal state reached, all sub-steps passed)
 * RUNNING → FAILED     (a sub-step or hook threw/returned failed)
 * FAILED  → RUNNING    (proceed() called to resume from failure point)
 * <p>
 * A TERMINATED or permanently FAILED (exhausted retries) machine cannot be
 * transitioned further. Calling trigger() on a non-RUNNING machine throws.
 */
public enum ExecutionStatus {

    /**
     * Normal operating state. trigger() and proceed() are accepted.
     */
    RUNNING,

    /**
     * A terminal state was reached. trigger() will throw.
     */
    TERMINATED,

    /**
     * A sub-step or hook failed. The snapshot has been saved.
     * The machine is waiting for a retry (manual or automatic).
     * proceed() transitions back to RUNNING.
     */
    FAILED
}
