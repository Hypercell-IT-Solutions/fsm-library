package io.hypercell.fsm.core;

import io.hypercell.fsm.failure.FailurePolicy;

/**
 * Defines a single named step within a state.
 * <p>
 * Sub-steps are the granular units of work that the retry mechanism tracks.
 * When a machine resumes after a failure, it skips all sub-steps that already
 * completed successfully and re-runs from the first one that failed.
 * <p>
 * NAMING CONVENTION:
 * Sub-step names must be unique within their parent state. Use descriptive
 * verb-noun names that reflect the external system being called:
 * "validate-items", "charge-payment", "notify-warehouse"
 * <p>
 * The name is used as the key in the snapshot — if you rename a sub-step
 * in the code, existing snapshots won't be able to match it on resume.
 * Treat sub-step names as stable identifiers, like database column names.
 *
 * @param <C> the context type flowing through the machine
 */
public interface SubStepDefinition<C> {

    /**
     * Unique name within the parent state. Used as the snapshot key.
     */
    String name();

    /**
     * The actual work this sub-step performs.
     */
    Action<C> action();

    /**
     * The most specific {@link FailurePolicy} in the chain — consulted first when this sub-step
     * fails, before the state's policy and the machine's policy.
     *
     * @return the policy, or {@code null} when this sub-step has no opinion of its own
     */
    FailurePolicy<C> failurePolicy();
}
