package io.hypercell.fsm.manager;

import io.hypercell.fsm.core.ExecutionStatus;

/**
 * The result of a {@link StateMachineManager#trigger(String, String)},
 * {@link StateMachineManager#proceed(String)}, or {@link StateMachineManager#resume(String)} call.
 * <p>
 * Contains everything the HTTP endpoint needs to build a response:
 * the new state, the overall status, and — on failure — which sub-step failed and the root cause.
 *
 * @param <C> the context type (carried for type safety, not always populated)
 */
public final class ManagedTransitionResult<C> {

    private final String executionId;
    private final String fromState;
    private final String toState;
    private final ExecutionStatus executionStatus;

    /**
     * Non-null only when executionStatus == FAILED.
     * Tells the caller which sub-step failed so they can surface it in their response.
     */
    private final String failedSubStepName;
    private final String failedStateName;

    /**
     * The root exception that caused the failure, if available.
     * Only populated when executionStatus == FAILED. Allows callers to handle
     * different exception types (e.g., SQLException vs other exceptions).
     */
    private final Throwable rootCause;

    /**
     * The context object as it exists after execution completes.
     * Sub-steps may have mutated it during the run, so this reflects the final state.
     * <p>
     * Null only for idempotent {@code initialize()} calls where a snapshot already
     * existed and no execution was performed.
     */
    private final C context;

    private ManagedTransitionResult(Builder<C> b) {
        this.executionId = b.executionId;
        this.fromState = b.fromState;
        this.toState = b.toState;
        this.executionStatus = b.executionStatus;
        this.failedSubStepName = b.failedSubStepName;
        this.failedStateName = b.failedStateName;
        this.rootCause = b.rootCause;
        this.context = b.context;
    }

    /**
     * The business entity ID passed to {@code manager.trigger(executionId, event)}.
     */
    public String getExecutionId() {
        return executionId;
    }

    /**
     * The state the machine was in before this transition.
     */
    public String getFromState() {
        return fromState;
    }

    /**
     * The state the machine is in after this transition (or the state it failed in).
     */
    public String getToState() {
        return toState;
    }

    /**
     * Overall lifecycle status after this operation: {@code RUNNING}, {@code TERMINATED}, or {@code FAILED}.
     */
    public ExecutionStatus getExecutionStatus() {
        return executionStatus;
    }

    /**
     * The sub-step name that failed; non-null only when {@link #getExecutionStatus()} is {@code FAILED}.
     */
    public String getFailedSubStepName() {
        return failedSubStepName;
    }

    /**
     * The state name in which failure occurred; non-null only when {@link #getExecutionStatus()} is {@code FAILED}.
     */
    public String getFailedStateName() {
        return failedStateName;
    }

    /**
     * The root cause of the failure, if available.
     * Only populated when {@link #getExecutionStatus()} is {@code FAILED}.
     * Allows callers to handle different exception types (e.g., check for SQLException).
     * <p>
     * Example:
     * <pre>{@code
     * if (result.isFailed() && result.getRootCause() instanceof SQLException) {
     *     // Handle database error
     * }
     * }</pre>
     *
     * @return the exception that caused the failure, or null if unavailable
     */
    public Throwable getRootCause() {
        return rootCause;
    }

    /**
     * The context object as it exists after execution completes.
     * Sub-steps may have mutated it, so this reflects the post-execution state —
     * no second DB load needed to build an HTTP response.
     * <p>
     * Null only for idempotent {@code initialize()} calls where a snapshot already
     * existed and no execution was performed.
     */
    public C getContext() {
        return context;
    }

    /**
     * {@code true} when the execution reached a terminal state.
     */
    public boolean isTerminated() {
        return executionStatus == ExecutionStatus.TERMINATED;
    }

    /**
     * {@code true} when the execution is ongoing and awaiting the next event.
     */
    public boolean isRunning() {
        return executionStatus == ExecutionStatus.RUNNING;
    }

    /**
     * {@code true} when a sub-step failed; the snapshot has been saved for retry.
     */
    public boolean isFailed() {
        return executionStatus == ExecutionStatus.FAILED;
    }

    @Override
    public String toString() {
        return "ManagedTransitionResult{" +
                "executionId='" + executionId + '\'' +
                ", " + fromState + " → " + toState +
                ", status=" + executionStatus +
                (failedSubStepName != null ? ", failedAt=" + failedStateName + "/" + failedSubStepName : "") +
                '}';
    }

    /**
     * For internal use — constructs results inside {@link io.hypercell.fsm.manager.DefaultStateMachineManager}.
     */
    public static <C> Builder<C> builder() {
        return new Builder<>();
    }

    /**
     * Internal builder; not part of the consumer-facing API.
     */
    public static final class Builder<C> {
        private String executionId;
        private String fromState;
        private String toState;
        private ExecutionStatus executionStatus;
        private String failedSubStepName;
        private String failedStateName;
        private Throwable rootCause;
        private C context;

        public Builder<C> executionId(String v) {
            executionId = v;
            return this;
        }

        public Builder<C> fromState(String v) {
            fromState = v;
            return this;
        }

        public Builder<C> toState(String v) {
            toState = v;
            return this;
        }

        public Builder<C> executionStatus(ExecutionStatus v) {
            executionStatus = v;
            return this;
        }

        public Builder<C> failedSubStepName(String v) {
            failedSubStepName = v;
            return this;
        }

        public Builder<C> failedStateName(String v) {
            failedStateName = v;
            return this;
        }

        public Builder<C> rootCause(Throwable v) {
            rootCause = v;
            return this;
        }

        public Builder<C> context(C v) {
            context = v;
            return this;
        }

        public ManagedTransitionResult<C> build() {
            return new ManagedTransitionResult<>(this);
        }
    }
}
