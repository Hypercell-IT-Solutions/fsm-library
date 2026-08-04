package io.hypercell.fsm.exception;

/**
 * Thrown when a recovery operation is attempted on an execution whose failure was classified
 * {@link io.hypercell.fsm.failure.FailureDisposition#ABORT}.
 * <p>
 * An aborted execution failed for a reason that retrying cannot fix — a business rule was
 * violated, an input was invalid, the entity is ineligible. The snapshot is retained for
 * auditing, but {@code proceed()} refuses it, no auto-retry is scheduled, and
 * {@code recoverFailedExecutions(int)} skips it. This is the difference between "failed" and
 * "failed for good".
 * <p>
 * CALLER GUIDANCE:
 * Return HTTP 422 Unprocessable Entity — retrying the same request will never succeed. To move
 * on, either delete the snapshot and start a fresh execution, or correct the underlying data and
 * start a new one; the aborted execution itself is not resumable.
 */
public class ExecutionAbortedException extends StateMachineException {

    private final String executionId;
    private final String failedStateName;
    private final String failedSubStepName;

    public ExecutionAbortedException(String executionId, String failedStateName,
                                     String failedSubStepName, String errorMessage) {
        super(String.format(
                "Execution '%s' was aborted at '%s / %s' and cannot be resumed: %s. "
                        + "The failure was classified ABORT, meaning retrying will not help.",
                executionId, failedStateName, failedSubStepName, errorMessage));
        this.executionId = executionId;
        this.failedStateName = failedStateName;
        this.failedSubStepName = failedSubStepName;
    }

    public String getExecutionId() {
        return executionId;
    }

    /**
     * The state containing the sub-step that aborted the execution.
     */
    public String getFailedStateName() {
        return failedStateName;
    }

    public String getFailedSubStepName() {
        return failedSubStepName;
    }
}
