package io.hypercell.fsm.execution;

import io.hypercell.fsm.core.ActionResult;

/**
 * The aggregate outcome of running SubStepRunner over one state's sub-steps.
 * <p>
 * This is an internal class — callers never see it. The SubStepRunner returns it,
 * and DefaultStateMachineInstance inspects it to decide whether to continue
 * or enter FAILED status.
 * <p>
 * On failure it carries enough detail to build a
 * {@link io.hypercell.fsm.failure.FailureContext}: the failing step's name and position, the
 * {@link ActionResult} that recorded the failure, and the original throwable.
 */
public final class SubStepRunResult {

    private final boolean completed;
    private final String failedSubStepName;
    private final int failedSubStepIndex;
    private final Throwable error;
    private final ActionResult result;

    private SubStepRunResult(boolean completed, String failedSubStepName, int failedSubStepIndex,
                             Throwable error, ActionResult result) {
        this.completed = completed;
        this.failedSubStepName = failedSubStepName;
        this.failedSubStepIndex = failedSubStepIndex;
        this.error = error;
        this.result = result;
    }

    /**
     * All sub-steps completed successfully.
     */
    public static SubStepRunResult completed() {
        return new SubStepRunResult(true, null, -1, null, null);
    }

    /**
     * A sub-step failed.
     *
     * @param subStepName the name of the failing sub-step
     * @param index       its zero-based position within the state's sub-step list
     * @param error       the original throwable, or a synthetic one when the step reported
     *                    failure without throwing
     * @param result      the recorded failure result
     */
    public static SubStepRunResult failed(String subStepName, int index,
                                          Throwable error, ActionResult result) {
        return new SubStepRunResult(false, subStepName, index, error, result);
    }

    public boolean isCompleted() {
        return completed;
    }

    public boolean isFailed() {
        return !completed;
    }

    public String getFailedSubStepName() {
        return failedSubStepName;
    }

    /**
     * Zero-based position of the failed sub-step within its state; {@code -1} on success.
     */
    public int getFailedSubStepIndex() {
        return failedSubStepIndex;
    }

    public Throwable getError() {
        return error;
    }

    /**
     * The recorded failure result, carrying the error message and type; {@code null} on success.
     */
    public ActionResult getResult() {
        return result;
    }
}
