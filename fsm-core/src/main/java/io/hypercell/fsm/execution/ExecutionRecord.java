package io.hypercell.fsm.execution;

import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.core.ExecutionStatus;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * The mutable, append-only execution log for one StateMachineInstance.
 * <p>
 * This class serves two roles:
 * <p>
 * 1. LIVE LOG: During execution, every sub-step appends a StepRecord here.
 * The SubStepRunner reads it to know which steps have already completed.
 * <p>
 * 2. SNAPSHOT SOURCE: When a failure occurs, takeSnapshot() reads this record
 * to build the serializable ExecutionSnapshot that gets persisted.
 * <p>
 * NOT THREAD SAFE:
 * This is intentional. A machine instance is single-threaded. The RetryCoordinator
 * ensures only one execution runs at a time for a given executionId.
 */
public class ExecutionRecord {

    private final String executionId;

    /**
     * Append-only list of every step taken. Never remove entries.
     */
    private final List<StepRecord> steps = new ArrayList<>();

    private String currentStateName;
    private String previousStateName;
    private String failedStateName;
    private String failedSubStepName;
    private String lastTriggerEvent;
    private ExecutionStatus status;

    public ExecutionRecord(String executionId, String initialStateName) {
        this.executionId = executionId;
        this.currentStateName = initialStateName;
        this.status = ExecutionStatus.RUNNING;
    }

    /**
     * Record the outcome of a sub-step execution.
     */
    public void recordStep(String stateName, String subStepName, ActionResult result) {
        steps.add(new StepRecord(stateName, subStepName, result, Instant.now()));
    }

    /**
     * Called at the start of trigger() to track which event is in flight.
     */
    public void setLastTriggerEvent(String event) {
        this.lastTriggerEvent = event;
    }

    public void setCurrentStateName(String name) {
        this.currentStateName = name;
    }

    /**
     * Move to a new state, remembering the one being left.
     * <p>
     * Called by {@code trigger()} as the transition commits. The remembered state is what a
     * {@link io.hypercell.fsm.failure.FailureDisposition#REWIND} returns the execution to, so it
     * is only meaningful within the transition that set it — it is deliberately not persisted.
     * An execution rebuilt from a snapshot has no previous state until it triggers again.
     */
    public void moveTo(String name) {
        this.previousStateName = this.currentStateName;
        this.currentStateName = name;
    }

    /**
     * The state this execution transitioned <em>from</em> to reach {@link #getCurrentStateName()},
     * or {@code null} if no transition has fired on this instance.
     */
    public String getPreviousStateName() {
        return previousStateName;
    }

    /**
     * Rewind to {@code stateName}, discarding every step recorded for the state being abandoned.
     * <p>
     * The append-only rule is suspended here on purpose: a rewind means the abandoned state was
     * never really entered, so leaving its step records behind would make the next attempt skip
     * work that has to run again. Only the abandoned state's entries are dropped; earlier states
     * keep their history.
     *
     * @param stateName      the state to return to
     * @param abandonedState the state whose step records should be discarded
     */
    public void rewindTo(String stateName, String abandonedState) {
        steps.removeIf(s -> s.getStateName().equals(abandonedState));
        this.currentStateName = stateName;
        this.previousStateName = null;
    }

    public void setStatus(ExecutionStatus status) {
        this.status = status;
    }

    public void markFailed(String stateName, String subStepName) {
        this.failedStateName = stateName;
        this.failedSubStepName = subStepName;
        this.status = ExecutionStatus.FAILED;
    }

    public void markTerminated() {
        this.status = ExecutionStatus.TERMINATED;
        this.failedStateName = null;
        this.failedSubStepName = null;
    }

    public void clearFailure() {
        this.failedStateName = null;
        this.failedSubStepName = null;
        this.status = ExecutionStatus.RUNNING;
    }

    /**
     * True if this sub-step has a SUCCESS record in the log.
     * Used by SubStepRunner to skip already-completed steps on resume.
     */
    public boolean isSubStepCompleted(String stateName, String subStepName) {
        return steps.stream()
                .anyMatch(s -> s.getStateName().equals(stateName)
                        && s.getSubStepName().equals(subStepName)
                        && s.getResult().isSuccess());
    }

    /**
     * True if any sub-step of the given state has committed — succeeded outright, or been
     * skipped on a resume because it succeeded in an earlier attempt.
     * <p>
     * Both count as "work was done": a skipped step is the record of a side effect that already
     * happened. This is what makes a {@link io.hypercell.fsm.failure.FailureDisposition#REWIND}
     * unsafe, since rewinding would discard the evidence and re-run the step.
     */
    public boolean hasCommittedStepsFor(String stateName) {
        return steps.stream()
                .anyMatch(s -> s.getStateName().equals(stateName)
                        && (s.getResult().isSuccess() || s.getResult().isSkipped()));
    }

    /**
     * The most recent result for a sub-step, regardless of success/failure.
     * Used by the ResumePolicy to retrieve stored output for skipped steps.
     */
    public Optional<ActionResult> resultOf(String stateName, String subStepName) {
        for (int i = steps.size() - 1; i >= 0; i--) {
            StepRecord step = steps.get(i);
            if (step.getStateName().equals(stateName)
                    && step.getSubStepName().equals(subStepName)) {
                return Optional.of(step.getResult());
            }
        }
        return Optional.empty();
    }

    public String getExecutionId() {
        return executionId;
    }

    public String getCurrentStateName() {
        return currentStateName;
    }

    public String getFailedStateName() {
        return failedStateName;
    }

    public String getFailedSubStepName() {
        return failedSubStepName;
    }

    public String getLastTriggerEvent() {
        return lastTriggerEvent;
    }

    public ExecutionStatus getStatus() {
        return status;
    }

    public List<StepRecord> getSteps() {
        return Collections.unmodifiableList(steps);
    }
}
