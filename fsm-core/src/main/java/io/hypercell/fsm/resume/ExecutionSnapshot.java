package io.hypercell.fsm.resume;

import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.execution.ExecutionRecord;
import io.hypercell.fsm.execution.StepRecord;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A serializable point-in-time capture of a failed execution.
 * <p>
 * This is what gets persisted to the SnapshotRepository when a machine fails.
 * On resume, the DefaultStateMachineDefinition reads it to reconstruct an
 * instance positioned at the failure point.
 * <p>
 * KEY DESIGN DECISION — WHAT TO STORE:
 * We store only the COMPLETED sub-step results of the current state (not the failed ones), because:
 * - Completed steps are what we want to SKIP on resume
 * - The failed step will be re-executed, so we don't need its old result
 * - When resuming, you only care about the current state's completed steps
 * - Prior states are never revisited, so their step history is not needed for recovery
 * <p>
 * SERIALIZATION:
 * This class is designed to be easily serialized to JSON or any other format.
 * All fields are either primitives, Strings, Instants, or Maps of those types.
 * The SnapshotRepository implementation handles the actual serialization.
 * <p>
 * The composite key format for completedSubStepResults is {@code "stateName::subStepName"}
 * (e.g., {@code "PROCESSING::validateOrder"}, {@code "PROCESSING::processPayment"}).
 * Using a composite key prevents collisions when steps from multiple states are recorded.
 */
public class ExecutionSnapshot {

    private final String executionId;
    private final String machineDefinitionId;
    private final String currentStateName;
    private final String failedStateName;
    private final String failedSubStepName;
    private final String lastTriggerEvent;
    private final Map<String, ActionResult> completedSubStepResults;
    private final int attemptNumber;
    private final Instant lastFailedAt;
    private final Instant scheduledRetryAt;
    private final String lastErrorMessage;
    private SnapshotStatus status;
    private final Instant capturedAt;

    private ExecutionSnapshot(Builder builder) {
        this.executionId = builder.executionId;
        this.machineDefinitionId = builder.machineDefinitionId;
        this.currentStateName = builder.currentStateName;
        this.failedStateName = builder.failedStateName;
        this.failedSubStepName = builder.failedSubStepName;
        this.lastTriggerEvent = builder.lastTriggerEvent;
        this.completedSubStepResults = Collections.unmodifiableMap(
                new HashMap<>(builder.completedSubStepResults));
        this.attemptNumber = builder.attemptNumber;
        this.lastFailedAt = builder.lastFailedAt;
        this.scheduledRetryAt = builder.scheduledRetryAt;
        this.lastErrorMessage = builder.lastErrorMessage;
        this.status = builder.status;
        this.capturedAt = builder.capturedAt;
    }

    /**
     * Factory method called by DefaultStateMachineInstance.takeSnapshot().
     * Converts the live ExecutionRecord into a serializable snapshot.
     */
    public static ExecutionSnapshot fromRecord(
            ExecutionRecord executionRecord,
            String pendingEvent,
            String machineDefinitionId,
            int attemptNumber,
            Map<String, ActionResult> completedSubStepResults) {
        return new Builder()
                .executionId(executionRecord.getExecutionId())
                .machineDefinitionId(machineDefinitionId)
                .currentStateName(executionRecord.getFailedStateName())
                .failedStateName(executionRecord.getFailedStateName())
                .failedSubStepName(executionRecord.getFailedSubStepName())
                .lastTriggerEvent(pendingEvent)
                .completedSubStepResults(completedSubStepResults)
                .attemptNumber(attemptNumber)
                .lastFailedAt(Instant.now())
                .lastErrorMessage(null)
                .status(SnapshotStatus.FAILED)
                .capturedAt(Instant.now())
                .build();
    }

    /**
     * Create a {@code RUNNING} checkpoint snapshot from a live execution record.
     * Used internally to save progress after a successful transition so the machine
     * can be reconstituted if the process restarts.
     */
    public static ExecutionSnapshot checkpoint(ExecutionRecord executionRecord, String machineDefinitionId) {
        Map<String, ActionResult> completed = executionRecord.getSteps().stream()
                .filter(s -> s.getResult().isSuccess())
                .collect(Collectors.toMap(
                        StepRecord::compositeKey,
                        StepRecord::getResult,
                        (a, b) -> b
                ));

        return new Builder()
                .executionId(executionRecord.getExecutionId())
                .machineDefinitionId(machineDefinitionId)
                .currentStateName(executionRecord.getCurrentStateName())
                .lastTriggerEvent(executionRecord.getLastTriggerEvent())
                .completedSubStepResults(completed)
                .status(SnapshotStatus.RUNNING)
                .capturedAt(Instant.now())
                .build();
    }

    /**
     * Return a copy of this snapshot with the attempt number incremented to {@code newAttempt}.
     */
    public ExecutionSnapshot withAttemptNumber(int newAttempt) {
        return new Builder(this).attemptNumber(newAttempt).build();
    }

    /**
     * Return a copy of this snapshot with a different {@link SnapshotStatus}.
     */
    public ExecutionSnapshot withStatus(SnapshotStatus newStatus) {
        return new Builder(this).status(newStatus).build();
    }

    /**
     * Return a copy of this snapshot with {@code scheduledRetryAt} set and
     * status automatically changed to {@code RETRY_SCHEDULED}.
     */
    public ExecutionSnapshot withScheduledRetryAt(Instant retryAt) {
        return new Builder(this).scheduledRetryAt(retryAt)
                .status(SnapshotStatus.RETRY_SCHEDULED).build();
    }

    /**
     * Return a copy of this snapshot with a different machine definition ID.
     */
    public ExecutionSnapshot withMachineDefinitionId(String id) {
        return new Builder(this).machineDefinitionId(id).build();
    }

    /**
     * {@code true} when status is {@code RUNNING} (execution is actively processing sub-steps).
     */
    public boolean isRunning() {
        return status == SnapshotStatus.RUNNING;
    }

    /**
     * {@code true} when status is {@code FAILED} (waiting for manual or scheduled retry).
     */
    public boolean isFailed() {
        return status == SnapshotStatus.FAILED;
    }

    /**
     * {@code true} when status is {@code WAITING} — sub-steps completed; machine is parked
     * at a non-terminal state awaiting the next event.
     */
    public boolean isWaiting() {
        return status == SnapshotStatus.WAITING;
    }

    /**
     * {@code true} when status is {@code TERMINATED} — a terminal state was reached
     * successfully (snapshot retained to guard against re-triggering).
     */
    public boolean isTerminated() {
        return status == SnapshotStatus.TERMINATED;
    }

    /**
     * The business entity ID; used as the repository storage key.
     */
    public String getExecutionId() {
        return executionId;
    }

    /**
     * The {@link io.hypercell.fsm.core.StateMachineDefinition#id()} this snapshot belongs to.
     */
    public String getMachineDefinitionId() {
        return machineDefinitionId;
    }

    /**
     * The state the machine is positioned in (where resumption should start).
     */
    public String getCurrentStateName() {
        return currentStateName;
    }

    /**
     * The state containing the failed sub-step; {@code null} for {@code RUNNING} checkpoints.
     */
    public String getFailedStateName() {
        return failedStateName;
    }

    /**
     * The sub-step that failed; {@code null} for {@code RUNNING} checkpoints.
     */
    public String getFailedSubStepName() {
        return failedSubStepName;
    }

    /**
     * The event that was being processed when this snapshot was taken; may be {@code null}.
     */
    public String getLastTriggerEvent() {
        return lastTriggerEvent;
    }

    /**
     * Completed sub-step results, keyed as {@code "stateName::subStepName"}.
     * These are the steps that will be skipped on resume. Unmodifiable.
     */
    public Map<String, ActionResult> getCompletedSubStepResults() {
        return completedSubStepResults;
    }

    /**
     * How many execution attempts have occurred so far (starts at 1 on first failure,
     * increments with each retry failure). Passed to {@link io.hypercell.fsm.retry.RetryPolicy#shouldRetry}.
     */
    public int getAttemptNumber() {
        return attemptNumber;
    }

    /**
     * When the most recent failure occurred; may be {@code null} for {@code RUNNING} checkpoints.
     */
    public Instant getLastFailedAt() {
        return lastFailedAt;
    }

    /**
     * When the next auto-retry is scheduled to fire; non-null only when status is
     * {@code RETRY_SCHEDULED}. Used by {@code recoverPendingRetries()} to calculate
     * the remaining delay after a process restart.
     */
    public Instant getScheduledRetryAt() {
        return scheduledRetryAt;
    }

    /**
     * The error message from the most recent failure; {@code null} for {@code RUNNING} checkpoints.
     */
    public String getLastErrorMessage() {
        return lastErrorMessage;
    }

    /**
     * The current persistence status of this snapshot.
     */
    public SnapshotStatus getStatus() {
        return status;
    }

    /**
     * When this snapshot object was created (wall-clock time).
     */
    public Instant getCapturedAt() {
        return capturedAt;
    }

    /**
     * Mutate the status in place.
     * <p>
     * <strong>For internal use only.</strong> Prefer the immutable
     * {@link #withStatus(SnapshotStatus)} copy-with method in all other contexts.
     * This mutating setter exists for the rare case where the repository needs to
     * update status on an already-loaded instance without creating a new object.
     */
    public void setStatus(SnapshotStatus status) {
        this.status = status;
    }

    /**
     * Check whether a specific sub-step was completed and recorded in this snapshot.
     * Key format: {@code "stateName::subStepName"} (e.g., {@code "PROCESSING::validateOrder"}).
     */
    public boolean isSubStepCompleted(String compositeKey) {
        return completedSubStepResults.containsKey(compositeKey);
    }

    /**
     * Builder for {@link ExecutionSnapshot}. Used internally by the library and by
     * custom {@link SnapshotRepository} implementations that need to reconstruct
     * a snapshot from a raw storage format (e.g. database row, Redis hash).
     */
    public static class Builder {
        String executionId = "";
        String machineDefinitionId = "";
        String currentStateName;
        String failedStateName;
        String failedSubStepName;
        String lastTriggerEvent;
        Map<String, ActionResult> completedSubStepResults = new HashMap<>();
        int attemptNumber = 1;
        Instant lastFailedAt = Instant.now();
        Instant scheduledRetryAt;
        String lastErrorMessage;
        SnapshotStatus status = SnapshotStatus.FAILED;
        Instant capturedAt = Instant.now();

        public Builder() {
        }

        Builder(ExecutionSnapshot source) {
            this.executionId = source.executionId;
            this.machineDefinitionId = source.machineDefinitionId;
            this.currentStateName = source.currentStateName;
            this.failedStateName = source.failedStateName;
            this.failedSubStepName = source.failedSubStepName;
            this.lastTriggerEvent = source.lastTriggerEvent;
            this.completedSubStepResults = new HashMap<>(source.completedSubStepResults);
            this.attemptNumber = source.attemptNumber;
            this.lastFailedAt = source.lastFailedAt;
            this.scheduledRetryAt = source.scheduledRetryAt;
            this.lastErrorMessage = source.lastErrorMessage;
            this.status = source.status;
            this.capturedAt = source.capturedAt;
        }

        public Builder executionId(String v) {
            executionId = v;
            return this;
        }

        public Builder machineDefinitionId(String v) {
            machineDefinitionId = v;
            return this;
        }

        public Builder currentStateName(String v) {
            currentStateName = v;
            return this;
        }

        public Builder failedStateName(String v) {
            failedStateName = v;
            return this;
        }

        public Builder failedSubStepName(String v) {
            failedSubStepName = v;
            return this;
        }

        public Builder lastTriggerEvent(String v) {
            lastTriggerEvent = v;
            return this;
        }

        public Builder completedSubStepResults(Map<String, ActionResult> v) {
            completedSubStepResults = v;
            return this;
        }

        public Builder attemptNumber(int v) {
            attemptNumber = v;
            return this;
        }

        public Builder lastFailedAt(Instant v) {
            lastFailedAt = v;
            return this;
        }

        public Builder scheduledRetryAt(Instant v) {
            scheduledRetryAt = v;
            return this;
        }

        public Builder lastErrorMessage(String v) {
            lastErrorMessage = v;
            return this;
        }

        public Builder status(SnapshotStatus v) {
            status = v;
            return this;
        }

        public Builder capturedAt(Instant v) {
            capturedAt = v;
            return this;
        }

        public ExecutionSnapshot build() {
            return new ExecutionSnapshot(this);
        }
    }
}
