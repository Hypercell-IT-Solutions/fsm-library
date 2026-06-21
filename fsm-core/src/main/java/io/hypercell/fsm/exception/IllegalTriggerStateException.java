package io.hypercell.fsm.exception;

import io.hypercell.fsm.resume.SnapshotStatus;

/**
 * Thrown by {@link io.hypercell.fsm.manager.StateMachineManager#trigger(String, String)} when
 * the execution is in a state that does not allow a new event to be applied directly.
 * <p>
 * {@code trigger()} applies exactly one transition. Executions that require prior recovery
 * (FAILED → {@code proceed()}, RUNNING-interrupted → {@code resume()}) or are not yet
 * retryable (RETRY_SCHEDULED) must be handled explicitly before a new event is triggered.
 * <p>
 * <strong>Caller guidance by status:</strong>
 * <ul>
 *   <li>{@code FAILED} — call {@code proceed(executionId)} to retry the failed sub-steps,
 *       then call {@code trigger(executionId, event)} once the execution is {@code WAITING}.</li>
 *   <li>{@code RUNNING} (interrupted) — call {@code resume(executionId)} to complete the
 *       in-flight transition, then call {@code trigger(executionId, event)} once {@code WAITING}.</li>
 *   <li>{@code RETRY_SCHEDULED} — an automatic retry is pending; wait for it to complete (the
 *       execution will become {@code WAITING} or {@code FAILED}), then trigger.</li>
 * </ul>
 * <p>
 * Use {@link io.hypercell.fsm.manager.StateMachineManager#eligibilityOf(String)} as an advisory
 * pre-check to avoid calling {@code trigger()} on an ineligible execution.
 */
public class IllegalTriggerStateException extends StateMachineException {

    private final String executionId;
    private final String attemptedEvent;
    private final SnapshotStatus snapshotStatus;

    public IllegalTriggerStateException(String executionId, String attemptedEvent,
                                        SnapshotStatus snapshotStatus, String message) {
        super(message);
        this.executionId = executionId;
        this.attemptedEvent = attemptedEvent;
        this.snapshotStatus = snapshotStatus;
    }

    /**
     * The business entity ID that was passed to {@code trigger()}.
     */
    public String getExecutionId() {
        return executionId;
    }

    /**
     * The event that was attempted but could not be applied.
     */
    public String getAttemptedEvent() {
        return attemptedEvent;
    }

    /**
     * The persisted status of the execution at the time {@code trigger()} was called.
     */
    public SnapshotStatus getSnapshotStatus() {
        return snapshotStatus;
    }

    /**
     * Creates an exception for a FAILED execution.
     *
     * @param executionId   the business entity ID
     * @param event         the event that was attempted
     * @param currentState  the FSM state the execution is stuck in
     * @return the exception
     */
    public static IllegalTriggerStateException failed(String executionId, String event,
                                                      String currentState) {
        return new IllegalTriggerStateException(
                executionId, event, SnapshotStatus.FAILED,
                String.format(
                        "Cannot trigger '%s' on execution '%s': it is FAILED at state '%s'"
                                + " — call proceed() to recover before triggering.",
                        event, executionId, currentState));
    }

    /**
     * Creates an exception for a RUNNING (interrupted) execution.
     *
     * @param executionId the business entity ID
     * @param event       the event that was attempted
     * @return the exception
     */
    public static IllegalTriggerStateException running(String executionId, String event) {
        return new IllegalTriggerStateException(
                executionId, event, SnapshotStatus.RUNNING,
                String.format(
                        "Cannot trigger '%s' on execution '%s': it is RUNNING (interrupted)"
                                + " — call resume() to recover before triggering.",
                        event, executionId));
    }

    /**
     * Creates an exception for a RETRY_SCHEDULED execution.
     *
     * @param executionId the business entity ID
     * @param event       the event that was attempted
     * @return the exception
     */
    public static IllegalTriggerStateException retryScheduled(String executionId, String event) {
        return new IllegalTriggerStateException(
                executionId, event, SnapshotStatus.RETRY_SCHEDULED,
                String.format(
                        "Cannot trigger '%s' on execution '%s': an auto-retry is scheduled"
                                + " — wait for recovery before triggering.",
                        event, executionId));
    }
}
