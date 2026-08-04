package io.hypercell.fsm.listener;

import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.failure.FailureDisposition;

import java.time.Instant;

/**
 * Base class for all events emitted by a running state machine.
 * <p>
 * WHY A CLASS HIERARCHY RATHER THAN AN ENUM:
 * Each event type carries different data. An enum would force everything into
 * a single strongly-typed bag. The hierarchy lets the listener pattern-match
 * with instanceof and get strongly-typed data without casting Maps.
 * <p>
 * USAGE IN A LISTENER:
 * <pre>{@code
 * public void onEvent(MachineEvent<?> event) {
 *     if (event instanceof SubStepFailedEvent<?> e) {
 *         alertOps(e.getSubStepName(), e.getErrorMessage());
 *     }
 * }
 * }</pre>
 *
 * @param <C> the context type of the machine that emitted this event
 */
@SuppressWarnings("java:S2326")
public abstract class MachineEvent<C> {

    private final String executionId;
    private final String machineId;
    private final Instant occurredAt;

    protected MachineEvent(String executionId, String machineId) {
        this.executionId = executionId;
        this.machineId = machineId;
        this.occurredAt = Instant.now();
    }

    public String getExecutionId() {
        return executionId;
    }

    public String getMachineId() {
        return machineId;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }


    /**
     * Fired when the machine successfully moves from one state to another.
     * Emitted AFTER the transition action runs but BEFORE the new state's onEntry.
     */
    public static final class TransitionFiredEvent<C> extends MachineEvent<C> {
        private final String fromState;
        private final String toState;
        private final String event;

        public TransitionFiredEvent(String executionId, String machineId,
                                    String fromState, String toState, String event) {
            super(executionId, machineId);
            this.fromState = fromState;
            this.toState = toState;
            this.event = event;
        }

        public String getFromState() {
            return fromState;
        }

        public String getToState() {
            return toState;
        }

        public String getEvent() {
            return event;
        }
    }

    /**
     * Fired immediately after a state's onEntry hook completes.
     */
    public static final class StateEnteredEvent<C> extends MachineEvent<C> {
        private final String stateName;

        public StateEnteredEvent(String executionId, String machineId, String stateName) {
            super(executionId, machineId);
            this.stateName = stateName;
        }

        public String getStateName() {
            return stateName;
        }
    }

    /**
     * Fired immediately after a state's onExit hook completes.
     */
    public static final class StateExitedEvent<C> extends MachineEvent<C> {
        private final String stateName;

        public StateExitedEvent(String executionId, String machineId, String stateName) {
            super(executionId, machineId);
            this.stateName = stateName;
        }

        public String getStateName() {
            return stateName;
        }
    }

    /**
     * Fired when a sub-step executes and returns SUCCESS.
     */
    public static final class SubStepCompletedEvent<C> extends MachineEvent<C> {
        private final String stateName;
        private final String subStepName;
        private final ActionResult result;

        public SubStepCompletedEvent(String executionId, String machineId,
                                     String stateName, String subStepName, ActionResult result) {
            super(executionId, machineId);
            this.stateName = stateName;
            this.subStepName = subStepName;
            this.result = result;
        }

        public String getStateName() {
            return stateName;
        }

        public String getSubStepName() {
            return subStepName;
        }

        public ActionResult getResult() {
            return result;
        }
    }

    /**
     * Fired when a sub-step is skipped because it already completed in a previous run.
     * This only fires during a resume (proceed()) — never on fresh executions.
     */
    public static final class SubStepSkippedEvent<C> extends MachineEvent<C> {
        private final String stateName;
        private final String subStepName;

        public SubStepSkippedEvent(String executionId, String machineId,
                                   String stateName, String subStepName) {
            super(executionId, machineId);
            this.stateName = stateName;
            this.subStepName = subStepName;
        }

        public String getStateName() {
            return stateName;
        }

        public String getSubStepName() {
            return subStepName;
        }
    }

    /**
     * Fired when a sub-step returns FAILED or throws an exception.
     * The machine enters FAILED status after this event.
     */
    public static final class SubStepFailedEvent<C> extends MachineEvent<C> {
        private final String stateName;
        private final String subStepName;
        private final String errorMessage;
        private final String errorType;

        public SubStepFailedEvent(String executionId, String machineId,
                                  String stateName, String subStepName,
                                  String errorMessage, String errorType) {
            super(executionId, machineId);
            this.stateName = stateName;
            this.subStepName = subStepName;
            this.errorMessage = errorMessage;
            this.errorType = errorType;
        }

        public String getStateName() {
            return stateName;
        }

        public String getSubStepName() {
            return subStepName;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public String getErrorType() {
            return errorType;
        }
    }

    /**
     * Fired when the machine reaches a terminal state and all its sub-steps complete.
     * This is the happy-path end event.
     */
    public static final class MachineCompletedEvent<C> extends MachineEvent<C> {
        private final String finalStateName;

        public MachineCompletedEvent(String executionId, String machineId, String finalStateName) {
            super(executionId, machineId);
            this.finalStateName = finalStateName;
        }

        public String getFinalStateName() {
            return finalStateName;
        }
    }

    /**
     * Fired when the machine enters FAILED status — after the snapshot is saved
     * and before SubStepExecutionException is thrown to the caller.
     * Use this for alerting, dashboards, and audit trails.
     */
    public static final class MachineFailedEvent<C> extends MachineEvent<C> {
        private final String stateName;
        private final String subStepName;
        private final int attemptNumber;
        private final FailureDisposition disposition;

        public MachineFailedEvent(String executionId, String machineId,
                                  String stateName, String subStepName, int attemptNumber,
                                  FailureDisposition disposition) {
            super(executionId, machineId);
            this.stateName = stateName;
            this.subStepName = subStepName;
            this.attemptNumber = attemptNumber;
            this.disposition = disposition;
        }

        public String getStateName() {
            return stateName;
        }

        public String getSubStepName() {
            return subStepName;
        }

        public int getAttemptNumber() {
            return attemptNumber;
        }

        /**
         * How this failure was classified — the disposition that will be persisted on the
         * snapshot. Alerting can use it to distinguish "will retry itself" from "needs a human".
         * Never {@code null}.
         */
        public FailureDisposition getDisposition() {
            return disposition;
        }
    }

    /**
     * Fired when a failure was classified {@link FailureDisposition#REWIND} and the in-flight
     * transition was abandoned — the execution has been parked back at the state it came from
     * and is eligible for {@code trigger()} again.
     * <p>
     * Emitted instead of nothing extra: a {@link MachineFailedEvent} carrying
     * {@code REWIND} is emitted first, then this one once the rewind has been persisted.
     */
    public static final class MachineRewoundEvent<C> extends MachineEvent<C> {
        private final String failedStateName;
        private final String failedSubStepName;
        private final String rewoundToState;
        private final String triggerEvent;
        private final int attemptNumber;

        public MachineRewoundEvent(String executionId, String machineId,
                                   String failedStateName, String failedSubStepName,
                                   String rewoundToState, String triggerEvent, int attemptNumber) {
            super(executionId, machineId);
            this.failedStateName = failedStateName;
            this.failedSubStepName = failedSubStepName;
            this.rewoundToState = rewoundToState;
            this.triggerEvent = triggerEvent;
            this.attemptNumber = attemptNumber;
        }

        /**
         * The state whose sub-step failed — the one that was abandoned.
         */
        public String getFailedStateName() {
            return failedStateName;
        }

        public String getFailedSubStepName() {
            return failedSubStepName;
        }

        /**
         * The state the execution was parked at; re-firing {@link #getTriggerEvent()} from here
         * retries the whole transition.
         */
        public String getRewoundToState() {
            return rewoundToState;
        }

        /**
         * The event that was in flight and can now be re-fired; may be {@code null}.
         */
        public String getTriggerEvent() {
            return triggerEvent;
        }

        public int getAttemptNumber() {
            return attemptNumber;
        }
    }

    /**
     * Fired at the start of a resume (proceed()) before any sub-steps run.
     */
    public static final class MachineResumedEvent<C> extends MachineEvent<C> {
        private final String resumedAtState;
        private final String resumedAtSubStep;
        private final int attemptNumber;

        public MachineResumedEvent(String executionId, String machineId,
                                   String resumedAtState, String resumedAtSubStep,
                                   int attemptNumber) {
            super(executionId, machineId);
            this.resumedAtState = resumedAtState;
            this.resumedAtSubStep = resumedAtSubStep;
            this.attemptNumber = attemptNumber;
        }

        public String getResumedAtState() {
            return resumedAtState;
        }

        public String getResumedAtSubStep() {
            return resumedAtSubStep;
        }

        public int getAttemptNumber() {
            return attemptNumber;
        }
    }
}
