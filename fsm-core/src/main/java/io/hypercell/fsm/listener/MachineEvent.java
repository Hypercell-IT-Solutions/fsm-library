package io.hypercell.fsm.listener;

import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.failure.FailureContext;
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
 * THE CONTEXT:
 * Every event carries the machine's context via {@link #getContext()}. This is the <em>live</em>
 * domain object, not a copy, and listeners run synchronously on the execution thread — so a
 * listener that mutates it is mutating the object the next sub-step is about to read. Treat it as
 * read-only. The same caveat applies to {@link FailureContext#context()}.
 *
 * @param <C> the context type of the machine that emitted this event
 */
public abstract class MachineEvent<C> {

    private final String executionId;
    private final String machineId;
    private final C context;
    private final Instant occurredAt;

    protected MachineEvent(String executionId, String machineId, C context) {
        this.executionId = executionId;
        this.machineId = machineId;
        this.context = context;
        this.occurredAt = Instant.now();
    }

    public String getExecutionId() {
        return executionId;
    }

    public String getMachineId() {
        return machineId;
    }

    /**
     * The machine's context at the moment the event fired.
     * <p>
     * This is the live domain object shared with the executing machine — never mutate it from a
     * listener. May be {@code null} only if the machine was built with a null context.
     */
    public C getContext() {
        return context;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }


    /**
     * Fired the moment a machine instance is constructed, before any state is entered, any
     * sub-step runs, or any other event is published.
     * <p>
     * This is the hook for per-execution setup that must be in place before work begins — most
     * commonly stamping a correlation ID into the SLF4J MDC. {@link #getOrigin()} tells you
     * whether this is ordinary traffic or a recovery, which matters because the recovery sweeps
     * execute on the shared {@code recoveryExecutor} rather than on the caller's thread.
     * <p>
     * IMPORTANT — this event has no matching "destroyed" event, because an instance has no single
     * point of death. If you set a thread-local here you must clear it yourself, and on a pooled
     * recovery thread failing to do so leaks it into the next execution. Prefer
     * {@link io.hypercell.fsm.scope.ExecutionScopeProvider}, which the library closes in a
     * {@code finally} for you.
     */
    public static final class InstanceCreatedEvent<C> extends MachineEvent<C> {
        private final InstanceOrigin origin;
        private final String currentStateName;
        private final int attemptNumber;

        public InstanceCreatedEvent(String executionId, String machineId, C context,
                                    InstanceOrigin origin, String currentStateName,
                                    int attemptNumber) {
            super(executionId, machineId, context);
            this.origin = origin;
            this.currentStateName = currentStateName;
            this.attemptNumber = attemptNumber;
        }

        /**
         * Why this instance exists — fresh, or one of the three recovery paths. Never {@code null}.
         */
        public InstanceOrigin getOrigin() {
            return origin;
        }

        /**
         * The state the instance starts positioned at: the initial state for {@link
         * InstanceOrigin#NEW}, otherwise the state recovered from the snapshot.
         */
        public String getCurrentStateName() {
            return currentStateName;
        }

        /**
         * The execution's attempt number — {@code 1} for a fresh instance, otherwise the value
         * carried on the snapshot being recovered. Never reset by a rewind, so it also counts
         * re-triggers of a rewound execution.
         */
        public int getAttemptNumber() {
            return attemptNumber;
        }
    }

    /**
     * Fired when the machine successfully moves from one state to another.
     * Emitted AFTER the transition action runs but BEFORE the new state's onEntry.
     */
    public static final class TransitionFiredEvent<C> extends MachineEvent<C> {
        private final String fromState;
        private final String toState;
        private final String event;

        public TransitionFiredEvent(String executionId, String machineId, C context,
                                    String fromState, String toState, String event) {
            super(executionId, machineId, context);
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

        public StateEnteredEvent(String executionId, String machineId, C context, String stateName) {
            super(executionId, machineId, context);
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

        public StateExitedEvent(String executionId, String machineId, C context, String stateName) {
            super(executionId, machineId, context);
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

        public SubStepCompletedEvent(String executionId, String machineId, C context,
                                     String stateName, String subStepName, ActionResult result) {
            super(executionId, machineId, context);
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

        public SubStepSkippedEvent(String executionId, String machineId, C context,
                                   String stateName, String subStepName) {
            super(executionId, machineId, context);
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
     * <p>
     * This is the low-level "a step failed" signal, published before the failure is classified.
     * It deliberately carries no {@link FailureContext}: the disposition does not exist yet at
     * this point. For the classified view, listen for {@link MachineFailedEvent}.
     */
    public static final class SubStepFailedEvent<C> extends MachineEvent<C> {
        private final String stateName;
        private final String subStepName;
        private final ActionResult result;

        public SubStepFailedEvent(String executionId, String machineId, C context,
                                  String stateName, String subStepName, ActionResult result) {
            super(executionId, machineId, context);
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

        /**
         * The failing result, mirroring {@link SubStepCompletedEvent#getResult()}.
         */
        public ActionResult getResult() {
            return result;
        }

        public String getErrorMessage() {
            return result != null ? result.getErrorMessage() : null;
        }

        public String getErrorType() {
            return result != null ? result.getErrorType() : null;
        }

        /**
         * The original throwable, or {@code null} if the action returned
         * {@link ActionResult#failed(String)} rather than throwing.
         */
        public Throwable getError() {
            return result != null ? result.getCause() : null;
        }
    }

    /**
     * Fired when the machine reaches a terminal state and all its sub-steps complete.
     * This is the happy-path end event.
     */
    public static final class MachineCompletedEvent<C> extends MachineEvent<C> {
        private final String finalStateName;

        public MachineCompletedEvent(String executionId, String machineId, C context,
                                     String finalStateName) {
            super(executionId, machineId, context);
            this.finalStateName = finalStateName;
        }

        public String getFinalStateName() {
            return finalStateName;
        }
    }

    /**
     * Fired when the machine enters FAILED status — after the failure has been classified and
     * before SubStepExecutionException is thrown to the caller.
     * Use this for alerting, dashboards, and audit trails.
     */
    public static final class MachineFailedEvent<C> extends MachineEvent<C> {
        private final String stateName;
        private final String subStepName;
        private final int failureCount;
        private final FailureDisposition disposition;
        private final FailureContext<C> failureContext;

        public MachineFailedEvent(String executionId, String machineId, C context,
                                  String stateName, String subStepName, int failureCount,
                                  FailureDisposition disposition, FailureContext<C> failureContext) {
            super(executionId, machineId, context);
            this.stateName = stateName;
            this.subStepName = subStepName;
            this.failureCount = failureCount;
            this.disposition = disposition;
            this.failureContext = failureContext;
        }

        public String getStateName() {
            return stateName;
        }

        public String getSubStepName() {
            return subStepName;
        }

        /**
         * How many sub-steps have failed across this execution's whole recorded history.
         * <p>
         * This is NOT the retry attempt number — for that, read
         * {@code getFailureContext().attemptNumber()}. The two diverge whenever one attempt
         * fails more than one sub-step, or a retry succeeds past an earlier failure.
         */
        public int getFailureCount() {
            return failureCount;
        }

        /**
         * How this failure was classified — the disposition that will be persisted on the
         * snapshot. Alerting can use it to distinguish "will retry itself" from "needs a human".
         * Never {@code null}.
         */
        public FailureDisposition getDisposition() {
            return disposition;
        }

        /**
         * Everything the {@link io.hypercell.fsm.failure.FailurePolicy} chain saw when it decided
         * {@link #getDisposition()} — including the original throwable and the sub-step index.
         * Never {@code null}.
         */
        public FailureContext<C> getFailureContext() {
            return failureContext;
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
        private final FailureContext<C> failureContext;

        public MachineRewoundEvent(String executionId, String machineId, C context,
                                   String failedStateName, String failedSubStepName,
                                   String rewoundToState, String triggerEvent, int attemptNumber,
                                   FailureContext<C> failureContext) {
            super(executionId, machineId, context);
            this.failedStateName = failedStateName;
            this.failedSubStepName = failedSubStepName;
            this.rewoundToState = rewoundToState;
            this.triggerEvent = triggerEvent;
            this.attemptNumber = attemptNumber;
            this.failureContext = failureContext;
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

        /**
         * The bumped attempt number persisted on the {@code WAITING} snapshot — one higher than
         * the attempt that failed. Never reset, so repeated rewinds stay countable.
         */
        public int getAttemptNumber() {
            return attemptNumber;
        }

        /**
         * The failure that triggered the rewind, as the policy chain saw it. Never {@code null}.
         */
        public FailureContext<C> getFailureContext() {
            return failureContext;
        }
    }

    /**
     * Fired at the start of a resume (proceed()) before any sub-steps run.
     */
    public static final class MachineResumedEvent<C> extends MachineEvent<C> {
        private final String resumedAtState;
        private final String resumedAtSubStep;
        private final int attemptNumber;

        public MachineResumedEvent(String executionId, String machineId, C context,
                                   String resumedAtState, String resumedAtSubStep,
                                   int attemptNumber) {
            super(executionId, machineId, context);
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
