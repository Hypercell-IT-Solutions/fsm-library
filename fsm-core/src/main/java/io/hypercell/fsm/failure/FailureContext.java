package io.hypercell.fsm.failure;

import io.hypercell.fsm.core.ActionResult;

/**
 * Everything a {@link FailurePolicy} needs to know about a sub-step failure.
 * <p>
 * Constructed by the runtime the moment a sub-step fails and handed to the resolved policy
 * chain. Immutable — a policy may read it freely but must not attempt to mutate the execution
 * through it. The one exception is {@link #context()}, which is the live domain object; policies
 * should treat it as read-only.
 *
 * @param <C> the context type flowing through the machine
 */
public final class FailureContext<C> {

    private final String executionId;
    private final String machineDefinitionId;
    private final String stateName;
    private final String sourceStateName;
    private final String triggerEvent;
    private final String subStepName;
    private final int subStepIndex;
    private final boolean firstSubStep;
    private final boolean committedSubSteps;
    private final int attemptNumber;
    private final ActionResult result;
    private final Throwable error;
    private final C context;

    private FailureContext(Builder<C> builder) {
        this.executionId = builder.executionId;
        this.machineDefinitionId = builder.machineDefinitionId;
        this.stateName = builder.stateName;
        this.sourceStateName = builder.sourceStateName;
        this.triggerEvent = builder.triggerEvent;
        this.subStepName = builder.subStepName;
        this.subStepIndex = builder.subStepIndex;
        this.firstSubStep = builder.firstSubStep;
        this.committedSubSteps = builder.committedSubSteps;
        this.attemptNumber = builder.attemptNumber;
        this.result = builder.result;
        this.error = builder.error;
        this.context = builder.context;
    }

    public static <C> Builder<C> builder() {
        return new Builder<>();
    }

    /**
     * The business entity ID of the failing execution.
     */
    public String executionId() {
        return executionId;
    }

    /**
     * The {@link io.hypercell.fsm.core.StateMachineDefinition#id()} this execution belongs to.
     */
    public String machineDefinitionId() {
        return machineDefinitionId;
    }

    /**
     * The state containing the sub-step that failed.
     */
    public String stateName() {
        return stateName;
    }

    /**
     * The state the machine transitioned <em>from</em> to reach {@link #stateName()}.
     * <p>
     * {@code null} when there is no source state to return to — the failure happened during
     * {@code initialize()} (the initial state has no predecessor) or during a {@code proceed()}
     * / {@code resume()} that did not fire a transition. {@link FailureDisposition#REWIND} is
     * not possible in that case and is downgraded to {@link FailureDisposition#MANUAL}.
     */
    public String sourceStateName() {
        return sourceStateName;
    }

    /**
     * The event that was in flight when the failure occurred; may be {@code null}.
     */
    public String triggerEvent() {
        return triggerEvent;
    }

    /**
     * The name of the sub-step that failed.
     */
    public String subStepName() {
        return subStepName;
    }

    /**
     * Zero-based position of the failed sub-step within its state's sub-step list.
     */
    public int subStepIndex() {
        return subStepIndex;
    }

    /**
     * {@code true} when the failed sub-step is the first one declared on the state.
     * <p>
     * Note that this is a purely positional check. It does not by itself mean nothing was
     * committed — see {@link #hasCommittedSubSteps()} for that.
     */
    public boolean isFirstSubStep() {
        return firstSubStep;
    }

    /**
     * {@code true} when at least one sub-step of {@link #stateName()} has already succeeded or
     * was skipped as already-complete from a prior attempt.
     * <p>
     * When this is {@code true}, side effects have been committed inside this state and a
     * {@link FailureDisposition#REWIND} would silently discard the record of them, so the
     * runtime downgrades the rewind to {@link FailureDisposition#MANUAL}.
     */
    public boolean hasCommittedSubSteps() {
        return committedSubSteps;
    }

    /**
     * How many execution attempts have occurred so far; starts at {@code 1}.
     */
    public int attemptNumber() {
        return attemptNumber;
    }

    /**
     * The failed {@link ActionResult}, carrying {@code errorMessage} and {@code errorType}.
     * Never {@code null}.
     */
    public ActionResult result() {
        return result;
    }

    /**
     * The original exception thrown by the sub-step, unwrapped and untouched.
     * <p>
     * {@code null} when the sub-step reported failure without throwing — that is, it returned
     * {@link ActionResult#failed(String)} or {@code null}. Use {@link #result()} in that case.
     */
    public Throwable error() {
        return error;
    }

    /**
     * The live domain context of the failing execution. Treat as read-only.
     */
    public C context() {
        return context;
    }

    @Override
    public String toString() {
        return "FailureContext{executionId='" + executionId + "', state='" + stateName
                + "', subStep='" + subStepName + "' (#" + subStepIndex + ")"
                + ", attempt=" + attemptNumber + ", error=" + result.getErrorMessage() + "}";
    }

    /**
     * Builder for {@link FailureContext}. Used by the runtime, and by tests that need to
     * exercise a {@link FailurePolicy} in isolation.
     *
     * @param <C> the context type flowing through the machine
     */
    public static final class Builder<C> {
        private String executionId;
        private String machineDefinitionId;
        private String stateName;
        private String sourceStateName;
        private String triggerEvent;
        private String subStepName;
        private int subStepIndex = -1;
        private boolean firstSubStep;
        private boolean committedSubSteps;
        private int attemptNumber = 1;
        private ActionResult result = ActionResult.failed("unspecified failure");
        private Throwable error;
        private C context;

        public Builder<C> executionId(String v) {
            executionId = v;
            return this;
        }

        public Builder<C> machineDefinitionId(String v) {
            machineDefinitionId = v;
            return this;
        }

        public Builder<C> stateName(String v) {
            stateName = v;
            return this;
        }

        public Builder<C> sourceStateName(String v) {
            sourceStateName = v;
            return this;
        }

        public Builder<C> triggerEvent(String v) {
            triggerEvent = v;
            return this;
        }

        public Builder<C> subStepName(String v) {
            subStepName = v;
            return this;
        }

        public Builder<C> subStepIndex(int v) {
            subStepIndex = v;
            return this;
        }

        public Builder<C> firstSubStep(boolean v) {
            firstSubStep = v;
            return this;
        }

        public Builder<C> committedSubSteps(boolean v) {
            committedSubSteps = v;
            return this;
        }

        public Builder<C> attemptNumber(int v) {
            attemptNumber = v;
            return this;
        }

        public Builder<C> result(ActionResult v) {
            result = v;
            return this;
        }

        public Builder<C> error(Throwable v) {
            error = v;
            return this;
        }

        public Builder<C> context(C v) {
            context = v;
            return this;
        }

        public FailureContext<C> build() {
            return new FailureContext<>(this);
        }
    }
}
