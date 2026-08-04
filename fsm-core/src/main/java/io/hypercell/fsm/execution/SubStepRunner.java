package io.hypercell.fsm.execution;

import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.core.StateDefinition;
import io.hypercell.fsm.core.SubStepDefinition;
import io.hypercell.fsm.listener.EventBus;
import io.hypercell.fsm.listener.MachineEvent;
import io.hypercell.fsm.resume.ResumePolicy;

import java.util.List;

/**
 * Executes the sub-steps of a state sequentially.
 * <p>
 * This class is the heart of the retry/resume mechanism. Before executing each
 * sub-step it checks the ResumePolicy — if a step is already recorded as complete
 * it is skipped (and a SubStepSkippedEvent is emitted). Otherwise, the step runs,
 * its result is recorded, and the appropriate event is emitted.
 * <p>
 * The EventBus is injected so this class never knows whether there are zero or
 * a hundred listeners — it always calls publish() and the bus handles dispatch.
 *
 * @param <C> the context type flowing through the machine
 */
public class SubStepRunner<C> {

    private final ResumePolicy<C> resumePolicy;
    private final EventBus<C> eventBus;
    private final String executionId;
    private final String machineId;

    public SubStepRunner(ResumePolicy<C> resumePolicy, EventBus<C> eventBus,
                         String executionId, String machineId) {
        this.resumePolicy = resumePolicy;
        this.eventBus = eventBus;
        this.executionId = executionId;
        this.machineId = machineId;
    }

    /**
     * Run all sub-steps of the given state, skipping completed ones via the resume policy.
     * <p>
     * After each freshly executed sub-step succeeds, {@code onStepCommitted} is invoked so the
     * caller can persist a mid-transition checkpoint. The callback is <em>not</em> invoked for
     * skipped steps (already durable) or failed steps (the caller handles failure separately).
     *
     * @param state           the state whose sub-steps are to be executed
     * @param ctx             the mutable context passed to each sub-step action
     * @param executionRecord the live execution log; updated in-place for each step
     * @param onStepCommitted called once after each freshly completed sub-step; never {@code null}
     * @return a result indicating overall completion or the first failure
     */
    public SubStepRunResult run(StateDefinition<C> state, C ctx, ExecutionRecord executionRecord,
                                Runnable onStepCommitted) {

        List<SubStepDefinition<C>> subSteps = state.subSteps();
        for (int index = 0; index < subSteps.size(); index++) {
            SubStepDefinition<C> subStep = subSteps.get(index);

            if (resumePolicy.shouldSkip(state, subStep, executionRecord)) {
                ActionResult stored = resumePolicy
                        .storedResult(state, subStep, executionRecord)
                        .orElse(ActionResult.skipped());
                executionRecord.recordStep(state.name(), subStep.name(), stored);

                eventBus.publish(new MachineEvent.SubStepSkippedEvent<>(
                        executionId, machineId, ctx, state.name(), subStep.name()));
                continue;
            }

            ActionResult result;
            try {
                result = subStep.action().execute(ctx);
                if (result == null) {
                    result = ActionResult.failed(
                            "Action returned null — use ActionResult.success() for void actions");
                }
            } catch (Exception e) {
                result = ActionResult.failed(e);
            }

            executionRecord.recordStep(state.name(), subStep.name(), result);

            if (result.isFailed()) {
                eventBus.publish(new MachineEvent.SubStepFailedEvent<>(
                        executionId, machineId, ctx,
                        state.name(), subStep.name(), result));

                return SubStepRunResult.failed(subStep.name(), index, causeOf(result), result);
            }

            onStepCommitted.run();

            eventBus.publish(new MachineEvent.SubStepCompletedEvent<>(
                    executionId, machineId, ctx, state.name(), subStep.name(), result));
        }

        return SubStepRunResult.completed();
    }

    /**
     * The throwable to propagate for a failed result.
     * <p>
     * When the sub-step threw, the original exception is carried through untouched — its type is
     * what {@link io.hypercell.fsm.failure.FailurePolicy} and
     * {@link io.hypercell.fsm.retry.RetryPolicy} branch on, and wrapping it here would erase that.
     * When the sub-step reported failure by returning {@code ActionResult.failed(message)} there
     * is no exception to carry, so a synthetic one stands in to keep the failure path uniform.
     */
    private static Throwable causeOf(ActionResult result) {
        Throwable cause = result.getCause();
        return cause != null ? cause : new RuntimeException(result.getErrorMessage());
    }
}
