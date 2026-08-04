package io.hypercell.fsm.execution;

import io.hypercell.fsm.core.*;
import io.hypercell.fsm.exception.InvalidEventException;
import io.hypercell.fsm.exception.StateMachineException;
import io.hypercell.fsm.exception.SubStepExecutionException;
import io.hypercell.fsm.failure.FailureContext;
import io.hypercell.fsm.failure.FailureDisposition;
import io.hypercell.fsm.failure.FailurePolicy;
import io.hypercell.fsm.listener.EventBus;
import io.hypercell.fsm.listener.MachineEvent;
import io.hypercell.fsm.resume.ExecutionSnapshot;
import io.hypercell.fsm.resume.SnapshotRepository;
import io.hypercell.fsm.resume.SnapshotStatus;
import io.hypercell.fsm.retry.RetryCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * The runtime engine of the state machine library.
 * <p>
 * WHAT CHANGED WITH EVENTS:
 * This class now holds an {@code EventBus<C>} and emits events at every meaningful
 * lifecycle point. The EventBus is always non-null (it's either a real bus
 * with listeners or an empty no-op bus) — no null checks needed here.
 * <p>
 * The event emission points are:
 * trigger()  → TransitionFired, StateExited, StateEntered, MachineCompleted/Failed
 * proceed()  → MachineResumed, StateEntered, MachineCompleted/Failed
 * SubSteps   → SubStepCompleted, SubStepSkipped, SubStepFailed  (emitted by SubStepRunner)
 *
 * @param <C> the context type flowing through the machine
 */
public class DefaultStateMachineInstance<C> implements StateMachineInstance<C> {
    private static final Logger log = LoggerFactory.getLogger(DefaultStateMachineInstance.class);

    private final String executionId;
    private final StateMachineDefinition<C> definition;
    private final SubStepRunner<C> subStepRunner;
    private final ExecutionRecord executionRecord;
    private final SnapshotRepository snapshotRepository;
    private final RetryCoordinator<C> retryCoordinator;
    private final EventBus<C> eventBus;
    private final int attemptNumber;
    private final C ctx;

    private StateDefinition<C> currentState;
    private ExecutionStatus executionStatus;

    DefaultStateMachineInstance(StateMachineDefinition<C> definition,
                                StateDefinition<C> initialState,
                                C ctx,
                                SnapshotRepository snapshotRepository,
                                RetryCoordinator<C> retryCoordinator,
                                EventBus<C> eventBus) {
        this(definition, initialState, ctx, UUID.randomUUID().toString(), snapshotRepository, retryCoordinator, eventBus);
    }

    DefaultStateMachineInstance(StateMachineDefinition<C> definition,
                                StateDefinition<C> initialState,
                                C ctx,
                                String executionId,
                                SnapshotRepository snapshotRepository,
                                RetryCoordinator<C> retryCoordinator,
                                EventBus<C> eventBus) {
        this.executionId = executionId;
        this.definition = definition;
        this.currentState = initialState;
        this.ctx = ctx;
        this.snapshotRepository = snapshotRepository;
        this.retryCoordinator = retryCoordinator;
        this.eventBus = eventBus != null ? eventBus : EventBus.empty();
        this.executionStatus = ExecutionStatus.RUNNING;
        this.executionRecord = new ExecutionRecord(executionId, initialState.name());
        this.attemptNumber = 1;

        this.subStepRunner = new SubStepRunner<>(
                definition.resumePolicy(), this.eventBus, executionId, definition.id());

        runEntryHook(initialState);
        this.eventBus.publish(new MachineEvent.StateEnteredEvent<>(
                executionId, definition.id(), initialState.name()));

        if (!initialState.subSteps().isEmpty()) {
            saveCheckpoint();
            SubStepRunResult result = subStepRunner.run(
                    initialState, ctx, executionRecord, this::saveCheckpoint);
            if (result.isFailed()) {
                handleFailure(initialState, result, null);
                throw new SubStepExecutionException(
                        initialState.name(), result.getFailedSubStepName(), result.getError());
            }
        }

        checkTerminal(initialState);

        if (executionStatus == ExecutionStatus.RUNNING) {
            saveAtRestCheckpoint();
        }
    }

    DefaultStateMachineInstance(StateMachineDefinition<C> definition,
                                StateDefinition<C> failedState,
                                C ctx,
                                int attemptNumber,
                                ExecutionRecord hydratedRecord,
                                ExecutionStatus initialStatus,
                                SnapshotRepository snapshotRepository,
                                RetryCoordinator<C> retryCoordinator,
                                EventBus<C> eventBus) {
        this.executionId = hydratedRecord.getExecutionId();
        this.definition = definition;
        this.currentState = failedState;
        this.ctx = ctx;
        this.executionRecord = hydratedRecord;
        this.snapshotRepository = snapshotRepository;
        this.retryCoordinator = retryCoordinator;
        this.eventBus = eventBus != null ? eventBus : EventBus.empty();
        this.executionStatus = initialStatus;
        this.attemptNumber = attemptNumber;

        this.subStepRunner = new SubStepRunner<>(
                definition.resumePolicy(), this.eventBus, executionId, definition.id());
    }

    @Override
    public StateDefinition<C> trigger(String event) {
        if (executionStatus != ExecutionStatus.RUNNING) {
            throw new InvalidEventException(String.format(
                    "Cannot trigger '%s' — machine is %s. Call proceed() if FAILED.",
                    event, executionStatus));
        }

        TransitionDefinition<C> transition = definition.transitionsFrom(currentState.name())
                .stream()
                .filter(t -> t.event().equals(event))
                .filter(t -> t.guard().map(g -> g.evaluate(ctx)).orElse(true))
                .findFirst()
                .orElseThrow(() -> new InvalidEventException(event, currentState.name()));

        executionRecord.setLastTriggerEvent(event);

        String fromState = currentState.name();
        runExitHook(currentState);
        eventBus.publish(new MachineEvent.StateExitedEvent<>(
                executionId, definition.id(), fromState));

        transition.action().ifPresent(action -> {
            try {
                ActionResult r = action.execute(ctx);
                if (r != null && r.isFailed()) {
                    throw new StateMachineException("Transition action failed: " + r.getErrorMessage());
                }
            } catch (StateMachineException e) {
                throw e;
            } catch (Exception e) {
                throw new StateMachineException("Transition action threw: " + e.getMessage(), e);
            }
        });

        StateDefinition<C> nextState = definition.stateByName(transition.targetState());
        currentState = nextState;
        executionRecord.moveTo(nextState.name());

        eventBus.publish(new MachineEvent.TransitionFiredEvent<>(
                executionId, definition.id(), fromState, nextState.name(), event));

        runEntryHook(nextState);
        eventBus.publish(new MachineEvent.StateEnteredEvent<>(
                executionId, definition.id(), nextState.name()));

        if (!nextState.subSteps().isEmpty()) {
            saveCheckpoint();
            SubStepRunResult runResult = subStepRunner.run(
                    nextState, ctx, executionRecord, this::saveCheckpoint);
            if (runResult.isFailed()) {
                handleFailure(nextState, runResult, event);
                throw new SubStepExecutionException(
                        nextState.name(), runResult.getFailedSubStepName(), runResult.getError());
            }
        }

        checkTerminal(nextState);

        if (executionStatus == ExecutionStatus.RUNNING) {
            saveAtRestCheckpoint();
        }

        return nextState;
    }

    @Override
    public StateDefinition<C> proceed() {
        if (executionStatus != ExecutionStatus.FAILED) {
            throw new InvalidEventException(
                    "proceed() can only be called when status is FAILED. Current: " + executionStatus);
        }

        eventBus.publish(new MachineEvent.MachineResumedEvent<>(
                executionId, definition.id(),
                executionRecord.getFailedStateName(),
                executionRecord.getFailedSubStepName(),
                executionRecord.getSteps().size()));

        executionRecord.clearFailure();
        executionStatus = ExecutionStatus.RUNNING;

        return runRemainingSubSteps(executionRecord.getLastTriggerEvent());
    }

    /**
     * Resume an interrupted {@code RUNNING} execution by completing the remaining
     * sub-steps of the current state, skipping those that have already been checkpointed.
     * <p>
     * This is distinct from {@link #proceed()}: it does <em>not</em> require {@code FAILED}
     * status, does not emit a {@code MachineResumedEvent}, and does not re-run the transition
     * action or entry/exit hooks — those already completed before the crash. It simply picks up
     * sub-step execution from where it was interrupted.
     *
     * @return the current state after sub-steps complete (may be RUNNING or COMPLETED)
     * @throws InvalidEventException     if the status is not {@code RUNNING}
     * @throws SubStepExecutionException if a remaining sub-step fails
     */
    @Override
    public StateDefinition<C> resume() {
        if (executionStatus != ExecutionStatus.RUNNING) {
            throw new InvalidEventException(
                    "resume() can only be called when status is RUNNING. Current: " + executionStatus);
        }
        return runRemainingSubSteps(executionRecord.getLastTriggerEvent());
    }

    /**
     * Shared logic: run (or skip) the sub-steps of {@link #currentState}, then
     * {@link #checkTerminal} and {@link #saveCheckpoint}.
     * Used by both {@link #proceed()} and {@link #resume()}.
     *
     * @param pendingEvent the event in-flight at the time of a failure, or {@code null}
     * @return the current state after all sub-steps have been processed
     */
    private StateDefinition<C> runRemainingSubSteps(String pendingEvent) {
        if (!currentState.subSteps().isEmpty()) {
            StateDefinition<C> state = currentState;
            SubStepRunResult runResult = subStepRunner.run(
                    state, ctx, executionRecord, this::saveCheckpoint);
            if (runResult.isFailed()) {
                handleFailure(state, runResult, pendingEvent);
                throw new SubStepExecutionException(
                        state.name(), runResult.getFailedSubStepName(), runResult.getError());
            }
        }

        checkTerminal(currentState);

        if (executionStatus == ExecutionStatus.RUNNING) {
            saveAtRestCheckpoint();
        }

        return currentState;
    }

    @Override
    public ExecutionSnapshot takeSnapshot(String pendingEvent) {
        return ExecutionSnapshot.fromRecord(executionRecord, pendingEvent, definition.id(), attemptNumber,
                executionRecord.getSteps().stream()
                        .filter(s -> s.getResult().isSuccess())
                        .collect(Collectors.toMap(
                                StepRecord::compositeKey,
                                StepRecord::getResult,
                                (existing, replacement) -> replacement)));
    }

    @Override
    public ExecutionSnapshot takeCheckpoint() {
        return ExecutionSnapshot.checkpoint(executionRecord, definition.id());
    }

    /**
     * Per-sub-step checkpoint — called by the {@code onStepCommitted} callback inside
     * {@link SubStepRunner} after each successfully executed sub-step. Persists status
     * {@code RUNNING} (the default from {@link ExecutionSnapshot#checkpoint}) so the startup
     * sweep can identify interrupted executions via a cheap indexed query.
     */
    private void saveCheckpoint() {
        if (snapshotRepository != null) {
            snapshotRepository.save(executionId, takeCheckpoint());
        }
    }

    /**
     * End-of-transition at-rest save — called after all sub-steps of a non-terminal state
     * have completed. Persists status {@code WAITING} to signal that the machine is parked
     * awaiting the next event (not interrupted). This differentiates at-rest executions from
     * genuinely interrupted ones in the startup sweep.
     */
    private void saveAtRestCheckpoint() {
        if (snapshotRepository != null) {
            snapshotRepository.save(executionId, takeCheckpoint().withStatus(SnapshotStatus.WAITING));
        }
    }

    private void checkTerminal(StateDefinition<C> state) {
        if (state.isTerminal()) {
            executionRecord.markTerminated();
            executionStatus = ExecutionStatus.TERMINATED;
            if (snapshotRepository != null) {
                snapshotRepository.save(executionId, takeCheckpoint().withStatus(SnapshotStatus.TERMINATED));
            }
            eventBus.publish(new MachineEvent.MachineCompletedEvent<>(
                    executionId, definition.id(), state.name()));
        }
    }

    /**
     * Record a sub-step failure and act on how it was classified.
     * <p>
     * The {@link FailurePolicy} chain runs first, because the resolved
     * {@link FailureDisposition} decides everything that follows: what status is persisted,
     * where the execution ends up, and whether the retry machinery is engaged at all. The
     * caller throws {@link SubStepExecutionException} afterwards in every case — the disposition
     * changes what is durable, not what the synchronous caller sees.
     *
     * @param state        the state whose sub-step failed
     * @param runResult    the failing run result, carrying the step name, index, and error
     * @param pendingEvent the event in flight, or {@code null}
     */
    private void handleFailure(StateDefinition<C> state, SubStepRunResult runResult, String pendingEvent) {
        String stateName = state.name();
        String subStepName = runResult.getFailedSubStepName();
        String sourceStateName = executionRecord.getPreviousStateName();

        FailureDisposition disposition = resolveDisposition(
                state, runResult, pendingEvent, sourceStateName);

        if (disposition == FailureDisposition.REWIND
                && !canRewind(stateName, sourceStateName)) {
            log.warn("[FSM] Execution '{}' requested REWIND at '{}/{}' but it is not safe "
                            + "(sourceState={}, committedSubSteps={}); falling back to MANUAL",
                    executionId, stateName, subStepName, sourceStateName,
                    executionRecord.hasCommittedStepsFor(stateName));
            disposition = FailureDisposition.MANUAL;
        }

        executionStatus = ExecutionStatus.FAILED;
        executionRecord.markFailed(stateName, subStepName);

        long failCount = executionRecord.getSteps().stream()
                .filter(s -> s.getResult().isFailed()).count();

        eventBus.publish(new MachineEvent.MachineFailedEvent<>(
                executionId, definition.id(), stateName, subStepName, (int) failCount,
                disposition));

        if (disposition == FailureDisposition.REWIND) {
            rewindTo(sourceStateName, stateName, subStepName, runResult, pendingEvent);
            return;
        }

        if (snapshotRepository != null) {
            snapshotRepository.save(executionId,
                    takeSnapshot(pendingEvent).withFailureDisposition(disposition));
        }

        if (retryCoordinator != null && disposition == FailureDisposition.RETRY) {
            retryCoordinator.onFailure(this, pendingEvent, runResult.getError());
        }
    }

    /**
     * Walk the policy chain — sub-step, then state, then machine — and return the first
     * disposition anyone commits to, defaulting to {@link FailureDisposition#RETRY}.
     * <p>
     * A policy that throws is treated as "no opinion" and logged: a broken policy should not
     * turn a recoverable failure into an unhandled exception on the execution thread.
     */
    private FailureDisposition resolveDisposition(StateDefinition<C> state,
                                                  SubStepRunResult runResult,
                                                  String pendingEvent,
                                                  String sourceStateName) {
        FailureContext<C> failure = buildFailureContext(
                state, runResult, pendingEvent, sourceStateName);

        int index = runResult.getFailedSubStepIndex();
        FailurePolicy<C> subStepPolicy = index >= 0 && index < state.subSteps().size()
                ? state.subSteps().get(index).failurePolicy()
                : null;

        FailureDisposition decided = decide(subStepPolicy, failure, "sub-step");
        if (decided == null) decided = decide(state.failurePolicy().orElse(null), failure, "state");
        if (decided == null) decided = decide(definition.failurePolicy(), failure, "machine");

        return decided != null ? decided : FailureDisposition.RETRY;
    }

    private FailureDisposition decide(FailurePolicy<C> policy, FailureContext<C> failure,
                                      String level) {
        if (policy == null) return null;
        try {
            return policy.decide(failure);
        } catch (Exception e) {
            log.warn("[FSM] {}-level FailurePolicy threw for execution '{}'; treating as "
                    + "no opinion", level, executionId, e);
            return null;
        }
    }

    private FailureContext<C> buildFailureContext(StateDefinition<C> state,
                                                  SubStepRunResult runResult,
                                                  String pendingEvent,
                                                  String sourceStateName) {
        return FailureContext.<C>builder()
                .executionId(executionId)
                .machineDefinitionId(definition.id())
                .stateName(state.name())
                .sourceStateName(sourceStateName)
                .triggerEvent(pendingEvent != null ? pendingEvent : executionRecord.getLastTriggerEvent())
                .subStepName(runResult.getFailedSubStepName())
                .subStepIndex(runResult.getFailedSubStepIndex())
                .firstSubStep(runResult.getFailedSubStepIndex() == 0)
                .committedSubSteps(executionRecord.hasCommittedStepsFor(state.name()))
                .attemptNumber(attemptNumber)
                .result(runResult.getResult())
                .error(runResult.getError())
                .context(ctx)
                .build();
    }

    /**
     * A rewind is only sound when the abandoned state committed nothing and there is somewhere
     * to go back to. Without a source state the execution has nowhere to park (the failure
     * happened on the initial state, or during a resume that fired no transition); with
     * committed sub-steps, rewinding would discard the record of side effects that really did
     * happen and re-run them on the next attempt.
     */
    private boolean canRewind(String stateName, String sourceStateName) {
        return sourceStateName != null && !executionRecord.hasCommittedStepsFor(stateName);
    }

    /**
     * Abandon the in-flight transition: drop the failed state's step records, reposition at the
     * source state, and persist a {@code WAITING} snapshot there so the caller can re-fire the
     * event.
     * <p>
     * The failure details ride along on the {@code WAITING} snapshot — failed state, failed
     * sub-step, error message and type, and the bumped attempt number — so a rewound execution
     * is still diagnosable and repeated rewinds are still countable. {@code attemptNumber} is
     * never reset.
     */
    private void rewindTo(String sourceStateName, String failedStateName, String failedSubStepName,
                          SubStepRunResult runResult, String pendingEvent) {
        executionRecord.rewindTo(sourceStateName, failedStateName);
        currentState = definition.stateByName(sourceStateName);

        if (snapshotRepository != null) {
            ActionResult failure = runResult.getResult();
            Instant now = Instant.now();
            snapshotRepository.save(executionId, new ExecutionSnapshot.Builder()
                    .executionId(executionId)
                    .machineDefinitionId(definition.id())
                    .currentStateName(sourceStateName)
                    .failedStateName(failedStateName)
                    .failedSubStepName(failedSubStepName)
                    .lastTriggerEvent(pendingEvent)
                    .completedSubStepResults(executionRecord.getSteps().stream()
                            .filter(s -> s.getResult().isSuccess())
                            .collect(Collectors.toMap(
                                    StepRecord::compositeKey,
                                    StepRecord::getResult,
                                    (existing, replacement) -> replacement)))
                    .attemptNumber(attemptNumber + 1)
                    .lastFailedAt(now)
                    .lastErrorMessage(failure != null ? failure.getErrorMessage() : null)
                    .lastErrorType(failure != null ? failure.getErrorType() : null)
                    .failureDisposition(FailureDisposition.REWIND)
                    .status(SnapshotStatus.WAITING)
                    .capturedAt(now)
                    .build());
        }

        eventBus.publish(new MachineEvent.MachineRewoundEvent<>(
                executionId, definition.id(), failedStateName, failedSubStepName,
                sourceStateName, pendingEvent, attemptNumber + 1));
    }

    private void runEntryHook(StateDefinition<C> state) {
        state.hook().ifPresent(h -> {
            try {
                h.onEntry(ctx);
            } catch (Exception e) {
                throw new StateMachineException(
                        "onEntry hook failed for '" + state.name() + "': " + e.getMessage(), e);
            }
        });
    }

    private void runExitHook(StateDefinition<C> state) {
        state.hook().ifPresent(h -> {
            try {
                h.onExit(ctx);
            } catch (Exception e) {
                throw new StateMachineException(
                        "onExit hook failed for '" + state.name() + "': " + e.getMessage(), e);
            }
        });
    }

    @Override
    public String executionId() {
        return executionId;
    }

    @Override
    public StateDefinition<C> currentState() {
        return currentState;
    }

    @Override
    public ExecutionStatus status() {
        return executionStatus;
    }

    @Override
    public ExecutionRecord executionRecord() {
        return executionRecord;
    }

    @Override
    public C context() {
        return ctx;
    }

    @Override
    public boolean isInInitialState() {
        return definition.isInitialState(currentState.name());
    }

    @Override
    public boolean isInTerminalState() {
        return currentState.isTerminal();
    }

    @Override
    public boolean isTerminated() {
        return executionStatus == ExecutionStatus.TERMINATED;
    }

    @Override
    public boolean isFailed() {
        return executionStatus == ExecutionStatus.FAILED;
    }

    @Override
    public boolean isRunning() {
        return executionStatus == ExecutionStatus.RUNNING;
    }
}
