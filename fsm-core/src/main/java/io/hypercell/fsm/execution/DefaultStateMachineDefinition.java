package io.hypercell.fsm.execution;

import io.hypercell.fsm.core.*;
import io.hypercell.fsm.exception.InvalidStateException;
import io.hypercell.fsm.listener.EventBus;
import io.hypercell.fsm.manager.StateMachineManager;
import io.hypercell.fsm.resume.ExecutionSnapshot;
import io.hypercell.fsm.resume.ResumePolicy;
import io.hypercell.fsm.resume.SnapshotRepository;
import io.hypercell.fsm.resume.SnapshotStatus;
import io.hypercell.fsm.retry.RetryCoordinator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * The validated, immutable state machine blueprint.
 * <p>
 * Now holds an EventBus that is passed to every instance it creates.
 * The EventBus is constructed once in the builder with all registered listeners
 * and then shared across instances (it's read-only after construction).
 *
 * @param <C> the context type flowing through the machine
 */
public class DefaultStateMachineDefinition<C> implements StateMachineDefinition<C> {

    private final String id;
    private final StateDefinition<C> initialState;
    private final Map<String, StateDefinition<C>> states;
    private final Map<String, List<TransitionDefinition<C>>> transitions;
    private final ResumePolicy<C> resumePolicy;
    private final SnapshotRepository snapshotRepository;
    private final RetryCoordinator<C> retryCoordinator;
    private final EventBus<C> eventBus;
    private final ContextLoader<C> contextLoader;
    private final ExecutorService recoveryExecutor;
    private final int recoveryPageSize;

    /**
     * Default recovery page size when none is configured on the builder.
     */
    public static final int DEFAULT_RECOVERY_PAGE_SIZE = 100;

    public DefaultStateMachineDefinition(String id,
                                         StateDefinition<C> initialState,
                                         Map<String, StateDefinition<C>> states,
                                         Map<String, List<TransitionDefinition<C>>> transitions,
                                         ResumePolicy<C> resumePolicy,
                                         SnapshotRepository snapshotRepository,
                                         RetryCoordinator<C> retryCoordinator,
                                         EventBus<C> eventBus,
                                         ContextLoader<C> contextLoader) {
        this(id, initialState, states, transitions, resumePolicy, snapshotRepository,
                retryCoordinator, eventBus, contextLoader, null, DEFAULT_RECOVERY_PAGE_SIZE);
    }

    public DefaultStateMachineDefinition(String id,
                                         StateDefinition<C> initialState,
                                         Map<String, StateDefinition<C>> states,
                                         Map<String, List<TransitionDefinition<C>>> transitions,
                                         ResumePolicy<C> resumePolicy,
                                         SnapshotRepository snapshotRepository,
                                         RetryCoordinator<C> retryCoordinator,
                                         EventBus<C> eventBus,
                                         ContextLoader<C> contextLoader,
                                         ExecutorService recoveryExecutor) {
        this(id, initialState, states, transitions, resumePolicy, snapshotRepository,
                retryCoordinator, eventBus, contextLoader, recoveryExecutor, DEFAULT_RECOVERY_PAGE_SIZE);
    }

    public DefaultStateMachineDefinition(String id,
                                         StateDefinition<C> initialState,
                                         Map<String, StateDefinition<C>> states,
                                         Map<String, List<TransitionDefinition<C>>> transitions,
                                         ResumePolicy<C> resumePolicy,
                                         SnapshotRepository snapshotRepository,
                                         RetryCoordinator<C> retryCoordinator,
                                         EventBus<C> eventBus,
                                         ContextLoader<C> contextLoader,
                                         ExecutorService recoveryExecutor,
                                         int recoveryPageSize) {
        this.id = id;
        this.initialState = initialState;
        this.states = Collections.unmodifiableMap(states);
        this.transitions = Collections.unmodifiableMap(transitions);
        this.resumePolicy = resumePolicy;
        this.snapshotRepository = snapshotRepository;
        this.retryCoordinator = retryCoordinator;
        this.eventBus = eventBus != null ? eventBus : EventBus.empty();
        this.contextLoader = contextLoader;
        this.recoveryExecutor = recoveryExecutor;
        this.recoveryPageSize = recoveryPageSize > 0 ? recoveryPageSize : DEFAULT_RECOVERY_PAGE_SIZE;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public StateDefinition<C> initialState() {
        return initialState;
    }

    @Override
    public SnapshotRepository repository() {
        return snapshotRepository;
    }

    @Override
    public ResumePolicy<C> resumePolicy() {
        return resumePolicy;
    }

    @Override
    public RetryCoordinator<C> retryCoordinator() {
        return retryCoordinator;
    }

    @Override
    public ContextLoader<C> contextLoader() {
        return contextLoader;
    }

    @Override
    public ExecutorService recoveryExecutor() {
        return recoveryExecutor;
    }

    @Override
    public int recoveryPageSize() {
        return recoveryPageSize;
    }

    @Override
    public StateDefinition<C> stateByName(String name) {
        StateDefinition<C> s = states.get(name);
        if (s == null) throw new InvalidStateException(name);
        return s;
    }

    @Override
    public List<TransitionDefinition<C>> transitionsFrom(String stateName) {
        return transitions.getOrDefault(stateName, Collections.emptyList());
    }

    @Override
    public boolean isInitialState(String stateName) {
        return initialState.name().equals(stateName);
    }

    @Override
    public boolean isTerminal(String stateName) {
        StateDefinition<C> state = stateByName(stateName);
        return state.isTerminal();
    }

    @Override
    public StateMachineInstance<C> newInstance(C ctx) {
        return new DefaultStateMachineInstance<>(
                this, initialState, ctx, snapshotRepository, retryCoordinator, eventBus);
    }

    @Override
    public StateMachineInstance<C> newInstance(C ctx, String executionId) {
        return new DefaultStateMachineInstance<>(this, initialState, ctx, executionId,
                snapshotRepository, retryCoordinator, eventBus);
    }

    @Override
    public StateMachineManager<C> newManager() {
        return newManager(snapshotRepository);
    }

    @Override
    public StateMachineManager<C> newManager(SnapshotRepository repository) {
        return StateMachineManager.create(this, repository);
    }

    @Override
    public StateMachineInstance<C> reconstitute(C ctx, ExecutionSnapshot snapshot) {
        return reconstitute(ctx, snapshot, snapshotRepository);
    }

    @Override
    public StateMachineInstance<C> reconstitute(C ctx, ExecutionSnapshot snapshot, SnapshotRepository repository) {
        StateDefinition<C> currentState = stateByName(snapshot.getCurrentStateName());

        ExecutionRecord executionRecord = new ExecutionRecord(snapshot.getExecutionId(), snapshot.getCurrentStateName());

        executionRecord.setStatus(ExecutionStatus.RUNNING);
        if (snapshot.getLastTriggerEvent() != null) {
            executionRecord.setLastTriggerEvent(snapshot.getLastTriggerEvent());
        }

        return new DefaultStateMachineInstance<>(this, currentState, ctx, snapshot.getAttemptNumber(),
                executionRecord, ExecutionStatus.RUNNING, repository, retryCoordinator, eventBus);
    }

    @Override
    public StateMachineInstance<C> resume(C ctx, ExecutionSnapshot snapshot) {
        return resume(ctx, snapshot, snapshotRepository);
    }

    @Override
    public StateMachineInstance<C> resume(C ctx, ExecutionSnapshot snapshot,
                                          SnapshotRepository repository) {
        StateDefinition<C> failedState = stateByName(snapshot.getFailedStateName());
        ExecutionRecord hydratedRecord = hydrateRecord(snapshot);

        if (repository != null) {
            repository.save(snapshot.getExecutionId(), snapshot.withStatus(SnapshotStatus.RUNNING));
        }

        return new DefaultStateMachineInstance<>(
                this, failedState, ctx, snapshot.getAttemptNumber(), hydratedRecord,
                ExecutionStatus.FAILED, repository != null ? repository : snapshotRepository, retryCoordinator,
                eventBus);
    }

    @Override
    public DefaultStateMachineInstance<C> resumeInterrupted(C ctx, ExecutionSnapshot snapshot,
                                                            SnapshotRepository repository) {
        StateDefinition<C> currentState = stateByName(snapshot.getCurrentStateName());
        ExecutionRecord executionRecord = new ExecutionRecord(
                snapshot.getExecutionId(), snapshot.getCurrentStateName());

        String stateName = snapshot.getCurrentStateName();
        for (Map.Entry<String, ActionResult> entry
                : snapshot.getCompletedSubStepResults().entrySet()) {
            String key = entry.getKey();
            if (key.contains("::")) {
                String[] parts = key.split("::", 2);
                executionRecord.recordStep(parts[0], parts[1], entry.getValue());
            } else {
                executionRecord.recordStep(stateName, key, entry.getValue());
            }
        }

        if (snapshot.getLastTriggerEvent() != null) {
            executionRecord.setLastTriggerEvent(snapshot.getLastTriggerEvent());
        }

        SnapshotRepository effectiveRepo = repository != null ? repository : snapshotRepository;
        if (effectiveRepo != null) {
            effectiveRepo.save(snapshot.getExecutionId(), snapshot.withStatus(SnapshotStatus.RUNNING));
        }

        return new DefaultStateMachineInstance<>(
                this, currentState, ctx, snapshot.getAttemptNumber(), executionRecord,
                ExecutionStatus.RUNNING, effectiveRepo, retryCoordinator, eventBus);
    }

    private ExecutionRecord hydrateRecord(ExecutionSnapshot snapshot) {
        ExecutionRecord executionRecord = new ExecutionRecord(
                snapshot.getExecutionId(), snapshot.getFailedStateName());

        for (Map.Entry<String, ActionResult> entry
                : snapshot.getCompletedSubStepResults().entrySet()) {
            String[] parts = entry.getKey().split("::", 2);
            if (parts.length == 2) {
                executionRecord.recordStep(parts[0], parts[1], entry.getValue());
            }
        }

        if (snapshot.getLastTriggerEvent() != null) {
            executionRecord.setLastTriggerEvent(snapshot.getLastTriggerEvent());
        }
        executionRecord.markFailed(snapshot.getFailedStateName(), snapshot.getFailedSubStepName());
        return executionRecord;
    }
}
