package io.hypercell.fsm.manager;

import io.hypercell.fsm.core.ContextLoader;
import io.hypercell.fsm.core.ExecutionStatus;
import io.hypercell.fsm.core.StateMachineDefinition;
import io.hypercell.fsm.core.StateMachineInstance;
import io.hypercell.fsm.exception.CompletedMachineException;
import io.hypercell.fsm.exception.ConcurrentExecutionException;
import io.hypercell.fsm.exception.IllegalTriggerStateException;
import io.hypercell.fsm.exception.StateMachineException;
import io.hypercell.fsm.exception.SubStepExecutionException;
import io.hypercell.fsm.resume.ExecutionSnapshot;
import io.hypercell.fsm.resume.SnapshotRepository;
import io.hypercell.fsm.resume.SnapshotStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Default implementation of {@link StateMachineManager}.
 * <p>
 * CONCURRENCY MODEL:
 * A {@code ConcurrentHashMap<executionId, ReentrantLock>} provides per-execution in-process
 * locking. {@code ReentrantLock.tryLock()} returns immediately (non-blocking): if the lock
 * is held by another thread, {@link io.hypercell.fsm.exception.ConcurrentExecutionException}
 * is thrown rather than blocking the caller. This maps naturally to HTTP: return 409 Conflict
 * immediately rather than making the client wait.
 * <p>
 * Lock entries are cleaned up after each call to avoid unbounded map growth. Entries are only
 * removed when no thread is waiting — checked via {@code ReentrantLock.hasQueuedThreads()}.
 * <p>
 * STRICT TRIGGER CONTRACT:
 * {@link #trigger(String, String)} applies exactly one transition. Executions that require
 * prior recovery (FAILED → {@link #proceed(String)}, RUNNING → {@link #resume(String)}) or
 * are not yet retryable (RETRY_SCHEDULED) cause
 * {@link io.hypercell.fsm.exception.IllegalTriggerStateException} to be thrown immediately.
 * The caller is responsible for explicit recovery before re-triggering.
 *
 * @param <C> the context type flowing through the machine
 */
public class DefaultStateMachineManager<C> implements StateMachineManager<C> {
    private static final Logger log = LoggerFactory.getLogger(DefaultStateMachineManager.class);

    private final StateMachineDefinition<C> definition;
    private final SnapshotRepository repository;
    private final ContextLoader<C> contextLoader;
    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();
    private final ScheduledExecutorService recoveryExecutor;

    DefaultStateMachineManager(StateMachineDefinition<C> definition,
                               SnapshotRepository repository,
                               ContextLoader<C> contextLoader) {
        this.definition = definition;
        this.repository = repository;
        this.contextLoader = contextLoader;
        this.recoveryExecutor = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "fsm-recovery");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public ManagedTransitionResult<C> trigger(String executionId, String event) {
        return trigger(executionId, event, null);
    }

    @Override
    public ManagedTransitionResult<C> trigger(String executionId, String event, C contextOverride) {
        ReentrantLock lock = acquireLock(executionId);
        try {
            return doTrigger(executionId, event, contextOverride);
        } finally {
            releaseLock(executionId, lock);
        }
    }

    @Override
    public ManagedTransitionResult<C> proceed(String executionId) {
        return proceed(executionId, null);
    }

    @Override
    public ManagedTransitionResult<C> proceed(String executionId, C contextOverride) {
        ReentrantLock lock = acquireLock(executionId);
        try {
            return doProceed(executionId, contextOverride);
        } finally {
            releaseLock(executionId, lock);
        }
    }

    @Override
    public ManagedTransitionResult<C> initialize(String executionId) {
        return initialize(executionId, null);
    }

    @Override
    public ManagedTransitionResult<C> initialize(String executionId, C contextOverride) {
        ReentrantLock lock = acquireLock(executionId);
        try {
            return doInitialize(executionId, contextOverride);
        } finally {
            releaseLock(executionId, lock);
        }
    }

    @Override
    public Optional<ExecutionSnapshot> snapshotOf(String executionId) {
        return repository.load(executionId);
    }

    @Override
    public Optional<String> currentState(String executionId) {
        return repository.load(executionId)
                .map(ExecutionSnapshot::getCurrentStateName);
    }

    @Override
    public TriggerEligibility eligibilityOf(String executionId) {
        Optional<ExecutionSnapshot> snapshotOpt = repository.load(executionId);
        if (snapshotOpt.isEmpty()) {
            return TriggerEligibility.READY;
        }
        ExecutionSnapshot snapshot = snapshotOpt.get();
        return switch (snapshot.getStatus()) {
            case WAITING -> TriggerEligibility.READY;
            case FAILED, RETRY_SCHEDULED -> TriggerEligibility.NEEDS_PROCEED;
            case RUNNING -> TriggerEligibility.NEEDS_RESUME;
            case TERMINATED -> TriggerEligibility.TERMINATED;
        };
    }

    @Override
    public StateMachineManager<C> withContextLoader(ContextLoader<C> contextLoader) {
        return new DefaultStateMachineManager<>(definition, repository, contextLoader);
    }

    @Override
    public boolean isInitialState(String stateName) {
        return definition.isInitialState(stateName);
    }

    @Override
    public boolean isTerminal(String stateName) {
        return definition.isTerminal(stateName);
    }

    @Override
    public ManagedTransitionResult<C> resume(String executionId) {
        ReentrantLock lock = acquireLock(executionId);
        try {
            ExecutionSnapshot snapshot = repository.load(executionId)
                    .orElseThrow(() -> new IllegalArgumentException(
                            "No snapshot found for executionId: " + executionId));

            if (snapshot.isTerminated()) {
                throw new IllegalStateException(
                        "Cannot resume a TERMINATED execution '" + executionId
                                + "'. The workflow has already finished.");
            }
            if (snapshot.isFailed()) {
                throw new IllegalStateException(
                        "Cannot resume a FAILED execution '" + executionId
                                + "'. Call proceed() to retry failed sub-steps.");
            }
            if (snapshot.isWaiting()) {
                return ManagedTransitionResult.<C>builder()
                        .executionId(executionId)
                        .fromState(snapshot.getCurrentStateName())
                        .toState(snapshot.getCurrentStateName())
                        .executionStatus(ExecutionStatus.RUNNING)
                        .build();
            }

            if (!isInterrupted(snapshot)) {
                return ManagedTransitionResult.<C>builder()
                        .executionId(executionId)
                        .fromState(snapshot.getCurrentStateName())
                        .toState(snapshot.getCurrentStateName())
                        .executionStatus(ExecutionStatus.RUNNING)
                        .build();
            }

            C ctx = resolveContext(executionId, null);
            StateMachineInstance<C> instance =
                    definition.resumeInterrupted(ctx, snapshot, repository);
            String fromState = instance.currentState().name();

            try {
                instance.resume();
                return ManagedTransitionResult.<C>builder()
                        .executionId(instance.executionId())
                        .fromState(fromState)
                        .toState(instance.currentState().name())
                        .executionStatus(instance.status())
                        .context(instance.context())
                        .build();
            } catch (SubStepExecutionException e) {
                return ManagedTransitionResult.<C>builder()
                        .executionId(instance.executionId())
                        .fromState(fromState)
                        .toState(instance.currentState().name())
                        .executionStatus(ExecutionStatus.FAILED)
                        .failedStateName(e.getStateName())
                        .failedSubStepName(e.getSubStepName())
                        .rootCause(e.getCause())
                        .context(instance.context())
                        .build();
            }
        } finally {
            releaseLock(executionId, lock);
        }
    }

    /**
     * Resume all interrupted executions in parallel using the consumer-supplied executor.
     * <p>
     * <strong>Requires a {@code recoveryExecutor} configured on the builder.</strong>
     * Throws {@link IllegalStateException} if none is configured; use
     * {@link #resume(String)} per execution instead, or configure one via
     * {@link io.hypercell.fsm.builder.StateMachineBuilder#recoveryExecutor(java.util.concurrent.ExecutorService)}.
     * <p>
     * This method is <strong>single-instance only</strong>. Do not call it in a
     * multi-replica deployment — a starting node could resume an execution being
     * processed by a live peer. Multi-replica deployments should rely on lazy resume.
     * <p>
     * Implementation: keyset-paginate {@code listInterrupted(limit, afterId)} to collect
     * all interrupted execution IDs (stable under concurrent status flips as those rows
     * flip to WAITING/TERMINATED and fall out of future pages), submit each to the executor,
     * and return the total submitted count immediately (async).
     *
     * @return the number of resume tasks submitted (executions may still be in-flight)
     * @throws IllegalStateException if no {@code recoveryExecutor} is configured
     */
    @Override
    public int recoverInterruptedExecutions() {
        java.util.concurrent.ExecutorService executor = definition.recoveryExecutor();
        if (executor == null) {
            throw new IllegalStateException(
                    "recoverInterruptedExecutions requires a recoveryExecutor; configure one on "
                            + "the builder via .recoveryExecutor(executor), or use resume(executionId) "
                            + "per execution");
        }

        final int PAGE = definition.recoveryPageSize();
        int toBeRecovered = 0;
        String lastId = null;
        List<ExecutionSnapshot> page;
        do {
            page = repository.listInterrupted(PAGE, lastId);
            if (page.isEmpty()) break;
            for (ExecutionSnapshot s : page) {
                executor.submit(() -> {
                    try {
                        var result = resume(s.getExecutionId());
                        if (result.isFailed()) {
                            log.error("[recoverInterruptedExecutions] Failed to recover '{}': {}",
                                    s.getExecutionId(), result.getRootCause().getMessage(), result.getRootCause());
                        }
                    } catch (Exception e) {
                        log.error("[recoverInterruptedExecutions] Failed to recover '{}': {}",
                                s.getExecutionId(), e.getMessage(), e);
                    }
                });
            }
            toBeRecovered += page.size();
            lastId = page.get(page.size() - 1).getExecutionId();
        } while (page.size() == PAGE);

        return toBeRecovered;
    }

    @Override
    public void recoverPendingRetries() {
        if (definition.retryCoordinator() == null) {
            return;
        }

        List<ExecutionSnapshot> pending = repository.listPendingRetries();

        for (ExecutionSnapshot snapshot : pending) {
            boolean shouldRecover = (snapshot.getStatus() == SnapshotStatus.RETRY_SCHEDULED || snapshot.getStatus() == SnapshotStatus.FAILED)
                    && definition.retryCoordinator().getRetryPolicy().shouldRetry(snapshot.getAttemptNumber(), null);

            if (!shouldRecover) continue;

            Duration delay = Duration.ZERO;
            if (snapshot.getScheduledRetryAt() != null) {
                Duration remaining = Duration.between(
                        Instant.now(), snapshot.getScheduledRetryAt());
                if (!remaining.isNegative()) {
                    delay = remaining;
                }
            }

            String executionId = snapshot.getExecutionId();
            long delayMs = delay.toMillis();

            recoveryExecutor.schedule(() -> {
                try {
                    proceed(executionId);
                } catch (Exception e) {
                    log.warn("Recovery retry failed for '{}': {}", executionId, e.getMessage());
                }
            }, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Returns {@code true} when the snapshot was interrupted mid-transition
     * (process crashed while sub-steps were executing).
     * <p>
     * With the new persisted-status model, {@code RUNNING} means exclusively
     * "actively processing sub-steps". A crash leaves the snapshot in this state;
     * an at-rest execution is now saved as {@code WAITING}. Therefore, interrupted
     * detection is a simple status check.
     *
     * @param snapshot a snapshot whose status has already been checked (callers pass RUNNING snapshots)
     * @return {@code true} if the execution was interrupted, {@code false} otherwise
     */
    private boolean isInterrupted(ExecutionSnapshot snapshot) {
        return snapshot.getStatus() == SnapshotStatus.RUNNING;
    }

    private ManagedTransitionResult<C> doTrigger(String executionId, String event,
                                                 C contextOverride) {
        Optional<ExecutionSnapshot> snapshotOpt = repository.load(executionId);

        if (snapshotOpt.isEmpty()) {
            C ctx = resolveContext(executionId, contextOverride);
            return firstTrigger(executionId, event, ctx);
        }

        ExecutionSnapshot snapshot = snapshotOpt.get();

        if (snapshot.isTerminated()) {
            throw new CompletedMachineException(executionId, snapshot.getCurrentStateName());
        }

        if (snapshot.isFailed()) {
            throw IllegalTriggerStateException.failed(
                    executionId, event, snapshot.getCurrentStateName());
        }

        if (snapshot.getStatus() == SnapshotStatus.RETRY_SCHEDULED) {
            throw IllegalTriggerStateException.retryScheduled(executionId, event);
        }

        if (snapshot.isRunning()) {
            throw IllegalTriggerStateException.running(executionId, event);
        }

        C ctx = resolveContext(executionId, contextOverride);
        return reconstituteThenTrigger(executionId, event, ctx, snapshot);
    }

    /**
     * First event ever for this executionId.
     * Creates a fresh instance (initial state sub-steps run in constructor),
     * then fires the transition event.
     */
    private ManagedTransitionResult<C> firstTrigger(String executionId, String event, C ctx) {
        String fromState = definition.initialState().name();

        StateMachineInstance<C> instance;
        try {
            instance = definition.newInstance(ctx, executionId);
        } catch (SubStepExecutionException e) {
            return ManagedTransitionResult.<C>builder()
                    .executionId(executionId)
                    .fromState(fromState)
                    .toState(fromState)
                    .executionStatus(ExecutionStatus.FAILED)
                    .failedStateName(e.getStateName())
                    .failedSubStepName(e.getSubStepName())
                    .rootCause(e.getCause())
                    .context(ctx)
                    .build();
        }

        return executeTrigger(instance, event, fromState);
    }

    /**
     * WAITING snapshot — normal next-event path.
     * Reconstitutes at currentStateName and fires the event.
     */
    private ManagedTransitionResult<C> reconstituteThenTrigger(String executionId, String event,
                                                               C ctx, ExecutionSnapshot snapshot) {
        StateMachineInstance<C> instance = definition.reconstitute(ctx, snapshot, repository);
        String fromState = instance.currentState().name();
        return executeTrigger(instance, event, fromState);
    }

    /**
     * Fires trigger(event) on an instance and builds the result.
     * SubStepExecutionException is caught here — the snapshot is already saved
     * inside handleFailure() so we just build a FAILED result.
     */
    private ManagedTransitionResult<C> executeTrigger(StateMachineInstance<C> instance,
                                                      String event, String fromState) {
        try {
            instance.trigger(event);
            return ManagedTransitionResult.<C>builder()
                    .executionId(instance.executionId())
                    .fromState(fromState)
                    .toState(instance.currentState().name())
                    .executionStatus(instance.status())
                    .context(instance.context())
                    .build();
        } catch (SubStepExecutionException e) {
            return ManagedTransitionResult.<C>builder()
                    .executionId(instance.executionId())
                    .fromState(fromState)
                    .toState(instance.currentState().name())
                    .executionStatus(ExecutionStatus.FAILED)
                    .failedStateName(e.getStateName())
                    .failedSubStepName(e.getSubStepName())
                    .rootCause(e.getCause())
                    .context(instance.context())
                    .build();
        }
    }

    /**
     * Manual proceed — retry failed sub-steps without a new event.
     */
    private ManagedTransitionResult<C> doProceed(String executionId, C contextOverride) {
        ExecutionSnapshot snapshot = repository.load(executionId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "No snapshot found for executionId: " + executionId));

        if (snapshot.isTerminated()) {
            throw new CompletedMachineException(executionId, snapshot.getCurrentStateName());
        }
        if (!snapshot.isFailed()) {
            throw new IllegalStateException(
                    "proceed() requires a FAILED snapshot. Current status: " + snapshot.getStatus());
        }

        C ctx = resolveContext(executionId, contextOverride);
        StateMachineInstance<C> instance = definition.resume(ctx, snapshot, repository);
        String fromState = instance.currentState().name();

        try {
            instance.proceed();
            return ManagedTransitionResult.<C>builder()
                    .executionId(executionId)
                    .fromState(fromState)
                    .toState(instance.currentState().name())
                    .executionStatus(instance.status())
                    .context(instance.context())
                    .build();
        } catch (SubStepExecutionException e) {
            return ManagedTransitionResult.<C>builder()
                    .executionId(executionId)
                    .fromState(fromState)
                    .toState(instance.currentState().name())
                    .executionStatus(ExecutionStatus.FAILED)
                    .failedStateName(e.getStateName())
                    .failedSubStepName(e.getSubStepName())
                    .rootCause(e.getCause())
                    .context(instance.context())
                    .build();
        }
    }

    /**
     * Initialize a new execution: create instance, run initial sub-steps, save checkpoint,
     * stay in initial state. If already initialized, return the current state.
     */
    private ManagedTransitionResult<C> doInitialize(String executionId, C contextOverride) {
        Optional<ExecutionSnapshot> existing = repository.load(executionId);
        if (existing.isPresent()) {
            ExecutionSnapshot snapshot = existing.get();
            if (snapshot.isTerminated()) {
                throw new CompletedMachineException(
                        executionId, snapshot.getCurrentStateName());
            }
            ExecutionStatus status = snapshot.isFailed() ? ExecutionStatus.FAILED : ExecutionStatus.RUNNING;
            ManagedTransitionResult.Builder<C> builder = ManagedTransitionResult.<C>builder()
                    .executionId(executionId)
                    .fromState(snapshot.getCurrentStateName())
                    .toState(snapshot.getCurrentStateName())
                    .executionStatus(status);
            if (snapshot.isFailed()) {
                builder.failedStateName(snapshot.getFailedStateName())
                        .failedSubStepName(snapshot.getFailedSubStepName());
            }
            return builder.build();
        }

        String initialStateName = definition.initialState().name();
        C ctx = resolveContext(executionId, contextOverride);

        try {
            StateMachineInstance<C> instance = definition.newInstance(ctx, executionId);
            return ManagedTransitionResult.<C>builder()
                    .executionId(executionId)
                    .fromState(initialStateName)
                    .toState(initialStateName)
                    .executionStatus(ExecutionStatus.RUNNING)
                    .context(instance.context())
                    .build();
        } catch (SubStepExecutionException e) {
            return ManagedTransitionResult.<C>builder()
                    .executionId(executionId)
                    .fromState(initialStateName)
                    .toState(initialStateName)
                    .executionStatus(ExecutionStatus.FAILED)
                    .failedStateName(e.getStateName())
                    .failedSubStepName(e.getSubStepName())
                    .rootCause(e.getCause())
                    .context(ctx)
                    .build();
        }
    }

    /**
     * Resolves the context for this request.
     * If contextOverride is provided, use it directly.
     * Otherwise, delegate to the manager's configured contextLoader.
     * <p>
     * Checked exceptions from the context loader are wrapped in StateMachineException;
     * unchecked exceptions are propagated as-is.
     */
    private C resolveContext(String executionId, C contextOverride) {
        if (contextOverride != null) {
            return contextOverride;
        }
        if (contextLoader != null) {
            try {
                return contextLoader.load(executionId);
            } catch (RuntimeException | Error e) {
                throw e;
            } catch (Exception e) {
                throw new StateMachineException(
                        "Failed to load context for executionId '" + executionId + "'", e);
            }
        }
        throw new IllegalStateException(
                "No ctx available for executionId '" + executionId + "'. " +
                        "Either configure a contextLoader or pass a contextOverride.");
    }

    /**
     * Acquire the per-executionId lock. Returns immediately.
     * Throws ConcurrentExecutionException if another thread holds the lock.
     * <p>
     * NOTE: This is in-process only. For distributed deployments, implement
     * optimistic locking in your SnapshotRepository (e.g. compare-and-swap
     * in PostgreSQL, or SET NX with TTL in Redis).
     */
    private ReentrantLock acquireLock(String executionId) {
        ReentrantLock lock = locks.computeIfAbsent(executionId, k -> new ReentrantLock());
        if (!lock.tryLock()) {
            throw new ConcurrentExecutionException(executionId);
        }
        return lock;
    }

    /**
     * Release the lock and clean up the map entry if no threads are waiting.
     * Prevents unbounded growth of the locks map for long-running applications.
     */
    private void releaseLock(String executionId, ReentrantLock lock) {
        lock.unlock();
        locks.computeIfPresent(executionId, (k, l) ->
                l.hasQueuedThreads() ? l : null);
    }
}
