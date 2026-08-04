package io.hypercell.fsm.core;

import io.hypercell.fsm.failure.FailurePolicy;
import io.hypercell.fsm.lock.ExecutionLockProvider;
import io.hypercell.fsm.lock.ReentrantExecutionLockProvider;
import io.hypercell.fsm.manager.StateMachineManager;
import io.hypercell.fsm.resume.ExecutionSnapshot;
import io.hypercell.fsm.resume.ResumePolicy;
import io.hypercell.fsm.resume.SnapshotRepository;
import io.hypercell.fsm.retry.RetryCoordinator;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * The immutable, validated blueprint of a state machine.
 * <p>
 * Created once via {@code StateMachine.define(id).....build()} and then reused to
 * produce as many instances as needed. The definition itself is thread-safe and
 * stateless — it carries no per-execution data.
 * <p>
 * THREAD SAFETY: immutable after {@code build()}; share freely across threads.
 *
 * @param <C> the context type flowing through the machine
 */
public interface StateMachineDefinition<C> {

    /**
     * Stable identifier for this machine type, set in {@code StateMachine.define(id)}.
     */
    String id();

    /**
     * The state the machine enters when a new instance is created.
     */
    StateDefinition<C> initialState();

    /**
     * The snapshot repository configured on this definition, if any.
     * May be {@code null} if no repository was set in the builder.
     */
    SnapshotRepository repository();

    /**
     * Look up a state by name.
     *
     * @throws io.hypercell.fsm.exception.InvalidStateException if no state with that name exists
     */
    StateDefinition<C> stateByName(String name);

    /**
     * All transitions defined from the named state, in definition order.
     * Returns an empty list for terminal states.
     */
    List<TransitionDefinition<C>> transitionsFrom(String stateName);

    /**
     * Check if a state is the initial state of this machine.
     * Useful for validation and defensive programming against definition changes.
     *
     * @param stateName the state name to check
     * @return true if the state is the initial state, false otherwise
     */
    boolean isInitialState(String stateName);

    /**
     * Check if a state is terminal.
     * Useful for validation and determining if a machine execution is complete.
     *
     * @param stateName the state name to check
     * @return true if the state is terminal, false otherwise
     * @throws io.hypercell.fsm.exception.InvalidStateException if no state with that name exists
     */
    boolean isTerminal(String stateName);

    /**
     * The policy that decides which sub-steps to skip when resuming after failure.
     * The default ({@link io.hypercell.fsm.resume.DefaultResumePolicy}) skips any
     * sub-step that is recorded as completed in the execution record.
     */
    ResumePolicy<C> resumePolicy();

    /**
     * The retry coordinator wired in at build time, or {@code null} if no retry
     * policy was configured. Used internally; consumers should not call this directly.
     */
    RetryCoordinator<C> retryCoordinator();

    /**
     * The context loader configured at build time, or {@code null} if none was set.
     * Restores a fresh context from the given executionId. Used by the manager
     * on every request and by the retry coordinator on auto-retries.
     */
    ContextLoader<C> contextLoader();

    /**
     * The consumer-supplied executor used by
     * {@link StateMachineManager#recoverInterruptedExecutions()} to resume interrupted
     * executions in parallel. Returns {@code null} if none was configured.
     * <p>
     * The consumer owns this executor's lifecycle — the library never shuts it down.
     * {@code recoverInterruptedExecutions()} throws {@link IllegalStateException} if this
     * returns {@code null}.
     */
    ExecutorService recoveryExecutor();

    /**
     * The page size used by {@link StateMachineManager#recoverInterruptedExecutions()} when
     * keyset-paginating interrupted executions from the repository. Defaults to {@code 100}.
     *
     * @return the recovery page size; always {@code > 0}
     */
    int recoveryPageSize();

    /**
     * The lock provider that guards concurrent access to the same execution ID.
     * <p>
     * The default implementation returns a new {@link ReentrantExecutionLockProvider} which
     * is suitable for single-JVM deployments. Override in {@code DefaultStateMachineDefinition}
     * (via the builder) to return a shared, pre-configured provider — for example
     * {@code JdbcExecutionLockProvider} for distributed deployments.
     * <p>
     * Implementations of this interface that do not override this method should store and
     * return a single instance to ensure all managers created from the same definition share
     * one lock map.
     *
     * @return the lock provider; never {@code null}
     */
    default ExecutionLockProvider lockProvider() {
        return new ReentrantExecutionLockProvider();
    }

    /**
     * The machine-wide {@link FailurePolicy} — the last level consulted before the
     * {@link io.hypercell.fsm.failure.FailureDisposition#RETRY} default.
     * <p>
     * Consulted only when neither the failing sub-step nor its state expressed an opinion.
     * Returns {@code null} when no machine-level policy was configured, in which case every
     * unclassified failure is treated as {@code RETRY} — the library's behaviour before
     * dispositions existed.
     */
    FailurePolicy<C> failurePolicy();

    /**
     * Create a fresh instance starting at the initial state.
     * A UUID is generated as the execution ID (used as the snapshot key).
     *
     * @param ctx the mutable domain object passed to every guard, action, and sub-step
     */
    StateMachineInstance<C> newInstance(C ctx);

    /**
     * Create a fresh instance with an explicit execution ID.
     * Use this when your business entity already has a meaningful ID
     * (e.g. {@code orderId}) so the snapshot key matches your domain.
     *
     * @param ctx         the mutable domain object
     * @param executionId stable identifier; becomes the snapshot storage key
     */
    StateMachineInstance<C> newInstance(C ctx, String executionId);

    /**
     * Create a {@link StateMachineManager} bound to this definition's repository
     * and context loader.
     * <p>
     * The context loader is inherited from the definition (set via {@link #contextLoader()}).
     * If no context loader was configured on the definition, it must be supplied to every
     * {@code trigger()} or {@code proceed()} call via the {@code contextOverride} parameter.
     */
    StateMachineManager<C> newManager();

    /**
     * Create a {@link StateMachineManager} with an explicit repository.
     * Use this when you want the manager to use a different repository than the one
     * set on the definition (e.g. a production DB repo vs. the in-memory one used
     * during definition-time testing).
     * <p>
     * The context loader is inherited from the definition.
     *
     * @param repository where snapshots are stored and loaded
     */
    StateMachineManager<C> newManager(SnapshotRepository repository);

    /**
     * Restore a {@code RUNNING} instance from a checkpoint snapshot.
     * <p>
     * Use this when a process restarted between requests and the snapshot status is
     * {@code RUNNING} (the previous request saved a checkpoint but did not fail).
     * The returned instance is positioned at {@code snapshot.getCurrentStateName()}
     * with status {@code RUNNING}; the caller calls {@code trigger(event)} next.
     * <p>
     * This method does NOT re-run any sub-steps. It is purely a position restore.
     *
     * @param ctx      a fresh ctx loaded for this execution
     * @param snapshot a snapshot with status {@code RUNNING}
     */
    StateMachineInstance<C> reconstitute(C ctx, ExecutionSnapshot snapshot);

    /**
     * Same as {@link #reconstitute(Object, ExecutionSnapshot)} but also binds the
     * provided repository to the reconstituted instance so that subsequent
     * checkpoints are saved there.
     */
    StateMachineInstance<C> reconstitute(C ctx, ExecutionSnapshot snapshot, SnapshotRepository repository);

    /**
     * Resume a {@code FAILED} instance from a failure snapshot.
     * <p>
     * Use this when the snapshot status is {@code FAILED} and you want to retry.
     * The returned instance is positioned at the failed state; the caller calls
     * {@code proceed()} next to re-run the failed sub-steps (skipping those that
     * already completed).
     * <p>
     * Unlike {@code reconstitute}, this method populates the execution record with
     * the completed sub-step results from the snapshot so the resume policy can
     * correctly skip them.
     *
     * @param ctx      a fresh ctx loaded for this execution (see
     *                 <a href="https://github.com/hypercell/fsm-library/blob/main/docs/05-persistence-and-retry.md#ctx-on-resume">Context on resume</a>)
     * @param snapshot a snapshot with status {@code FAILED}
     */
    StateMachineInstance<C> resume(C ctx, ExecutionSnapshot snapshot);

    /**
     * Same as {@link #resume(Object, ExecutionSnapshot)} but also binds the provided
     * repository to the resumed instance.
     */
    StateMachineInstance<C> resume(C ctx, ExecutionSnapshot snapshot,
                                   SnapshotRepository repository);

    /**
     * Reconstitute an execution that was interrupted mid-transition (process crashed while
     * sub-steps were executing). The snapshot status is {@code RUNNING} but not all sub-steps
     * of the current state have completed entries in {@code completedSubStepResults}.
     * <p>
     * The returned instance is anchored at {@code snapshot.getCurrentStateName()} with status
     * {@code RUNNING} and the execution record pre-populated with the completed sub-steps,
     * enabling the {@link io.hypercell.fsm.resume.ResumePolicy} to skip them when
     * {@link StateMachineInstance#resume()} runs the remaining steps.
     * <p>
     * Unlike {@link #resume(Object, ExecutionSnapshot, SnapshotRepository)}, this method
     * does <em>not</em> mark the record as failed — the in-flight transition simply has
     * remaining work to complete.
     *
     * @param ctx        a fresh context loaded for this execution
     * @param snapshot   a {@code RUNNING} snapshot where not all sub-steps are yet complete
     * @param repository where subsequent checkpoints are saved; may be {@code null}
     * @return an instance ready for {@link StateMachineInstance#resume()} to be called
     */
    default StateMachineInstance<C> resumeInterrupted(C ctx, ExecutionSnapshot snapshot,
                                                      SnapshotRepository repository) {
        return reconstitute(ctx, snapshot, repository);
    }
}
