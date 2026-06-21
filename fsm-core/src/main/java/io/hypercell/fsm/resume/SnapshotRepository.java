package io.hypercell.fsm.resume;

import java.util.List;
import java.util.Optional;

/**
 * Pluggable storage for execution snapshots.
 * <p>
 * The library calls this interface; the caller provides the implementation.
 * Built-in implementations: {@link InMemorySnapshotRepository} (testing) and
 * {@link FileSnapshotRepository} (single-JVM production).
 * For distributed deployments, implement this against a database or Redis with
 * optimistic locking to prevent concurrent writes from different replicas.
 */
public interface SnapshotRepository {

    /**
     * Persist or overwrite a snapshot for the given execution.
     * For distributed deployments, implementations should use optimistic locking
     * (e.g. database {@code WHERE version = :expected}) to prevent two replicas
     * from committing conflicting snapshots simultaneously.
     *
     * @param executionId the business entity ID used as the storage key
     * @param snapshot    the snapshot to persist
     */
    void save(String executionId, ExecutionSnapshot snapshot);

    /**
     * Load the snapshot for the given execution, if one exists.
     *
     * @param executionId the business entity ID
     * @return the snapshot, or empty if never saved or already deleted
     */
    Optional<ExecutionSnapshot> load(String executionId);

    /**
     * Delete the snapshot for the given execution.
     * <p>
     * The library no longer calls this automatically — completed executions retain
     * their snapshot with status {@code COMPLETED} so that subsequent {@code trigger()}
     * or {@code proceed()} calls correctly throw {@code CompletedMachineException}.
     * Call this manually when you no longer need the execution record (e.g. after
     * archiving it, or as part of a scheduled cleanup job).
     */
    void delete(String executionId);

    /**
     * Return all snapshots with status {@code FAILED} or {@code RETRY_SCHEDULED}.
     * Called on startup by {@link io.hypercell.fsm.manager.StateMachineManager#recoverPendingRetries()}
     * to re-schedule retries that were in-flight when the process last stopped.
     * <p>
     * Implementations backed by a database should query by the status column.
     * Implementations that only scan the store and filter on {@code isFailed()} will
     * miss {@code RETRY_SCHEDULED} snapshots — those retries will not be recovered
     * on startup.
     */
    List<ExecutionSnapshot> listPendingRetries();

    /**
     * Return a page of interrupted executions (those with status {@code RUNNING}).
     * <p>
     * With the new status model, {@code RUNNING} means exclusively "actively processing
     * sub-steps" — a crash leaves the snapshot here. At-rest executions are now saved as
     * {@code WAITING}, so this query returns only genuinely interrupted rows (cheap indexed
     * scan).
     * <p>
     * Results are ordered by {@code executionId} ascending and keyset-paginated: pass the
     * last seen {@code executionId} as {@code afterExecutionId} (or {@code null} for the
     * first page). The caller keeps paginating until the returned list is smaller than
     * {@code limit}.
     * <p>
     * An implementation that does not support the interrupted-execution startup sweep
     * ({@code recoverInterruptedExecutions()}) may simply return an empty list. Lazy
     * recovery via {@code resume(executionId)} does not depend on this method.
     *
     * @param limit            maximum number of rows to return per page (must be &gt; 0)
     * @param afterExecutionId exclusive lower bound for keyset pagination; {@code null}
     *                         means start from the beginning
     * @return at most {@code limit} {@code RUNNING} snapshots ordered by execution ID;
     * never {@code null}
     */
    List<ExecutionSnapshot> listInterrupted(int limit, String afterExecutionId);
}
