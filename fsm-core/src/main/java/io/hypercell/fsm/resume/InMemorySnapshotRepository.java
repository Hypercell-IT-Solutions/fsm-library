package io.hypercell.fsm.resume;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link SnapshotRepository} backed by a {@link java.util.concurrent.ConcurrentHashMap}.
 * <p>
 * Intended for unit tests and single-run scripts only. Data is lost on JVM shutdown.
 * Use {@link FileSnapshotRepository} or a custom DB/Redis implementation for production.
 * <p>
 * THREAD SAFETY: thread-safe within a single JVM.
 * <p>
 * KNOWN LIMITATION: {@link #listPendingRetries()} currently only returns snapshots with
 * status {@code FAILED}. Snapshots with status {@code RETRY_SCHEDULED} are not included,
 * so {@code StateMachineManager.recoverPendingRetries()} will not recover previously
 * scheduled (but not yet fired) retries after a restart.
 */
public class InMemorySnapshotRepository implements SnapshotRepository {
    private final Map<String, ExecutionSnapshot> store = new ConcurrentHashMap<>();

    @Override
    public void save(String id, ExecutionSnapshot s) {
        store.put(id, s);
    }

    @Override
    public Optional<ExecutionSnapshot> load(String id) {
        return Optional.ofNullable(store.get(id));
    }

    @Override
    public void delete(String id) {
        store.remove(id);
    }

    @Override
    public List<ExecutionSnapshot> listPendingRetries() {
        return store.values().stream()
                .filter(s -> s.isFailed() || s.getStatus() == SnapshotStatus.RETRY_SCHEDULED)
                .toList();
    }

    @Override
    public List<ExecutionSnapshot> listInterrupted(int limit, String afterExecutionId) {
        return store.values().stream()
                .filter(ExecutionSnapshot::isRunning)
                .sorted(Comparator.comparing(ExecutionSnapshot::getExecutionId))
                .filter(s -> afterExecutionId == null
                        || s.getExecutionId().compareTo(afterExecutionId) > 0)
                .limit(limit)
                .toList();
    }

    @Override
    public List<ExecutionSnapshot> listFailed(int limit, String afterExecutionId, int maxAttempts) {
        return store.values().stream()
                .filter(s -> s.isFailed() && s.getAttemptNumber() < maxAttempts)
                .sorted(Comparator.comparing(ExecutionSnapshot::getExecutionId))
                .filter(s -> afterExecutionId == null
                        || s.getExecutionId().compareTo(afterExecutionId) > 0)
                .limit(limit)
                .toList();
    }

    /**
     * Returns the number of snapshots currently held in memory. Useful in tests.
     */
    public int size() {
        return store.size();
    }

    /**
     * Factory method; equivalent to {@code new InMemorySnapshotRepository()}.
     */
    public static InMemorySnapshotRepository create() {
        return new InMemorySnapshotRepository();
    }
}
