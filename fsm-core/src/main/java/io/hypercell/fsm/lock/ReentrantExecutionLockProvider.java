package io.hypercell.fsm.lock;

import io.hypercell.fsm.exception.ConcurrentExecutionException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Default {@link ExecutionLockProvider} that guards executions within a single JVM using
 * one {@link ReentrantLock} per execution ID, stored in a {@link ConcurrentHashMap}.
 * <p>
 * {@link #acquire(String)} uses {@code tryLock()} (non-blocking): if the lock is held by
 * another thread, {@link ConcurrentExecutionException} is thrown immediately rather than
 * blocking the caller. Lock map entries are cleaned up after each release to prevent
 * unbounded growth in long-running applications.
 * <p>
 * This implementation does <strong>not</strong> protect against concurrent access across
 * multiple JVM instances (distributed deployments). Use {@code JdbcExecutionLockProvider}
 * or a Redis-backed implementation for distributed mutual exclusion.
 */
public final class ReentrantExecutionLockProvider implements ExecutionLockProvider {

    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    @Override
    @SuppressWarnings("java:S2222")
    public ExecutionLockHandle acquire(String executionId) {
        ReentrantLock lock = locks.computeIfAbsent(executionId, k -> new ReentrantLock());
        if (!lock.tryLock()) {
            throw new ConcurrentExecutionException(executionId);
        }
        return () -> release(executionId, lock);
    }

    private void release(String executionId, ReentrantLock lock) {
        lock.unlock();
        locks.computeIfPresent(executionId, (k, l) -> l.hasQueuedThreads() ? l : null);
    }
}
