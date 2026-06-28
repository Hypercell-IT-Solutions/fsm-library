package io.hypercell.fsm.lock;

import io.hypercell.fsm.exception.ConcurrentExecutionException;

/**
 * SPI for per-execution mutual exclusion.
 * <p>
 * The library calls {@link #acquire(String)} at the start of every execution form
 * (trigger, proceed, initialize, resume, and recovery retries) and releases the returned
 * handle in a finally block. Implementations must be thread-safe.
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li>If the execution is not locked, return an {@link ExecutionLockHandle} that releases
 *       the lock on {@link ExecutionLockHandle#close()}.</li>
 *   <li>If the execution is already locked, throw {@link ConcurrentExecutionException}
 *       immediately — do not block the caller.</li>
 *   <li>Stale locks (held by a crashed process) must be reclaimed automatically after a
 *       configurable TTL so that no execution is locked permanently.</li>
 * </ul>
 *
 * <h2>Built-in implementations</h2>
 * <ul>
 *   <li>{@link ReentrantExecutionLockProvider} — single-JVM default; uses
 *       {@code ConcurrentHashMap&lt;String, ReentrantLock&gt;}. Zero additional dependencies.</li>
 *   <li>{@code JdbcExecutionLockProvider} (in {@code fsm-jdbc}) — distributed lock backed by
 *       a database row with TTL-based stale-lock takeover. Suitable for multi-replica
 *       deployments sharing a relational database.</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * Configure on the builder via {@code .executionLockProvider(provider)}; the library uses
 * {@link ReentrantExecutionLockProvider} by default when none is specified.
 */
public interface ExecutionLockProvider {

    /**
     * Acquire an exclusive lock for the given execution.
     *
     * @param executionId the business entity ID being locked
     * @return a handle that releases the lock when {@link ExecutionLockHandle#close()} is called
     * @throws ConcurrentExecutionException if the execution is already locked by another
     *                                      thread or process
     */
    ExecutionLockHandle acquire(String executionId);
}
