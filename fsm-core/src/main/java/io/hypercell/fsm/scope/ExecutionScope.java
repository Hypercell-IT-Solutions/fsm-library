package io.hypercell.fsm.scope;

/**
 * A handle to an open execution scope. Tear it down by calling {@link #close()}, which is safe
 * to use in a try-with-resources block.
 * <p>
 * Implementations must not throw from {@code close()} — any teardown failure must be logged
 * internally and swallowed so that finally blocks remain clean. Mirrors the contract of
 * {@link io.hypercell.fsm.lock.ExecutionLockHandle}.
 */
public interface ExecutionScope extends AutoCloseable {

    /**
     * Tear the scope down — clear the MDC keys, pop the tracing span, whatever was established
     * by {@link ExecutionScopeProvider#open}. Safe to call multiple times. Never throws a
     * checked exception.
     */
    @Override
    void close();
}
