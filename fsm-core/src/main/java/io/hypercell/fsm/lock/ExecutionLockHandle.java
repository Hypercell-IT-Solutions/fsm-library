package io.hypercell.fsm.lock;

/**
 * A handle to an acquired execution lock. Release the lock by calling {@link #close()},
 * which is safe to use in a try-with-resources block.
 * <p>
 * Implementations must not throw from {@code close()} — any release failure must be
 * logged internally and swallowed so that finally blocks remain clean.
 */
public interface ExecutionLockHandle extends AutoCloseable {

    /**
     * Release the lock. Safe to call multiple times; subsequent calls are no-ops or
     * log a warning. Never throws a checked exception.
     */
    @Override
    void close();
}
