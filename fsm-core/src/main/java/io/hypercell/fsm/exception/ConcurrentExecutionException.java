package io.hypercell.fsm.exception;

/**
 * Thrown by StateMachineManager when a second request arrives for an executionId
 * that is already being processed by another thread or process.
 * <p>
 * WHAT THIS PROTECTS:
 * Without this guard, two simultaneous requests for the same workflow could both load
 * the snapshot, both reconstitute an instance, and both call trigger(). The second
 * write would silently overwrite the first checkpoint — leaving the machine in an
 * inconsistent state.
 * <p>
 * SCOPE:
 * The default {@link io.hypercell.fsm.lock.ReentrantExecutionLockProvider} protects
 * within a single JVM only. For distributed deployments, configure
 * {@code JdbcExecutionLockProvider} (or another cross-process implementation) on the
 * builder via {@code .executionLockProvider(provider)} to extend protection across
 * multiple replicas. Stale locks from crashed processes are reclaimed automatically
 * after a configurable TTL.
 * <p>
 * CALLER GUIDANCE:
 * Catch this exception at the HTTP layer and return HTTP 409 Conflict. The client
 * should retry after a short delay.
 */
public class ConcurrentExecutionException extends StateMachineException {
    private final String executionId;

    public ConcurrentExecutionException(String executionId) {
        super(String.format(
                "Execution '%s' is already being processed by another request. " +
                        "Retry after the in-flight request completes.", executionId));
        this.executionId = executionId;
    }

    public String getExecutionId() {
        return executionId;
    }
}
