package io.hypercell.fsm.scope;

/**
 * SPI for per-execution ambient state — typically the SLF4J MDC, but equally a tracing span or
 * any other thread-local a consumer needs in place while a machine runs.
 * <p>
 * The library calls {@link #open} at the start of every unit of work (trigger, proceed, resume,
 * and each recovery retry) and closes the returned handle in a {@code finally}. Implementations
 * must be thread-safe.
 *
 * <h2>Why this exists alongside {@code onInstanceCreated}</h2>
 * {@link io.hypercell.fsm.listener.MachineEvent.InstanceCreatedEvent} tells a listener that an
 * execution is starting, which is enough to <em>set</em> a correlation ID — but an instance has no
 * single point of death, so no event reliably tells you when to <em>clear</em> it. That asymmetry
 * matters because {@code recoverFailedExecutions(...)} and {@code recoverInterruptedExecutions()}
 * run their work on the shared {@code recoveryExecutor}: a thread-local set and never cleared
 * leaks straight into whatever execution the pool runs next, silently mislabelling its logs.
 * <p>
 * This SPI makes teardown structural rather than a convention a consumer has to remember.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * .executionScopeProvider(info -> {
 *     MDC.put("executionId", info.executionId());
 *     MDC.put("correlationId", info.context().getCorrelationId());
 *     MDC.put("fsmOrigin", info.origin().name());
 *     return () -> {
 *         MDC.remove("executionId");
 *         MDC.remove("correlationId");
 *         MDC.remove("fsmOrigin");
 *     };
 * })
 * }</pre>
 * The default is {@link NoOpExecutionScopeProvider}, which costs nothing when unconfigured.
 *
 * @param <C> the context type flowing through the machine
 */
@FunctionalInterface
public interface ExecutionScopeProvider<C> {

    /**
     * Establish ambient state for one unit of work.
     * <p>
     * Called on the thread that will run the work, so anything thread-local set here is visible
     * to the sub-steps. An implementation that throws will fail the execution, so keep it cheap
     * and total.
     *
     * @param info what is known about the execution before the instance is built
     * @return a handle the library closes when the unit of work ends; never {@code null}
     */
    ExecutionScope open(ExecutionScopeInfo<C> info);
}
