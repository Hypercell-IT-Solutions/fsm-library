package io.hypercell.fsm.scope;

/**
 * The default {@link ExecutionScopeProvider}: establishes nothing and tears nothing down.
 * <p>
 * Used when no provider is configured on the builder, so the library can call the SPI
 * unconditionally instead of null-checking at every unit of work.
 */
public final class NoOpExecutionScopeProvider<C> implements ExecutionScopeProvider<C> {

    private static final ExecutionScope NO_OP = () -> {
        // nothing to tear down
    };

    private static final NoOpExecutionScopeProvider<?> INSTANCE = new NoOpExecutionScopeProvider<>();

    private NoOpExecutionScopeProvider() {
    }

    @SuppressWarnings("unchecked")
    public static <C> ExecutionScopeProvider<C> instance() {
        return (ExecutionScopeProvider<C>) INSTANCE;
    }

    @Override
    public ExecutionScope open(ExecutionScopeInfo<C> info) {
        return NO_OP;
    }
}
