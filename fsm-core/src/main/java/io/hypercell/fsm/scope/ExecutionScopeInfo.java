package io.hypercell.fsm.scope;

import io.hypercell.fsm.listener.InstanceOrigin;

/**
 * What is known about an execution at the moment its scope opens.
 * <p>
 * Deliberately smaller than {@link io.hypercell.fsm.listener.MachineEvent.InstanceCreatedEvent}:
 * the scope has to open <em>before</em> the machine instance is constructed — creating a fresh
 * instance already enters the initial state and runs its sub-steps — so only these four fields
 * are reliably known. For the richer view, listen for {@code InstanceCreatedEvent}, which is
 * published from inside the scope.
 *
 * @param <C> the context type flowing through the machine
 */
public final class ExecutionScopeInfo<C> {

    private final String executionId;
    private final String machineId;
    private final InstanceOrigin origin;
    private final C context;

    public ExecutionScopeInfo(String executionId, String machineId,
                              InstanceOrigin origin, C context) {
        this.executionId = executionId;
        this.machineId = machineId;
        this.origin = origin;
        this.context = context;
    }

    public String executionId() {
        return executionId;
    }

    public String machineId() {
        return machineId;
    }

    /**
     * Why this execution is running — the discriminator that separates ordinary traffic from the
     * recovery sweeps, which run on the shared {@code recoveryExecutor}. Never {@code null}.
     */
    public InstanceOrigin origin() {
        return origin;
    }

    /**
     * The live domain object. Read-only from a scope provider, like everywhere else it is exposed.
     * May be {@code null} if no {@code ContextLoader} is configured and none was supplied.
     */
    public C context() {
        return context;
    }
}
