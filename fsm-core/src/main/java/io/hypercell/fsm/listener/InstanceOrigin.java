package io.hypercell.fsm.listener;

/**
 * Why a {@link io.hypercell.fsm.core.StateMachineInstance} was created.
 * <p>
 * Carried on {@link MachineEvent.InstanceCreatedEvent}, this is the discriminator a listener needs
 * to tell ordinary traffic apart from recovery. It matters most for logging scope: the recovery
 * sweeps run their work on the shared {@code recoveryExecutor}, so a correlation ID established by
 * the original caller is not present on that thread and has to be re-established from the context.
 *
 * @see MachineEvent.InstanceCreatedEvent
 */
public enum InstanceOrigin {

    /**
     * A fresh execution from {@code newInstance(...)}. Nothing has been persisted yet and the
     * machine sits at its initial state.
     */
    NEW,

    /**
     * An at-rest {@code WAITING} execution rebuilt from its snapshot so a new event can be
     * triggered against it. No sub-step re-runs — the instance is positioned, not resumed.
     */
    RECONSTITUTED,

    /**
     * A {@code FAILED} execution rebuilt so it can continue from the sub-step that failed.
     * Produced by {@code proceed(...)}, by the auto-retry coordinator, and by
     * {@code recoverFailedExecutions(...)}.
     */
    RESUMED_FAILED,

    /**
     * A {@code RUNNING} execution rebuilt after the process that owned it died mid-transition.
     * Produced by {@code resume(...)} and {@code recoverInterruptedExecutions()}.
     */
    RESUMED_INTERRUPTED
}
