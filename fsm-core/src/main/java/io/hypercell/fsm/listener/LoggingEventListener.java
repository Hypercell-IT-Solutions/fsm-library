package io.hypercell.fsm.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * A ready-to-use listener that prints a structured, human-readable execution log.
 * <p>
 * Useful during development and debugging. Wire it in via the builder:
 * <pre>{@code
 * StateMachine.<Ctx>define("my-machine")
 *     .listener(LoggingEventListener.withPrefix("[ORDER]"))
 *     ...
 * }</pre>
 * Example output:
 * [ORDER] 10:42:01 | CREATED    | order-42 | NEW | at 'PENDING' | attempt 0
 * [ORDER] 10:42:01 | ENTERED    | PROCESSING
 * [ORDER] 10:42:01 | STEP  ✓   | PROCESSING / charge-payment
 * [ORDER] 10:42:01 | STEP  ✗   | PROCESSING / notify-warehouse | Warehouse API is down
 * [ORDER] 10:42:01 | FAILED     | exec-abc123 | attempt 1
 * [ORDER] 10:42:06 | RESUMED    | PROCESSING / notify-warehouse | attempt 2
 * [ORDER] 10:42:06 | STEP  ↷   | PROCESSING / charge-payment   | (skipped — already done)
 * [ORDER] 10:42:06 | STEP  ✓   | PROCESSING / notify-warehouse
 * [ORDER] 10:42:06 | COMPLETED  | SHIPPED
 *
 * @param <C> the context type
 */
public class LoggingEventListener<C> implements MachineEventListener<C> {
    private static final Logger log = LoggerFactory.getLogger(LoggingEventListener.class);

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    private final String prefix;

    private LoggingEventListener(String prefix) {
        this.prefix = prefix;
    }

    public static <C> LoggingEventListener<C> withPrefix(String prefix) {
        return new LoggingEventListener<>(prefix);
    }

    public static <C> LoggingEventListener<C> create() {
        return new LoggingEventListener<>("[FSM]");
    }

    @Override
    public void onInstanceCreated(MachineEvent.InstanceCreatedEvent<C> e) {
        log("CREATED   ", String.format("%s | %s | at '%s' | attempt %d",
                e.getExecutionId(), e.getOrigin(), e.getCurrentStateName(),
                e.getAttemptNumber()), e);
    }

    @Override
    public void onTransitionFired(MachineEvent.TransitionFiredEvent<C> e) {
        log("TRANSITION", String.format("%-18s → %-18s via '%s'",
                e.getFromState(), e.getToState(), e.getEvent()), e);
    }

    @Override
    public void onStateEntered(MachineEvent.StateEnteredEvent<C> e) {
        log("ENTERED   ", e.getStateName(), e);
    }

    @Override
    public void onStateExited(MachineEvent.StateExitedEvent<C> e) {
        log("EXITED    ", e.getStateName(), e);
    }

    @Override
    public void onSubStepCompleted(MachineEvent.SubStepCompletedEvent<C> e) {
        log("STEP  ✓   ", e.getStateName() + " / " + e.getSubStepName(), e);
    }

    @Override
    public void onSubStepSkipped(MachineEvent.SubStepSkippedEvent<C> e) {
        log("STEP  ↷   ", e.getStateName() + " / " + e.getSubStepName()
                + " | (skipped — already done)", e);
    }

    @Override
    public void onSubStepFailed(MachineEvent.SubStepFailedEvent<C> e) {
        log("STEP  ✗   ", e.getStateName() + " / " + e.getSubStepName()
                + " | " + e.getErrorMessage(), e);
    }

    @Override
    public void onMachineCompleted(MachineEvent.MachineCompletedEvent<C> e) {
        log("COMPLETED ", e.getFinalStateName(), e);
    }

    @Override
    public void onMachineFailed(MachineEvent.MachineFailedEvent<C> e) {
        log("FAILED    ", String.format("%s | attempt %d | %d failure(s) | failed at '%s / %s' | %s",
                e.getExecutionId(), e.getFailureContext().attemptNumber(), e.getFailureCount(),
                e.getStateName(), e.getSubStepName(), e.getDisposition()), e);
    }

    @Override
    public void onMachineRewound(MachineEvent.MachineRewoundEvent<C> e) {
        log("REWOUND   ", String.format("%s / %s → parked at '%s' | re-fire '%s' | attempt %d",
                e.getFailedStateName(), e.getFailedSubStepName(),
                e.getRewoundToState(), e.getTriggerEvent(), e.getAttemptNumber()), e);
    }

    @Override
    public void onMachineResumed(MachineEvent.MachineResumedEvent<C> e) {
        log("RESUMED   ", String.format("%s / %s | attempt %d",
                e.getResumedAtState(), e.getResumedAtSubStep(), e.getAttemptNumber()), e);
    }

    private void log(String type, String detail, MachineEvent<C> event) {
        log.info("{} {} | {} | {}", prefix, FMT.format(event.getOccurredAt()), type, detail);
    }
}
