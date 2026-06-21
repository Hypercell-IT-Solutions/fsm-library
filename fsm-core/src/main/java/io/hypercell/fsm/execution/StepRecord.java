package io.hypercell.fsm.execution;

import io.hypercell.fsm.core.ActionResult;

import java.time.Instant;

/**
 * An immutable record of a single sub-step execution.
 * <p>
 * The ExecutionRecord is a list of these — it's the machine's append-only log.
 * When we need to know "did sub-step X in state Y already succeed?", we scan
 * this list.
 * <p>
 * IMMUTABILITY:
 * Once recorded, a StepRecord never changes. Retried sub-steps produce new
 * StepRecord entries — they don't overwrite the old one. This gives us a full
 * audit trail: we can see that step X failed at 10:00 and succeeded at 10:05.
 */
public final class StepRecord {

    private final String stateName;
    private final String subStepName;
    private final ActionResult result;
    private final Instant executedAt;

    public StepRecord(String stateName, String subStepName,
                      ActionResult result, Instant executedAt) {
        this.stateName = stateName;
        this.subStepName = subStepName;
        this.result = result;
        this.executedAt = executedAt;
    }

    public String getStateName() {
        return stateName;
    }

    public String getSubStepName() {
        return subStepName;
    }

    public ActionResult getResult() {
        return result;
    }

    public Instant getExecutedAt() {
        return executedAt;
    }

    /**
     * Composite key used to look up this record in a snapshot.
     * The key format is {@code "stateName::subStepName"} (e.g., {@code "PROCESSING::charge"}).
     * Using a composite key prevents collisions when a snapshot accumulates completed steps
     * across multiple states and ensures unambiguous identification.
     */
    public String compositeKey() {
        return stateName + "::" + subStepName;
    }

    @Override
    public String toString() {
        return "StepRecord{state='" + stateName + "', subStep='" + subStepName +
                "', result=" + result + ", at=" + executedAt + "}";
    }
}
