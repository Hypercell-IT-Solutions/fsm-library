package io.hypercell.fsm.resume;

import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.execution.ExecutionRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionSnapshotTest {

    @Test
    void builder_populatesAllFields() {
        Instant now = Instant.now();
        ExecutionSnapshot snap = new ExecutionSnapshot.Builder()
                .executionId("exec-1")
                .machineDefinitionId("order")
                .currentStateName("PROCESSING")
                .failedStateName("PROCESSING")
                .failedSubStepName("charge")
                .lastTriggerEvent("APPROVE")
                .attemptNumber(2)
                .lastFailedAt(now)
                .lastErrorMessage("timeout")
                .status(SnapshotStatus.FAILED)
                .capturedAt(now)
                .build();

        assertThat(snap.getExecutionId()).isEqualTo("exec-1");
        assertThat(snap.getMachineDefinitionId()).isEqualTo("order");
        assertThat(snap.getCurrentStateName()).isEqualTo("PROCESSING");
        assertThat(snap.getFailedStateName()).isEqualTo("PROCESSING");
        assertThat(snap.getFailedSubStepName()).isEqualTo("charge");
        assertThat(snap.getLastTriggerEvent()).isEqualTo("APPROVE");
        assertThat(snap.getAttemptNumber()).isEqualTo(2);
        assertThat(snap.getLastFailedAt()).isEqualTo(now);
        assertThat(snap.getLastErrorMessage()).isEqualTo("timeout");
        assertThat(snap.getStatus()).isEqualTo(SnapshotStatus.FAILED);
        assertThat(snap.isFailed()).isTrue();
    }

    @Test
    void withStatus_returnsNewSnapshot_originalUnchanged() {
        ExecutionSnapshot original = new ExecutionSnapshot.Builder()
                .executionId("exec-1")
                .status(SnapshotStatus.FAILED)
                .capturedAt(Instant.now())
                .build();

        ExecutionSnapshot updated = original.withStatus(SnapshotStatus.RUNNING);

        assertThat(updated.getStatus()).isEqualTo(SnapshotStatus.RUNNING);
        assertThat(original.getStatus()).isEqualTo(SnapshotStatus.FAILED);
    }

    @Test
    void withAttemptNumber_returnsNewSnapshot_originalUnchanged() {
        ExecutionSnapshot original = new ExecutionSnapshot.Builder()
                .executionId("exec-1")
                .attemptNumber(1)
                .status(SnapshotStatus.FAILED)
                .capturedAt(Instant.now())
                .build();

        ExecutionSnapshot updated = original.withAttemptNumber(3);

        assertThat(updated.getAttemptNumber()).isEqualTo(3);
        assertThat(original.getAttemptNumber()).isEqualTo(1);
    }

    @Test
    void withScheduledRetryAt_setsStatusToRetryScheduled() {
        ExecutionSnapshot snap = new ExecutionSnapshot.Builder()
                .executionId("exec-1")
                .status(SnapshotStatus.FAILED)
                .capturedAt(Instant.now())
                .build();

        Instant retryAt = Instant.now().plusSeconds(30);
        ExecutionSnapshot updated = snap.withScheduledRetryAt(retryAt);

        assertThat(updated.getStatus()).isEqualTo(SnapshotStatus.RETRY_SCHEDULED);
        assertThat(updated.getScheduledRetryAt()).isEqualTo(retryAt);
    }

    @Test
    void isSubStepCompleted_checksCompletedResults() {
        ExecutionSnapshot snap = new ExecutionSnapshot.Builder()
                .executionId("exec-1")
                .status(SnapshotStatus.FAILED)
                .capturedAt(Instant.now())
                .completedSubStepResults(Map.of(
                        "PROCESSING::reserve", ActionResult.success(),
                        "PROCESSING::charge", ActionResult.success()
                ))
                .build();

        assertThat(snap.isSubStepCompleted("PROCESSING::reserve")).isTrue();
        assertThat(snap.isSubStepCompleted("PROCESSING::charge")).isTrue();
        assertThat(snap.isSubStepCompleted("PROCESSING::email")).isFalse();
    }

    @Test
    void checkpoint_factory_setsRunningStatusAndCurrentState() {
        ExecutionRecord record = new ExecutionRecord("exec-1", "PROCESSING");
        record.recordStep("PROCESSING", "reserve", ActionResult.success());
        record.setLastTriggerEvent("APPROVE");

        ExecutionSnapshot checkpoint = ExecutionSnapshot.checkpoint(record, "order");

        assertThat(checkpoint.getExecutionId()).isEqualTo("exec-1");
        assertThat(checkpoint.getMachineDefinitionId()).isEqualTo("order");
        assertThat(checkpoint.getCurrentStateName()).isEqualTo("PROCESSING");
        assertThat(checkpoint.getStatus()).isEqualTo(SnapshotStatus.RUNNING);
        assertThat(checkpoint.isRunning()).isTrue();
        assertThat(checkpoint.getCompletedSubStepResults()).containsKey("PROCESSING::reserve");
    }

    @Test
    void completedSubStepResults_isUnmodifiable() {
        ExecutionSnapshot snap = new ExecutionSnapshot.Builder()
                .executionId("exec-1")
                .status(SnapshotStatus.FAILED)
                .capturedAt(Instant.now())
                .build();

        assertThat(snap.getCompletedSubStepResults()).isUnmodifiable();
    }

    @Test
    void statusPredicates_reflectSnapshotStatus() {
        ExecutionSnapshot running = new ExecutionSnapshot.Builder()
                .executionId("e").status(SnapshotStatus.RUNNING).capturedAt(Instant.now()).build();
        ExecutionSnapshot waiting = new ExecutionSnapshot.Builder()
                .executionId("e").status(SnapshotStatus.WAITING).capturedAt(Instant.now()).build();
        ExecutionSnapshot terminated = new ExecutionSnapshot.Builder()
                .executionId("e").status(SnapshotStatus.TERMINATED).capturedAt(Instant.now()).build();

        assertThat(running.isRunning()).isTrue();
        assertThat(running.isFailed()).isFalse();
        assertThat(running.isWaiting()).isFalse();
        assertThat(running.isTerminated()).isFalse();

        assertThat(waiting.isWaiting()).isTrue();
        assertThat(waiting.isRunning()).isFalse();
        assertThat(waiting.isTerminated()).isFalse();

        assertThat(terminated.isTerminated()).isTrue();
        assertThat(terminated.isRunning()).isFalse();
        assertThat(terminated.isWaiting()).isFalse();
    }
}
