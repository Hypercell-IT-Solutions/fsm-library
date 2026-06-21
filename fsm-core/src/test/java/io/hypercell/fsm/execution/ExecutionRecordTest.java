package io.hypercell.fsm.execution;

import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.core.ExecutionStatus;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionRecordTest {

    @Test
    void initialState_isRunningAndEmpty() {
        ExecutionRecord record = new ExecutionRecord("exec-1", "PENDING");
        assertThat(record.getExecutionId()).isEqualTo("exec-1");
        assertThat(record.getCurrentStateName()).isEqualTo("PENDING");
        assertThat(record.getStatus()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(record.getSteps()).isEmpty();
        assertThat(record.getFailedStateName()).isNull();
        assertThat(record.getFailedSubStepName()).isNull();
    }

    @Test
    void recordStep_appendsAndIsQueryable() {
        ExecutionRecord record = new ExecutionRecord("exec-1", "PROCESSING");
        record.recordStep("PROCESSING", "step-a", ActionResult.success());
        record.recordStep("PROCESSING", "step-b", ActionResult.failed("oops"));

        assertThat(record.getSteps()).hasSize(2);
        assertThat(record.isSubStepCompleted("PROCESSING", "step-a")).isTrue();
        assertThat(record.isSubStepCompleted("PROCESSING", "step-b")).isFalse();
    }

    @Test
    void resultOf_returnsLatestResult() {
        ExecutionRecord record = new ExecutionRecord("exec-1", "PROCESSING");
        record.recordStep("PROCESSING", "step-a", ActionResult.failed("first attempt"));
        record.recordStep("PROCESSING", "step-a", ActionResult.success());

        assertThat(record.resultOf("PROCESSING", "step-a"))
                .isPresent()
                .hasValueSatisfying(r -> assertThat(r.isSuccess()).isTrue());
    }

    @Test
    void markFailed_setsFailureAndStatus() {
        ExecutionRecord record = new ExecutionRecord("exec-1", "PROCESSING");
        record.markFailed("PROCESSING", "step-a");

        assertThat(record.getStatus()).isEqualTo(ExecutionStatus.FAILED);
        assertThat(record.getFailedStateName()).isEqualTo("PROCESSING");
        assertThat(record.getFailedSubStepName()).isEqualTo("step-a");
    }

    @Test
    void clearFailure_resetsFailureFieldsToRunning() {
        ExecutionRecord record = new ExecutionRecord("exec-1", "PROCESSING");
        record.markFailed("PROCESSING", "step-a");
        record.clearFailure();

        assertThat(record.getStatus()).isEqualTo(ExecutionStatus.RUNNING);
        assertThat(record.getFailedStateName()).isNull();
        assertThat(record.getFailedSubStepName()).isNull();
    }

    @Test
    void markTerminated_clearsFailureFields() {
        ExecutionRecord record = new ExecutionRecord("exec-1", "DONE");
        record.markFailed("DONE", "step-x");
        record.markTerminated();

        assertThat(record.getStatus()).isEqualTo(ExecutionStatus.TERMINATED);
        assertThat(record.getFailedStateName()).isNull();
        assertThat(record.getFailedSubStepName()).isNull();
    }

    @Test
    void steps_listIsUnmodifiable() {
        ExecutionRecord record = new ExecutionRecord("exec-1", "PENDING");
        assertThat(record.getSteps()).isUnmodifiable();
    }

    @Test
    void resultOf_returnsEmptyWhenNoRecord() {
        ExecutionRecord record = new ExecutionRecord("exec-1", "PROCESSING");
        assertThat(record.resultOf("PROCESSING", "unknown-step")).isEmpty();
    }
}