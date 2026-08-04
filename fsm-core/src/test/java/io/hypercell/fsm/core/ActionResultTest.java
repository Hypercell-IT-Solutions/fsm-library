package io.hypercell.fsm.core;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ActionResultTest {

    @Test
    void success_hasSuccessStatus() {
        ActionResult r = ActionResult.success();
        assertThat(r.isSuccess()).isTrue();
        assertThat(r.isFailed()).isFalse();
        assertThat(r.isSkipped()).isFalse();
        assertThat(r.getOutput()).isEmpty();
        assertThat(r.getErrorMessage()).isNull();
        assertThat(r.getErrorType()).isNull();
    }

    @Test
    void successWithOutput_storesImmutableCopy() {
        Map<String, Object> output = Map.of("key", "value", "count", 42);
        ActionResult r = ActionResult.success(output);
        assertThat(r.isSuccess()).isTrue();
        assertThat(r.getOutput()).containsEntry("key", "value").containsEntry("count", 42);
        assertThat(r.getOutput()).isUnmodifiable();
    }

    @Test
    void failedFromThrowable_capturesMessageAndType() {
        RuntimeException ex = new RuntimeException("connection refused");
        ActionResult r = ActionResult.failed(ex);
        assertThat(r.isFailed()).isTrue();
        assertThat(r.getErrorMessage()).isEqualTo("connection refused");
        assertThat(r.getErrorType()).isEqualTo("java.lang.RuntimeException");
        assertThat(r.getOutput()).isEmpty();
    }

    @Test
    void failedFromThrowable_retainsTheLiveCause() {
        // FailurePolicy.onErrorType and RetryPolicy.shouldRetry branch on the real type,
        // so the throwable must survive alongside its stringified form.
        RuntimeException ex = new IllegalStateException("connection refused");
        ActionResult r = ActionResult.failed(ex);
        assertThat(r.getCause()).isSameAs(ex);
    }

    @Test
    void failedFromString_hasNullErrorType() {
        ActionResult r = ActionResult.failed("payment declined");
        assertThat(r.isFailed()).isTrue();
        assertThat(r.getErrorMessage()).isEqualTo("payment declined");
        assertThat(r.getErrorType()).isNull();
    }

    @Test
    void nonThrowableResults_haveNoCause() {
        assertThat(ActionResult.failed("payment declined").getCause()).isNull();
        assertThat(ActionResult.success().getCause()).isNull();
        assertThat(ActionResult.skipped().getCause()).isNull();
    }

    @Test
    void skipped_hasSkippedStatus() {
        ActionResult r = ActionResult.skipped();
        assertThat(r.isSkipped()).isTrue();
        assertThat(r.isSuccess()).isFalse();
        assertThat(r.isFailed()).isFalse();
    }

    @Test
    void getStatus_matchesFactory() {
        assertThat(ActionResult.success().getStatus()).isEqualTo(ActionResult.Status.SUCCESS);
        assertThat(ActionResult.failed("x").getStatus()).isEqualTo(ActionResult.Status.FAILED);
        assertThat(ActionResult.skipped().getStatus()).isEqualTo(ActionResult.Status.SKIPPED);
    }
}
