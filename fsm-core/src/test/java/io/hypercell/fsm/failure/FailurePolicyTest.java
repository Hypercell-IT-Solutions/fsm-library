package io.hypercell.fsm.failure;

import io.hypercell.fsm.OrderContext;
import io.hypercell.fsm.core.ActionResult;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the {@link FailurePolicy} combinators, exercised in isolation from a running
 * machine. The contract under test is that a policy returns {@code null} for "no opinion" and
 * that {@link FailurePolicy#orElse} chains on exactly that.
 */
class FailurePolicyTest {

    private static FailureContext.Builder<OrderContext> failureAt(String subStepName, int index) {
        return FailureContext.<OrderContext>builder()
                .executionId("exec-1")
                .machineDefinitionId("orders")
                .stateName("PROCESSING")
                .sourceStateName("PENDING")
                .triggerEvent("GO")
                .subStepName(subStepName)
                .subStepIndex(index)
                .firstSubStep(index == 0)
                .attemptNumber(1)
                .result(ActionResult.failed("boom"))
                .context(new OrderContext("exec-1"));
    }

    // ------------------------------------------------------------------ always

    @Test
    void always_returnsTheSameDispositionRegardlessOfFailure() {
        FailurePolicy<OrderContext> policy = FailurePolicy.always(FailureDisposition.ABORT);

        assertThat(policy.decide(failureAt("first", 0).build())).isEqualTo(FailureDisposition.ABORT);
        assertThat(policy.decide(failureAt("later", 7).build())).isEqualTo(FailureDisposition.ABORT);
    }

    // ------------------------------------------------------------------ onSubStep

    @Test
    void onSubStep_matchesByName_andDefersOtherwise() {
        FailurePolicy<OrderContext> policy =
                FailurePolicy.onSubStep("charge-payment", FailureDisposition.MANUAL);

        assertThat(policy.decide(failureAt("charge-payment", 1).build()))
                .isEqualTo(FailureDisposition.MANUAL);
        assertThat(policy.decide(failureAt("reserve-stock", 0).build()))
                .as("a non-matching sub-step must defer, not decide")
                .isNull();
    }

    // ------------------------------------------------------------------ onFirstSubStep

    @Test
    void onFirstSubStep_matchesIndexZeroOnly() {
        FailurePolicy<OrderContext> policy =
                FailurePolicy.onFirstSubStep(FailureDisposition.REWIND);

        assertThat(policy.decide(failureAt("reserve-stock", 0).build()))
                .isEqualTo(FailureDisposition.REWIND);
        assertThat(policy.decide(failureAt("charge-payment", 1).build())).isNull();
    }

    // ------------------------------------------------------------------ onErrorType

    @Test
    void onErrorType_matchesExactTypeAndSubtypes() {
        FailurePolicy<OrderContext> policy =
                FailurePolicy.onErrorType(IOException.class, FailureDisposition.RETRY);

        FailureContext<OrderContext> exact = failureAt("call", 0)
                .error(new IOException("connection reset")).build();
        FailureContext<OrderContext> subtype = failureAt("call", 0)
                .error(new java.io.FileNotFoundException("missing")).build();
        FailureContext<OrderContext> unrelated = failureAt("call", 0)
                .error(new IllegalArgumentException("bad")).build();

        assertThat(policy.decide(exact)).isEqualTo(FailureDisposition.RETRY);
        assertThat(policy.decide(subtype)).as("subtypes match").isEqualTo(FailureDisposition.RETRY);
        assertThat(policy.decide(unrelated)).isNull();
    }

    @Test
    void onErrorType_defersWhenThereIsNoException() {
        // ActionResult.failed(String) carries no throwable — the policy cannot match on type.
        FailurePolicy<OrderContext> policy =
                FailurePolicy.onErrorType(IOException.class, FailureDisposition.RETRY);

        assertThat(policy.decide(failureAt("call", 0).error(null).build())).isNull();
    }

    // ------------------------------------------------------------------ orElse

    @Test
    void orElse_fallsThroughUntilSomethingDecides() {
        FailurePolicy<OrderContext> policy =
                FailurePolicy.<OrderContext>onSubStep("reserve-stock", FailureDisposition.REWIND)
                        .orElse(FailurePolicy.onErrorType(IOException.class, FailureDisposition.RETRY))
                        .orElse(FailurePolicy.always(FailureDisposition.MANUAL));

        // first link matches
        assertThat(policy.decide(failureAt("reserve-stock", 0).build()))
                .isEqualTo(FailureDisposition.REWIND);
        // second link matches
        assertThat(policy.decide(failureAt("call", 1).error(new IOException("io")).build()))
                .isEqualTo(FailureDisposition.RETRY);
        // nothing matches until the terminal always()
        assertThat(policy.decide(failureAt("call", 1).error(new IllegalStateException("x")).build()))
                .isEqualTo(FailureDisposition.MANUAL);
    }

    @Test
    void orElse_doesNotConsultTheFallbackOnceADecisionIsMade() {
        FailurePolicy<OrderContext> shouldNotRun = f -> {
            throw new AssertionError("fallback must not be consulted after a decision");
        };

        FailurePolicy<OrderContext> policy =
                FailurePolicy.<OrderContext>always(FailureDisposition.ABORT).orElse(shouldNotRun);

        assertThat(policy.decide(failureAt("boom", 0).build())).isEqualTo(FailureDisposition.ABORT);
    }

    // ------------------------------------------------------------------ argument validation

    @Test
    void factories_rejectNullArguments() {
        assertThatThrownBy(() -> FailurePolicy.always(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("disposition");
        assertThatThrownBy(() -> FailurePolicy.onSubStep(null, FailureDisposition.RETRY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("subStepName");
        assertThatThrownBy(() -> FailurePolicy.onErrorType(null, FailureDisposition.RETRY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("errorType");
        assertThatThrownBy(() -> FailurePolicy.always(FailureDisposition.RETRY).orElse(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("next policy");
    }

    // ------------------------------------------------------------------ FailureContext

    @Test
    void failureContext_exposesEverythingAPolicyNeeds() {
        Throwable error = new IllegalStateException("stock service down");
        OrderContext ctx = new OrderContext("exec-9");

        FailureContext<OrderContext> failure = FailureContext.<OrderContext>builder()
                .executionId("exec-9")
                .machineDefinitionId("orders")
                .stateName("PROCESSING")
                .sourceStateName("PENDING")
                .triggerEvent("GO")
                .subStepName("reserve-stock")
                .subStepIndex(0)
                .firstSubStep(true)
                .committedSubSteps(false)
                .attemptNumber(3)
                .result(ActionResult.failed(error))
                .error(error)
                .context(ctx)
                .build();

        assertThat(failure.executionId()).isEqualTo("exec-9");
        assertThat(failure.machineDefinitionId()).isEqualTo("orders");
        assertThat(failure.stateName()).isEqualTo("PROCESSING");
        assertThat(failure.sourceStateName()).isEqualTo("PENDING");
        assertThat(failure.triggerEvent()).isEqualTo("GO");
        assertThat(failure.subStepName()).isEqualTo("reserve-stock");
        assertThat(failure.subStepIndex()).isZero();
        assertThat(failure.isFirstSubStep()).isTrue();
        assertThat(failure.hasCommittedSubSteps()).isFalse();
        assertThat(failure.attemptNumber()).isEqualTo(3);
        assertThat(failure.error()).isSameAs(error);
        assertThat(failure.context()).isSameAs(ctx);
        assertThat(failure.result().getErrorMessage()).isEqualTo("stock service down");
        assertThat(failure.result().getErrorType()).isEqualTo(IllegalStateException.class.getName());
    }
}
