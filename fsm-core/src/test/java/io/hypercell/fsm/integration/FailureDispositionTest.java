package io.hypercell.fsm.integration;

import io.hypercell.fsm.OrderContext;
import io.hypercell.fsm.StateMachine;
import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.core.StateMachineDefinition;
import io.hypercell.fsm.exception.ExecutionAbortedException;
import io.hypercell.fsm.exception.IllegalTriggerStateException;
import io.hypercell.fsm.failure.FailureDisposition;
import io.hypercell.fsm.failure.FailurePolicy;
import io.hypercell.fsm.manager.StateMachineManager;
import io.hypercell.fsm.manager.TriggerEligibility;
import io.hypercell.fsm.resume.ExecutionSnapshot;
import io.hypercell.fsm.resume.InMemorySnapshotRepository;
import io.hypercell.fsm.resume.SnapshotStatus;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for {@link FailureDisposition} — how a classified failure changes what is
 * persisted and which recovery paths will touch the execution.
 * <p>
 * The shape throughout mirrors the motivating case: a state whose <em>first</em> sub-step
 * reserves something external (nothing committed yet, so a failure should rewind the whole
 * transition) and whose <em>later</em> sub-steps have committed work (so a failure should resume
 * from the failure point, exactly as the library always behaved).
 */
class FailureDispositionTest {

    // ------------------------------------------------------------------ fixture

    /**
     * Builds the two-sub-step machine used by most tests.
     * <p>
     * {@code PENDING --GO--> PROCESSING[reserve-stock, charge-payment]}, where each sub-step
     * fails or succeeds according to the supplied flags, under the given state-level policy.
     */
    private static StateMachineDefinition<OrderContext> machine(
            String id,
            InMemorySnapshotRepository repo,
            FailurePolicy<OrderContext> policy,
            AtomicBoolean reserveFails,
            AtomicBoolean chargeFails,
            AtomicInteger reserveRuns,
            ExecutorService executor) {

        return StateMachine.<OrderContext>define(id)
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(OrderContext::new)
                .recoveryExecutor(executor)
                .state("PENDING")
                .on("GO").to("PROCESSING").end()
                .and()
                .state("PROCESSING")
                .failurePolicy(policy)
                .subStep("reserve-stock", ctx -> {
                    reserveRuns.incrementAndGet();
                    if (reserveFails.get()) throw new IllegalStateException("stock service down");
                    ctx.setReservationId("RES-1");
                    return ActionResult.success();
                })
                .subStep("charge-payment", ctx -> {
                    if (chargeFails.get()) throw new IllegalStateException("payment gateway down");
                    ctx.setPaymentCharged(true);
                    return ActionResult.success();
                })
                .on("DONE").to("SHIPPED").end()
                .and()
                .state("SHIPPED").terminal().and()
                .build();
    }

    private static void triggerExpectingFailure(StateMachineManager<OrderContext> manager,
                                                String executionId, String event) {
        var result = manager.trigger(executionId, event);
        assertThat(result.isFailed()).isTrue();
    }

    // ------------------------------------------------------------------ REWIND

    /**
     * The motivating case. A failure in the first sub-step is rewound: the execution is parked
     * WAITING back at PENDING, no sweep picks it up, and re-firing the same event runs the state
     * again from its first sub-step.
     */
    @Test
    void rewind_parksAtSourceState_excludedFromSweep_andIsRetriggerable() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean reserveFails = new AtomicBoolean(true);
        AtomicBoolean chargeFails = new AtomicBoolean(false);
        AtomicInteger reserveRuns = new AtomicInteger();

        StateMachineDefinition<OrderContext> definition = machine("rewind-basic", repo,
                FailurePolicy.onFirstSubStep(FailureDisposition.REWIND),
                reserveFails, chargeFails, reserveRuns, executor);
        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        manager.initialize("order-1");
        triggerExpectingFailure(manager, "order-1", "GO");

        // Parked back at the source state, not at the state that failed.
        ExecutionSnapshot snapshot = repo.load("order-1").orElseThrow();
        assertThat(snapshot.getStatus()).isEqualTo(SnapshotStatus.WAITING);
        assertThat(snapshot.getCurrentStateName()).isEqualTo("PENDING");
        assertThat(snapshot.getFailureDisposition()).isEqualTo(FailureDisposition.REWIND);

        // The failure is still on the record, so a rewound execution stays diagnosable.
        assertThat(snapshot.getFailedStateName()).isEqualTo("PROCESSING");
        assertThat(snapshot.getFailedSubStepName()).isEqualTo("reserve-stock");
        assertThat(snapshot.getLastErrorMessage()).isEqualTo("stock service down");
        assertThat(snapshot.getLastErrorType()).isEqualTo(IllegalStateException.class.getName());

        // Invisible to the sweep.
        assertThat(repo.listFailed(10, null, 5)).isEmpty();
        assertThat(manager.recoverFailedExecutions(5)).isZero();

        // And re-triggerable — which is the whole point of REWIND.
        assertThat(manager.eligibilityOf("order-1")).isEqualTo(TriggerEligibility.READY);
        reserveFails.set(false);
        var retry = manager.trigger("order-1", "GO");

        assertThat(retry.isFailed()).isFalse();
        assertThat(retry.getToState()).isEqualTo("PROCESSING");
        assertThat(reserveRuns.get()).as("reserve-stock re-runs from the top on re-trigger").isEqualTo(2);
        assertThat(repo.load("order-1").orElseThrow().getStatus()).isEqualTo(SnapshotStatus.WAITING);

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * The same state, failing on a <em>later</em> sub-step, must keep the historical behaviour:
     * FAILED with disposition RETRY, swept normally, and resuming skips what already committed.
     * This is the requirement that motivated per-sub-step classification in the first place.
     */
    @Test
    void sameState_laterSubStepFailure_staysRetryable_andResumesFromFailurePoint() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean reserveFails = new AtomicBoolean(false);
        AtomicBoolean chargeFails = new AtomicBoolean(true);
        AtomicInteger reserveRuns = new AtomicInteger();

        StateMachineDefinition<OrderContext> definition = machine("rewind-later-step", repo,
                FailurePolicy.onFirstSubStep(FailureDisposition.REWIND),
                reserveFails, chargeFails, reserveRuns, executor);
        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        manager.initialize("order-2");
        triggerExpectingFailure(manager, "order-2", "GO");

        ExecutionSnapshot snapshot = repo.load("order-2").orElseThrow();
        assertThat(snapshot.getStatus()).isEqualTo(SnapshotStatus.FAILED);
        assertThat(snapshot.getCurrentStateName()).isEqualTo("PROCESSING");
        assertThat(snapshot.getFailureDisposition()).isEqualTo(FailureDisposition.RETRY);
        assertThat(snapshot.getFailedSubStepName()).isEqualTo("charge-payment");

        // Swept, unlike the REWIND case.
        assertThat(repo.listFailed(10, null, 5)).hasSize(1);

        // proceed() skips the committed first step and re-runs only the failed one.
        chargeFails.set(false);
        var resumed = manager.proceed("order-2");

        assertThat(resumed.isFailed()).isFalse();
        assertThat(reserveRuns.get()).as("reserve-stock already committed, so it is skipped").isEqualTo(1);

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * A rewind that would discard committed work is downgraded to MANUAL rather than honoured.
     * Here the policy asks to rewind unconditionally, but {@code charge-payment} fails after
     * {@code reserve-stock} already succeeded.
     */
    @Test
    void rewind_afterCommittedSubStep_downgradesToManual() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean reserveFails = new AtomicBoolean(false);
        AtomicBoolean chargeFails = new AtomicBoolean(true);

        StateMachineDefinition<OrderContext> definition = machine("rewind-unsafe", repo,
                FailurePolicy.always(FailureDisposition.REWIND),
                reserveFails, chargeFails, new AtomicInteger(), executor);
        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        manager.initialize("order-3");
        triggerExpectingFailure(manager, "order-3", "GO");

        ExecutionSnapshot snapshot = repo.load("order-3").orElseThrow();
        assertThat(snapshot.getFailureDisposition())
                .as("unsafe REWIND must fall back to MANUAL, not silently discard the reservation")
                .isEqualTo(FailureDisposition.MANUAL);
        assertThat(snapshot.getStatus()).isEqualTo(SnapshotStatus.FAILED);
        assertThat(snapshot.getCurrentStateName()).isEqualTo("PROCESSING");

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * A rewind with no source state to return to — the failure happened on the initial state
     * during {@code initialize()} — is likewise downgraded to MANUAL.
     */
    @Test
    void rewind_onInitialState_downgradesToManual() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("rewind-initial")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(OrderContext::new)
                .failurePolicy(FailurePolicy.always(FailureDisposition.REWIND))
                .state("PENDING")
                .subStep("validate", ctx -> {
                    throw new IllegalStateException("bad input");
                })
                .on("GO").to("SHIPPED").end()
                .and()
                .state("SHIPPED").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        var result = manager.initialize("order-4");
        assertThat(result.isFailed()).isTrue();

        ExecutionSnapshot snapshot = repo.load("order-4").orElseThrow();
        assertThat(snapshot.getFailureDisposition()).isEqualTo(FailureDisposition.MANUAL);
        assertThat(snapshot.getStatus()).isEqualTo(SnapshotStatus.FAILED);
    }

    /**
     * attemptNumber accumulates across repeated rewind → re-trigger cycles; it is never reset.
     */
    @Test
    void rewind_bumpsAttemptNumber_andNeverResetsIt() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean reserveFails = new AtomicBoolean(true);

        StateMachineDefinition<OrderContext> definition = machine("rewind-attempts", repo,
                FailurePolicy.onFirstSubStep(FailureDisposition.REWIND),
                reserveFails, new AtomicBoolean(false), new AtomicInteger(), executor);
        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        manager.initialize("order-5");

        triggerExpectingFailure(manager, "order-5", "GO");
        assertThat(repo.load("order-5").orElseThrow().getAttemptNumber()).isEqualTo(2);

        triggerExpectingFailure(manager, "order-5", "GO");
        assertThat(repo.load("order-5").orElseThrow().getAttemptNumber()).isEqualTo(3);

        triggerExpectingFailure(manager, "order-5", "GO");
        assertThat(repo.load("order-5").orElseThrow().getAttemptNumber()).isEqualTo(4);

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    // ------------------------------------------------------------------ MANUAL

    /**
     * MANUAL keeps the execution FAILED and resumable, but hides it from every automatic sweep.
     */
    @Test
    void manual_excludedFromSweep_butProceedStillWorks() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean chargeFails = new AtomicBoolean(true);

        StateMachineDefinition<OrderContext> definition = machine("manual-basic", repo,
                FailurePolicy.always(FailureDisposition.MANUAL),
                new AtomicBoolean(false), chargeFails, new AtomicInteger(), executor);
        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        manager.initialize("order-6");
        triggerExpectingFailure(manager, "order-6", "GO");

        ExecutionSnapshot snapshot = repo.load("order-6").orElseThrow();
        assertThat(snapshot.getStatus()).isEqualTo(SnapshotStatus.FAILED);
        assertThat(snapshot.getFailureDisposition()).isEqualTo(FailureDisposition.MANUAL);

        assertThat(repo.listFailed(10, null, 5)).as("MANUAL rows are never swept").isEmpty();
        assertThat(manager.recoverFailedExecutions(5)).isZero();

        // trigger() is still refused — MANUAL means "resume from the failure point", not "re-fire".
        assertThat(manager.eligibilityOf("order-6")).isEqualTo(TriggerEligibility.NEEDS_PROCEED);
        assertThatThrownBy(() -> manager.trigger("order-6", "GO"))
                .isInstanceOf(IllegalTriggerStateException.class);

        // An explicit proceed() is exactly what MANUAL waits for.
        chargeFails.set(false);
        assertThat(manager.proceed("order-6").isFailed()).isFalse();

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    // ------------------------------------------------------------------ ABORT

    /**
     * ABORT is permanent: excluded from the sweep and refused by {@code proceed()}.
     */
    @Test
    void abort_excludedFromSweep_andProceedThrows() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        StateMachineDefinition<OrderContext> definition = machine("abort-basic", repo,
                FailurePolicy.always(FailureDisposition.ABORT),
                new AtomicBoolean(false), new AtomicBoolean(true), new AtomicInteger(), executor);
        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        manager.initialize("order-7");
        triggerExpectingFailure(manager, "order-7", "GO");

        ExecutionSnapshot snapshot = repo.load("order-7").orElseThrow();
        assertThat(snapshot.getStatus()).isEqualTo(SnapshotStatus.FAILED);
        assertThat(snapshot.getFailureDisposition()).isEqualTo(FailureDisposition.ABORT);

        assertThat(repo.listFailed(10, null, 5)).isEmpty();
        assertThat(manager.recoverFailedExecutions(5)).isZero();
        assertThat(manager.eligibilityOf("order-7")).isEqualTo(TriggerEligibility.ABORTED);

        assertThatThrownBy(() -> manager.proceed("order-7"))
                .isInstanceOf(ExecutionAbortedException.class)
                .hasMessageContaining("PROCESSING")
                .hasMessageContaining("charge-payment");

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    // ------------------------------------------------------------------ default & chain

    /**
     * With no policy configured anywhere, behaviour is unchanged: RETRY, swept as before.
     */
    @Test
    void noPolicy_defaultsToRetry_andIsSwept() throws Exception {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();
        ExecutorService executor = Executors.newSingleThreadExecutor();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("no-policy")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(OrderContext::new)
                .recoveryExecutor(executor)
                .state("PENDING")
                .on("GO").to("PROCESSING").end()
                .and()
                .state("PROCESSING")
                .subStep("charge-payment", ctx -> {
                    throw new IllegalStateException("boom");
                })
                .and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        manager.initialize("order-8");
        triggerExpectingFailure(manager, "order-8", "GO");

        ExecutionSnapshot snapshot = repo.load("order-8").orElseThrow();
        assertThat(snapshot.getFailureDisposition()).isEqualTo(FailureDisposition.RETRY);
        assertThat(repo.listFailed(10, null, 5)).hasSize(1);

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * The sub-step policy wins over the state policy, which wins over the machine policy.
     */
    @Test
    void policyChain_subStepBeatsState_stateBeatsMachine() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("chain")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(OrderContext::new)
                .failurePolicy(FailurePolicy.always(FailureDisposition.ABORT))
                .state("PENDING")
                .on("A").to("STATE_POLICY").end()
                .on("B").to("SUB_STEP_POLICY").end()
                .on("C").to("MACHINE_POLICY").end()
                .and()
                // state-level policy overrides the machine's ABORT
                .state("STATE_POLICY")
                .failurePolicy(FailurePolicy.always(FailureDisposition.MANUAL))
                .subStep("boom", ctx -> {
                    throw new IllegalStateException("boom");
                })
                .and()
                // sub-step-level policy overrides both
                .state("SUB_STEP_POLICY")
                .failurePolicy(FailurePolicy.always(FailureDisposition.MANUAL))
                .subStep("boom", ctx -> {
                    throw new IllegalStateException("boom");
                }, FailurePolicy.always(FailureDisposition.RETRY))
                .and()
                // nothing local, so the machine policy applies
                .state("MACHINE_POLICY")
                .subStep("boom", ctx -> {
                    throw new IllegalStateException("boom");
                })
                .and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        manager.initialize("chain-a");
        triggerExpectingFailure(manager, "chain-a", "A");
        assertThat(repo.load("chain-a").orElseThrow().getFailureDisposition())
                .isEqualTo(FailureDisposition.MANUAL);

        manager.initialize("chain-b");
        triggerExpectingFailure(manager, "chain-b", "B");
        assertThat(repo.load("chain-b").orElseThrow().getFailureDisposition())
                .isEqualTo(FailureDisposition.RETRY);

        manager.initialize("chain-c");
        triggerExpectingFailure(manager, "chain-c", "C");
        assertThat(repo.load("chain-c").orElseThrow().getFailureDisposition())
                .isEqualTo(FailureDisposition.ABORT);
    }

    /**
     * A policy returning {@code null} defers to the next level instead of forcing a decision.
     */
    @Test
    void policyReturningNull_defersToNextLevel() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("defer")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(OrderContext::new)
                .failurePolicy(FailurePolicy.always(FailureDisposition.ABORT))
                .state("PENDING")
                .on("GO").to("PROCESSING").end()
                .and()
                .state("PROCESSING")
                // only opinionated about a sub-step that is not the one that fails
                .failurePolicy(FailurePolicy.onSubStep("some-other-step", FailureDisposition.MANUAL))
                .subStep("boom", ctx -> {
                    throw new IllegalStateException("boom");
                })
                .and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        manager.initialize("defer-1");
        triggerExpectingFailure(manager, "defer-1", "GO");

        assertThat(repo.load("defer-1").orElseThrow().getFailureDisposition())
                .as("state policy had no opinion, so the machine policy applies")
                .isEqualTo(FailureDisposition.ABORT);
    }

    /**
     * The policy sees the real exception type, not a wrapper — this is what
     * {@link FailurePolicy#onErrorType} depends on.
     */
    @Test
    void onErrorType_seesOriginalException() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("error-type")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(OrderContext::new)
                .failurePolicy(FailurePolicy.<OrderContext>onErrorType(
                                NumberFormatException.class, FailureDisposition.ABORT)
                        .orElse(FailurePolicy.always(FailureDisposition.RETRY)))
                .state("PENDING")
                .on("BAD").to("BAD_INPUT").end()
                .on("FLAKY").to("FLAKY_IO").end()
                .and()
                .state("BAD_INPUT")
                .subStep("parse", ctx -> {
                    throw new NumberFormatException("not a number");
                })
                .and()
                .state("FLAKY_IO")
                .subStep("call", ctx -> {
                    throw new IllegalStateException("timeout");
                })
                .and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        manager.initialize("err-1");
        triggerExpectingFailure(manager, "err-1", "BAD");
        assertThat(repo.load("err-1").orElseThrow().getFailureDisposition())
                .isEqualTo(FailureDisposition.ABORT);

        manager.initialize("err-2");
        triggerExpectingFailure(manager, "err-2", "FLAKY");
        assertThat(repo.load("err-2").orElseThrow().getFailureDisposition())
                .isEqualTo(FailureDisposition.RETRY);
    }

    /**
     * A policy that throws must not take the execution down with it — the failure is recorded
     * with the default disposition instead.
     */
    @Test
    void throwingPolicy_isTreatedAsNoOpinion() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("bad-policy")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(OrderContext::new)
                .state("PENDING")
                .on("GO").to("PROCESSING").end()
                .and()
                .state("PROCESSING")
                .failurePolicy(f -> {
                    throw new IllegalStateException("policy is broken");
                })
                .subStep("boom", ctx -> {
                    throw new IllegalStateException("boom");
                })
                .and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        manager.initialize("bad-1");
        triggerExpectingFailure(manager, "bad-1", "GO");

        assertThat(repo.load("bad-1").orElseThrow().getFailureDisposition())
                .isEqualTo(FailureDisposition.RETRY);
    }

    /**
     * The disposition reaches the caller on the result, so an HTTP layer can pick a status code
     * without reloading the snapshot.
     */
    @Test
    void managedTransitionResult_carriesDisposition() {
        InMemorySnapshotRepository repo = InMemorySnapshotRepository.create();

        StateMachineDefinition<OrderContext> definition = StateMachine.<OrderContext>define("result-disposition")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(OrderContext::new)
                .state("PENDING")
                .on("GO").to("PROCESSING").end()
                .and()
                .state("PROCESSING")
                .failurePolicy(FailurePolicy.always(FailureDisposition.MANUAL))
                .subStep("boom", ctx -> {
                    throw new IllegalStateException("boom");
                })
                .and()
                .build();

        StateMachineManager<OrderContext> manager = StateMachine.manager(definition, repo);

        manager.initialize("res-1");
        var result = manager.trigger("res-1", "GO");

        assertThat(result.isFailed()).isTrue();
        assertThat(result.getFailureDisposition()).isEqualTo(FailureDisposition.MANUAL);
        assertThat(result.getFailedStateName()).isEqualTo("PROCESSING");
        assertThat(result.getRootCause())
                .as("the original exception survives to the caller")
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("boom");
    }
}
