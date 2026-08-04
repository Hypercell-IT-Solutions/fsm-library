package io.hypercell.fsm.listener;

import io.hypercell.fsm.OrderContext;
import io.hypercell.fsm.StateMachine;
import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.core.StateMachineDefinition;
import io.hypercell.fsm.failure.FailureDisposition;
import io.hypercell.fsm.failure.FailurePolicy;
import io.hypercell.fsm.manager.StateMachineManager;
import io.hypercell.fsm.resume.ExecutionSnapshot;
import io.hypercell.fsm.resume.InMemorySnapshotRepository;
import io.hypercell.fsm.resume.SnapshotStatus;
import io.hypercell.fsm.scope.ExecutionScope;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers {@link MachineEvent.InstanceCreatedEvent} — that it fires once per instance, before any
 * work, with the right {@link InstanceOrigin} for each of the paths that can build one — and the
 * context now carried on every event.
 */
class InstanceLifecycleTest {

    // ------------------------------------------------------------------ fixture

    /**
     * Records every event in order, so a test can assert both what fired and what fired first.
     */
    private static final class Recorder implements MachineEventListener<OrderContext> {
        private final List<MachineEvent<OrderContext>> events =
                Collections.synchronizedList(new ArrayList<>());

        @Override
        public void onEvent(MachineEvent<OrderContext> event) {
            events.add(event);
            MachineEventListener.super.onEvent(event);
        }

        List<MachineEvent.InstanceCreatedEvent<OrderContext>> created() {
            synchronized (events) {
                return events.stream()
                        .filter(MachineEvent.InstanceCreatedEvent.class::isInstance)
                        .map(e -> (MachineEvent.InstanceCreatedEvent<OrderContext>) e)
                        .toList();
            }
        }

        List<String> typeNames() {
            synchronized (events) {
                return events.stream().map(e -> e.getClass().getSimpleName()).toList();
            }
        }
    }

    private static StateMachineDefinition<OrderContext> machine(
            String id, InMemorySnapshotRepository repo, Recorder recorder,
            AtomicBoolean chargeFails, ExecutorService executor) {

        return StateMachine.<OrderContext>define(id)
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(OrderContext::new)
                .recoveryExecutor(executor)
                .listener(recorder)
                .state("PENDING")
                .on("GO").to("PROCESSING").end()
                .and()
                .state("PROCESSING")
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

    // ------------------------------------------------------------------ origins

    @Test
    void newInstance_firesCreatedEventWithOriginNew_beforeAnyOtherEvent() {
        InMemorySnapshotRepository repo = new InMemorySnapshotRepository();
        Recorder recorder = new Recorder();
        var def = machine("m-new", repo, recorder, new AtomicBoolean(false), null);

        OrderContext ctx = new OrderContext("order-1");
        def.newInstance(ctx, "order-1");

        assertThat(recorder.created()).hasSize(1);
        MachineEvent.InstanceCreatedEvent<OrderContext> e = recorder.created().get(0);
        assertThat(e.getOrigin()).isEqualTo(InstanceOrigin.NEW);
        assertThat(e.getExecutionId()).isEqualTo("order-1");
        assertThat(e.getCurrentStateName()).isEqualTo("PENDING");
        assertThat(e.getAttemptNumber()).isEqualTo(1);
        assertThat(e.getContext()).isSameAs(ctx);

        // The whole point of the event is that it precedes the work.
        assertThat(recorder.typeNames().get(0)).isEqualTo("InstanceCreatedEvent");
    }

    @Test
    void triggerOnWaitingExecution_firesCreatedEventWithOriginReconstituted() {
        InMemorySnapshotRepository repo = new InMemorySnapshotRepository();
        Recorder recorder = new Recorder();
        var def = machine("m-recon", repo, recorder, new AtomicBoolean(false), null);
        StateMachineManager<OrderContext> manager = def.newManager(repo);

        manager.initialize("order-2");
        recorder.events.clear();

        manager.trigger("order-2", "GO");

        assertThat(recorder.created()).hasSize(1);
        MachineEvent.InstanceCreatedEvent<OrderContext> e = recorder.created().get(0);
        assertThat(e.getOrigin()).isEqualTo(InstanceOrigin.RECONSTITUTED);
        assertThat(e.getCurrentStateName()).isEqualTo("PENDING");
        assertThat(recorder.typeNames().get(0)).isEqualTo("InstanceCreatedEvent");
    }

    @Test
    void proceedAfterFailure_firesCreatedEventWithOriginResumedFailed() {
        InMemorySnapshotRepository repo = new InMemorySnapshotRepository();
        Recorder recorder = new Recorder();
        AtomicBoolean chargeFails = new AtomicBoolean(true);
        var def = machine("m-failed", repo, recorder, chargeFails, null);
        StateMachineManager<OrderContext> manager = def.newManager(repo);

        manager.initialize("order-3");
        manager.trigger("order-3", "GO");
        assertThat(repo.load("order-3")).get()
                .extracting(ExecutionSnapshot::getStatus).isEqualTo(SnapshotStatus.FAILED);

        chargeFails.set(false);
        recorder.events.clear();
        manager.proceed("order-3");

        assertThat(recorder.created()).hasSize(1);
        MachineEvent.InstanceCreatedEvent<OrderContext> e = recorder.created().get(0);
        assertThat(e.getOrigin()).isEqualTo(InstanceOrigin.RESUMED_FAILED);
        assertThat(e.getCurrentStateName()).isEqualTo("PROCESSING");
    }

    @Test
    void resumeInterrupted_firesCreatedEventWithOriginResumedInterrupted() {
        InMemorySnapshotRepository repo = new InMemorySnapshotRepository();
        Recorder recorder = new Recorder();
        var def = machine("m-interrupted", repo, recorder, new AtomicBoolean(false), null);
        StateMachineManager<OrderContext> manager = def.newManager(repo);

        manager.initialize("order-4");
        // Simulate a crash mid-transition: a RUNNING row is what the sweep looks for.
        ExecutionSnapshot waiting = repo.load("order-4").orElseThrow();
        repo.save("order-4", waiting.withStatus(SnapshotStatus.RUNNING));

        recorder.events.clear();
        manager.resume("order-4");

        assertThat(recorder.created()).hasSize(1);
        assertThat(recorder.created().get(0).getOrigin())
                .isEqualTo(InstanceOrigin.RESUMED_INTERRUPTED);
    }

    // ------------------------------------------------------------------ context

    @Test
    void everyEventCarriesTheLiveContext() {
        InMemorySnapshotRepository repo = new InMemorySnapshotRepository();
        Recorder recorder = new Recorder();
        var def = machine("m-ctx", repo, recorder, new AtomicBoolean(false), null);

        OrderContext ctx = new OrderContext("order-5");
        var instance = def.newInstance(ctx, "order-5");
        instance.trigger("GO");

        assertThat(recorder.events).isNotEmpty();
        assertThat(recorder.events).allSatisfy(e -> assertThat(e.getContext()).isSameAs(ctx));
    }

    @Test
    void subStepFailedEventCarriesTheOriginalThrowable() {
        InMemorySnapshotRepository repo = new InMemorySnapshotRepository();
        Recorder recorder = new Recorder();
        var def = machine("m-throwable", repo, recorder, new AtomicBoolean(true), null);
        StateMachineManager<OrderContext> manager = def.newManager(repo);

        manager.initialize("order-6");
        manager.trigger("order-6", "GO");

        MachineEvent.SubStepFailedEvent<OrderContext> failed = recorder.events.stream()
                .filter(MachineEvent.SubStepFailedEvent.class::isInstance)
                .map(e -> (MachineEvent.SubStepFailedEvent<OrderContext>) e)
                .findFirst()
                .orElseThrow();

        assertThat(failed.getError()).isInstanceOf(IllegalStateException.class);
        assertThat(failed.getError()).hasMessage("payment gateway down");
        assertThat(failed.getErrorType()).isEqualTo("java.lang.IllegalStateException");
        assertThat(failed.getResult().isFailed()).isTrue();
    }

    @Test
    void machineFailedEventCarriesTheFailureContextThePolicySaw() {
        InMemorySnapshotRepository repo = new InMemorySnapshotRepository();
        Recorder recorder = new Recorder();
        var def = machine("m-failctx", repo, recorder, new AtomicBoolean(true), null);
        StateMachineManager<OrderContext> manager = def.newManager(repo);

        manager.initialize("order-7");
        manager.trigger("order-7", "GO");

        MachineEvent.MachineFailedEvent<OrderContext> failed = recorder.events.stream()
                .filter(MachineEvent.MachineFailedEvent.class::isInstance)
                .map(e -> (MachineEvent.MachineFailedEvent<OrderContext>) e)
                .findFirst()
                .orElseThrow();

        assertThat(failed.getFailureContext()).isNotNull();
        assertThat(failed.getFailureContext().stateName()).isEqualTo("PROCESSING");
        assertThat(failed.getFailureContext().subStepName()).isEqualTo("charge-payment");
        assertThat(failed.getFailureContext().isFirstSubStep()).isTrue();
        assertThat(failed.getFailureContext().sourceStateName()).isEqualTo("PENDING");
        assertThat(failed.getFailureContext().error())
                .isInstanceOf(IllegalStateException.class);
        // failureCount counts failed steps; attemptNumber is the retry counter. Different things.
        assertThat(failed.getFailureCount()).isEqualTo(1);
        assertThat(failed.getFailureContext().attemptNumber()).isEqualTo(1);
    }

    @Test
    void machineRewoundEventCarriesTheFailureContext() {
        InMemorySnapshotRepository repo = new InMemorySnapshotRepository();
        Recorder recorder = new Recorder();

        StateMachineDefinition<OrderContext> def = StateMachine.<OrderContext>define("m-rewind")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(OrderContext::new)
                .listener(recorder)
                .state("PENDING")
                .on("GO").to("PROCESSING").end()
                .and()
                .state("PROCESSING")
                .failurePolicy(FailurePolicy.onFirstSubStep(FailureDisposition.REWIND))
                .subStep("reserve-stock", ctx -> {
                    throw new IllegalStateException("stock service down");
                })
                .on("DONE").to("SHIPPED").end()
                .and()
                .state("SHIPPED").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = def.newManager(repo);
        manager.initialize("order-9");
        manager.trigger("order-9", "GO");

        MachineEvent.MachineRewoundEvent<OrderContext> rewound = recorder.events.stream()
                .filter(MachineEvent.MachineRewoundEvent.class::isInstance)
                .map(e -> (MachineEvent.MachineRewoundEvent<OrderContext>) e)
                .findFirst()
                .orElseThrow();

        assertThat(rewound.getRewoundToState()).isEqualTo("PENDING");
        assertThat(rewound.getFailureContext()).isNotNull();
        assertThat(rewound.getFailureContext().subStepName()).isEqualTo("reserve-stock");
        assertThat(rewound.getFailureContext().isFirstSubStep()).isTrue();
        assertThat(rewound.getFailureContext().error())
                .isInstanceOf(IllegalStateException.class);
    }

    // ------------------------------------------------------------------ scope

    @Test
    void executionScopeIsClosedEvenOnThePooledRecoveryThread() throws Exception {
        InMemorySnapshotRepository repo = new InMemorySnapshotRepository();
        Recorder recorder = new Recorder();
        ExecutorService executor = Executors.newFixedThreadPool(1);
        AtomicBoolean chargeFails = new AtomicBoolean(true);

        // A stand-in for MDC: set on open, cleared on close, asserted empty afterwards.
        ThreadLocal<String> ambient = new ThreadLocal<>();
        List<String> seenInsideScope = Collections.synchronizedList(new ArrayList<>());
        List<InstanceOrigin> origins = Collections.synchronizedList(new ArrayList<>());

        StateMachineDefinition<OrderContext> def = StateMachine.<OrderContext>define("m-scope")
                .initial("PENDING")
                .snapshotRepository(repo)
                .contextLoader(OrderContext::new)
                .recoveryExecutor(executor)
                .listener(recorder)
                .executionScopeProvider(info -> {
                    ambient.set(info.executionId());
                    origins.add(info.origin());
                    return (ExecutionScope) ambient::remove;
                })
                .state("PENDING")
                .on("GO").to("PROCESSING").end()
                .and()
                .state("PROCESSING")
                .subStep("charge-payment", ctx -> {
                    seenInsideScope.add(String.valueOf(ambient.get()));
                    if (chargeFails.get()) throw new IllegalStateException("payment gateway down");
                    return ActionResult.success();
                })
                .on("DONE").to("SHIPPED").end()
                .and()
                .state("SHIPPED").terminal().and()
                .build();

        StateMachineManager<OrderContext> manager = def.newManager(repo);
        manager.initialize("order-8");
        manager.trigger("order-8", "GO");

        // The sub-step ran with the scope established.
        assertThat(seenInsideScope).containsExactly("order-8");
        // …and the caller's thread was left clean.
        assertThat(ambient.get()).isNull();

        chargeFails.set(false);
        int swept = manager.recoverFailedExecutions(5);
        assertThat(swept).isEqualTo(1);

        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        assertThat(repo.load("order-8")).get()
                .extracting(ExecutionSnapshot::getStatus).isNotEqualTo(SnapshotStatus.FAILED);
        assertThat(seenInsideScope).containsExactly("order-8", "order-8");
        assertThat(origins).contains(InstanceOrigin.RESUMED_FAILED);
    }
}
