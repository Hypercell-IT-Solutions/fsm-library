package io.hypercell.fsm.examples;

import io.hypercell.fsm.StateMachine;
import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.core.StateMachineDefinition;
import io.hypercell.fsm.exception.CompletedMachineException;
import io.hypercell.fsm.exception.ConcurrentExecutionException;
import io.hypercell.fsm.exception.InvalidEventException;
import io.hypercell.fsm.manager.ManagedTransitionResult;
import io.hypercell.fsm.manager.StateMachineManager;
import io.hypercell.fsm.resume.InMemorySnapshotRepository;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Demonstrates the {@link StateMachineManager} pattern for HTTP-driven workflows.
 *
 * <p>Run with: {@code mvn -pl fsm-examples compile exec:java -Dexec.mainClass=io.hypercell.fsm.examples.HttpManagerExample}
 *
 * <p>What this example shows:
 * <ul>
 *   <li>Manager as a long-lived singleton that handles the full load→execute→save cycle</li>
 *   <li>Each simulated "HTTP request" calls {@code manager.trigger(executionId, event)}</li>
 *   <li>{@code ConcurrentExecutionException} when two requests race for the same execution
 *       (map to HTTP 409)</li>
 *   <li>{@code InvalidEventException} for an event with no valid transition (HTTP 400)</li>
 *   <li>{@code CompletedMachineException} when triggering on a completed execution (HTTP 409)</li>
 *   <li>{@code recoverPendingRetries()} called once at startup</li>
 * </ul>
 *
 * <p>See also: <a href="../../../../../../../../../../docs/03-use-cases.md#b-http-request-driven-workflow">Use cases — HTTP manager</a>
 */
public class HttpManagerExample {

    // -------------------------------------------------------------------------
    // Domain ctx
    // -------------------------------------------------------------------------

    static class OrderContext {
        final String orderId;
        String reservationId;
        boolean paymentCharged;

        OrderContext(String orderId) {
            this.orderId = orderId;
        }
    }

    // -------------------------------------------------------------------------
    // Simulated "database" — ctx loaded by executionId
    // In a real app this would be orderRepository.findById(id)
    // -------------------------------------------------------------------------

    static final Map<String, OrderContext> DB = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Machine definition and manager (singleton in a real app, e.g. Spring @Bean)
    // -------------------------------------------------------------------------

    static final InMemorySnapshotRepository REPOSITORY = InMemorySnapshotRepository.create();

    static final StateMachineDefinition<OrderContext> DEFINITION =
            StateMachine.<OrderContext>define("order-workflow")
                    .initial("PENDING")
                    .listener(StateMachine.loggingListener("[ORDER]"))
                    .snapshotRepository(REPOSITORY)
                    .contextLoader(id -> DB.get(id))

                    .state("PENDING")
                        .on("APPROVE").to("PROCESSING").end()
                        .on("CANCEL").to("CANCELLED").end()
                        .and()

                    .state("PROCESSING")
                        .subStep("reserve-stock", ctx -> {
                            ctx.reservationId = "RSV-" + ctx.orderId;
                            return ActionResult.success();
                        })
                        .subStep("charge-payment", ctx -> {
                            ctx.paymentCharged = true;
                            return ActionResult.success();
                        })
                        .on("COMPLETE").to("SHIPPED").end()
                        .and()

                    .state("SHIPPED").terminal().and()
                    .state("CANCELLED").terminal().and()
                    .build();

    static final StateMachineManager<OrderContext> MANAGER =
            StateMachine.manager(DEFINITION, REPOSITORY);

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    static void simulateRequest(String label, Runnable request) {
        System.out.printf("%n=== %s ===%n", label);
        request.run();
    }

    static void logResult(ManagedTransitionResult<OrderContext> r) {
        System.out.printf("  %s → %s  [%s]%n",
                r.getFromState(), r.getToState(), r.getExecutionStatus());
    }

    // -------------------------------------------------------------------------
    // main
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws InterruptedException {
        // Seed the "database" — in a real app the ctx exists before the first event
        String orderId = "order-42";
        DB.put(orderId, new OrderContext(orderId));

        // --- Startup recovery (call once at application boot) ---
        // Re-schedules any retries that were in-flight when the process last stopped.
        // Safe to call even when nothing is pending.
        System.out.println("=== Startup: recoverPendingRetries() ===");
        MANAGER.recoverPendingRetries();
        System.out.println("  (nothing pending on first start)");

        // --- Request 1: first event — creates a new execution ---
        simulateRequest("Request 1: APPROVE (first event, creates new execution)", () -> {
            ManagedTransitionResult<OrderContext> r = MANAGER.trigger(orderId, "APPROVE");
            logResult(r);
        });

        // --- Request 2: invalid event from current state ---
        simulateRequest("Request 2: APPROVE again (no valid transition from PROCESSING)", () -> {
            try {
                MANAGER.trigger(orderId, "APPROVE");
            } catch (InvalidEventException e) {
                System.out.println("  HTTP 400 — " + e.getMessage());
            }
        });

        // --- Request 3: concurrent request (two threads, same executionId) ---
        simulateRequest("Request 3: two concurrent requests for the same execution", () -> {
            CountDownLatch firstHoldsLock = new CountDownLatch(1);
            CountDownLatch allowFirstToFinish = new CountDownLatch(1);

            // First thread: slow sub-step that holds the lock
            StateMachineDefinition<OrderContext> slowDef =
                    StateMachine.<OrderContext>define("slow-workflow")
                            .initial("START")
                            .snapshotRepository(InMemorySnapshotRepository.create())
                            .contextLoader(id -> DB.get(id))
                            .state("START")
                                .subStep("slow-step", ctx -> {
                                    firstHoldsLock.countDown();       // signal: lock is held
                                    try { allowFirstToFinish.await(); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                                    return ActionResult.success();
                                })
                                .on("GO").to("DONE").end()
                                .and()
                            .state("DONE").terminal().and()
                            .build();

            String concurrentId = "order-99";
            DB.put(concurrentId, new OrderContext(concurrentId));
            StateMachineManager<OrderContext> slowManager =
                    StateMachine.manager(slowDef, InMemorySnapshotRepository.create());

            Thread t1 = new Thread(() -> slowManager.trigger(concurrentId, "GO"));
            Thread t2 = new Thread(() -> {
                try {
                    firstHoldsLock.await();  // wait until t1 holds the lock
                    slowManager.trigger(concurrentId, "GO");
                } catch (ConcurrentExecutionException e) {
                    System.out.println("  HTTP 409 (thread 2) — " + e.getMessage());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            });

            t1.start();
            t2.start();
            try {
                firstHoldsLock.await();
                allowFirstToFinish.countDown();
                t1.join();
                t2.join();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        });

        // --- Request 4: complete the order ---
        simulateRequest("Request 4: COMPLETE", () -> {
            ManagedTransitionResult<OrderContext> r = MANAGER.trigger(orderId, "COMPLETE");
            logResult(r);
        });

        // --- Request 5: trigger on an already-completed execution ---
        simulateRequest("Request 5: trigger on completed execution", () -> {
            try {
                MANAGER.trigger(orderId, "COMPLETE");
            } catch (CompletedMachineException e) {
                System.out.println("  HTTP 409 — " + e.getMessage());
            }
        });
    }
}
