package io.hypercell.fsm.manager;

import io.hypercell.fsm.core.ContextLoader;
import io.hypercell.fsm.core.StateMachineDefinition;
import io.hypercell.fsm.resume.ExecutionSnapshot;
import io.hypercell.fsm.resume.SnapshotRepository;

import java.util.Optional;

/**
 * Orchestrates the full request lifecycle for HTTP-driven state machine workflows.
 * <p>
 * This is the component your HTTP endpoint talks to directly. Each call to
 * {@link #trigger(String, String)} encapsulates the complete load → reconstruct → execute →
 * save cycle, so the endpoint never needs to know about snapshots, reconstitution, or
 * machine instances.
 * <p>
 * STRICT TRIGGER CONTRACT (1.0.0-RC2+):
 * {@code trigger()} applies <em>exactly one</em> transition. It will only succeed when the
 * execution is ready — snapshot absent (first event) or status {@code WAITING} (normal
 * next-event path). For any other status it throws immediately:
 * <ul>
 *   <li>{@code FAILED} → call {@link #proceed(String)} first, then trigger.</li>
 *   <li>{@code RUNNING} (interrupted crash) → call {@link #resume(String)} first, then trigger.</li>
 *   <li>{@code RETRY_SCHEDULED} → wait for recovery, then trigger.</li>
 *   <li>{@code TERMINATED} → throws {@link io.hypercell.fsm.exception.CompletedMachineException}.</li>
 * </ul>
 * Use {@link #eligibilityOf(String)} as an advisory pre-check before calling {@code trigger()}.
 * <p>
 * FULL LIFECYCLE PER REQUEST:
 * <p>
 * 1. Acquire per-executionId lock (throws ConcurrentExecutionException if locked)
 * 2. Load snapshot from repository
 * 3. Resolve context (from contextLoader or caller-supplied override)
 * 4. Branch on snapshot status:
 *    No snapshot  → newInstance(ctx, executionId) → trigger(event)
 *    WAITING      → reconstitute(ctx, snapshot)   → trigger(event)
 *    FAILED       → throw {@link io.hypercell.fsm.exception.IllegalTriggerStateException}
 *    RUNNING      → throw {@link io.hypercell.fsm.exception.IllegalTriggerStateException}
 *    RETRY_SCHED  → throw {@link io.hypercell.fsm.exception.IllegalTriggerStateException}
 *    TERMINATED   → throw {@link io.hypercell.fsm.exception.CompletedMachineException}
 * 5. Checkpoint is saved internally by trigger()
 * 6. Release lock
 * 7. Return ManagedTransitionResult
 * <p>
 * EXPLICIT RECOVER-THEN-TRIGGER PATTERN:
 * <pre>{@code
 * TriggerEligibility eligibility = manager.eligibilityOf(orderId);
 * if (eligibility == TriggerEligibility.NEEDS_PROCEED) {
 *     manager.proceed(orderId);       // retry failed sub-steps
 * } else if (eligibility == TriggerEligibility.NEEDS_RESUME) {
 *     manager.resume(orderId);        // complete the interrupted transition
 * }
 * // Now READY — safe to trigger
 * ManagedTransitionResult<OrderContext> result = manager.trigger(orderId, event);
 * }</pre>
 * <p>
 * CONCURRENCY:
 * Per-executionId in-process locking prevents concurrent access within the same JVM.
 * For distributed deployments, the SnapshotRepository implementation should add
 * optimistic locking (e.g. database compare-and-swap, Redis SET NX).
 * <p>
 * SPRING BOOT USAGE:
 * <pre>{@code
 * @Bean
 * public StateMachineManager<OrderContext> orderMachineManager(
 *         StateMachineDefinition<OrderContext> definition,
 *         SnapshotRepository repository) {
 *     return definition.newManager(repository);
 * }
 *
 * // In your controller:
 * ManagedTransitionResult<OrderContext> result =
 *     manager.trigger(orderId, request.getEvent());
 * }</pre>
 *
 * @param <C> the context type flowing through the machine
 */
public interface StateMachineManager<C> {

    /**
     * Apply a single event to the execution.
     * <p>
     * The execution must be in a triggerable state:
     * <ul>
     *   <li><strong>No snapshot</strong> — first event ever; a new instance is created.</li>
     *   <li><strong>WAITING</strong> — normal next-event path; the machine is reconstituted and
     *       the event is applied.</li>
     * </ul>
     * Any other status causes an immediate throw:
     * <ul>
     *   <li>{@code FAILED} / {@code RETRY_SCHEDULED} →
     *       {@link io.hypercell.fsm.exception.IllegalTriggerStateException} —
     *       call {@link #proceed(String)} first.</li>
     *   <li>{@code RUNNING} (interrupted) →
     *       {@link io.hypercell.fsm.exception.IllegalTriggerStateException} —
     *       call {@link #resume(String)} first.</li>
     *   <li>{@code TERMINATED} →
     *       {@link io.hypercell.fsm.exception.CompletedMachineException}.</li>
     * </ul>
     * Use {@link #eligibilityOf(String)} as an advisory pre-check.
     *
     * @param executionId your business entity ID (orderId, transactionId, etc.)
     * @param event       the event name from the incoming request
     * @throws io.hypercell.fsm.exception.IllegalTriggerStateException if the execution is
     *                                                                 FAILED, RUNNING, or RETRY_SCHEDULED
     * @throws io.hypercell.fsm.exception.CompletedMachineException    if the execution is TERMINATED
     */
    ManagedTransitionResult<C> trigger(String executionId, String event);

    /**
     * Apply a single event with a caller-supplied context that overrides the contextLoader
     * for this call only. Useful when the context is already available in the request
     * (e.g. the HTTP request body contains the order data).
     * <p>
     * Subject to the same eligibility constraints as {@link #trigger(String, String)}.
     *
     * @param contextOverride the ctx to use; null falls back to the contextLoader
     * @throws io.hypercell.fsm.exception.IllegalTriggerStateException if the execution is
     *                                                                 FAILED, RUNNING, or RETRY_SCHEDULED
     * @throws io.hypercell.fsm.exception.CompletedMachineException    if the execution is TERMINATED
     */
    ManagedTransitionResult<C> trigger(String executionId, String event, C contextOverride);

    /**
     * Manually retry failed sub-steps without triggering a new event.
     * <p>
     * Use this when you want to retry a failed execution before the next business
     * event arrives — for example, a scheduled job that retries all FAILED executions.
     * The caller controls when to retry; the library handles which sub-steps to skip.
     */
    ManagedTransitionResult<C> proceed(String executionId);

    /**
     * Manually retry with a context override.
     */
    ManagedTransitionResult<C> proceed(String executionId, C contextOverride);

    /**
     * Initialize a new execution in the initial state without applying any event.
     * <p>
     * This method creates a fresh instance (executing the initial state's sub-steps),
     * saves a checkpoint, and returns immediately. The execution stays in the initial
     * state awaiting the first event. This is useful for workflows where setup is
     * needed before the first business event arrives.
     * <p>
     * Uses the manager's configured contextLoader to load the initial context.
     * <p>
     * If the initial state's sub-steps fail, the execution is marked FAILED and
     * the failure details are returned (like {@code trigger()} behavior).
     * <p>
     * LIFECYCLE: {@code initialize(id)} → execution in initial state → later: {@code trigger(id, event)}
     *
     * @param executionId your business entity ID (orderId, transactionId, etc.)
     * @return result showing initial state with status RUNNING (or FAILED if sub-steps fail)
     */
    ManagedTransitionResult<C> initialize(String executionId);

    /**
     * Initialize with a caller-supplied context override.
     * Useful when the initial context is available in the request.
     *
     * @param contextOverride the ctx to use; null falls back to the contextLoader
     */
    ManagedTransitionResult<C> initialize(String executionId, C contextOverride);

    /**
     * Load the current snapshot without changing anything.
     * Returns empty if the execution has never started or has completed and been cleaned up.
     */
    Optional<ExecutionSnapshot> snapshotOf(String executionId);

    /**
     * Returns the current FSM state name for the given execution, or {@code empty} if the
     * execution does not exist yet (no snapshot).
     * <p>
     * This is a read-only, lock-free call — it loads the snapshot directly from the repository
     * and returns {@link io.hypercell.fsm.resume.ExecutionSnapshot#getCurrentStateName()}.
     * <p>
     * <strong>Advisory only.</strong> A concurrent {@code trigger()}, {@code proceed()}, or
     * {@code resume()} call may advance the state between this call and any subsequent action.
     *
     * @param executionId the business entity ID
     * @return the current state name, or {@code empty} if the execution has not started
     */
    Optional<String> currentState(String executionId);

    /**
     * Returns an advisory indication of whether {@link #trigger(String, String)} may be called
     * on the given execution without throwing
     * {@link io.hypercell.fsm.exception.IllegalTriggerStateException}.
     * <p>
     * Mapping:
     * <ul>
     *   <li>No snapshot → {@link TriggerEligibility#READY} (first event)</li>
     *   <li>{@code WAITING} → {@link TriggerEligibility#READY}</li>
     *   <li>{@code FAILED} → {@link TriggerEligibility#NEEDS_PROCEED}</li>
     *   <li>{@code RETRY_SCHEDULED} → {@link TriggerEligibility#NEEDS_PROCEED}</li>
     *   <li>{@code RUNNING} → {@link TriggerEligibility#NEEDS_RESUME}</li>
     *   <li>{@code TERMINATED} → {@link TriggerEligibility#TERMINATED}</li>
     * </ul>
     * <p>
     * This is a read-only, lock-free call. <strong>Advisory only</strong> — a concurrent
     * operation may change the status between this call and the subsequent {@code trigger()}.
     * {@code trigger()} always re-validates authoritatively under its per-execution lock and
     * throws {@link io.hypercell.fsm.exception.IllegalTriggerStateException} if the execution
     * is not eligible at that point.
     *
     * @param executionId the business entity ID
     * @return the eligibility of the execution for a {@code trigger()} call
     */
    TriggerEligibility eligibilityOf(String executionId);

    /**
     * Check if a state is the initial state of the machine definition.
     * Useful for validation and defensive programming.
     *
     * @param stateName the state name to check
     * @return true if the state is initial, false otherwise
     */
    boolean isInitialState(String stateName);

    /**
     * Check if a state is terminal in the machine definition.
     * Useful for determining if a workflow is complete.
     *
     * @param stateName the state name to check
     * @return true if the state is terminal, false otherwise
     */
    boolean isTerminal(String stateName);

    /**
     * Re-schedules retries for all FAILED/RETRY_SCHEDULED executions found
     * in the repository. Call this once on application startup.
     * <p>
     * For RETRY_SCHEDULED snapshots: re-schedules using the remaining delay
     * (or immediately if the scheduled time has already passed).
     * For FAILED snapshots awaiting manual retry: skips them — the developer
     * decided not to auto-retry these, that decision should be respected.
     */
    void recoverPendingRetries();

    /**
     * Resume a specific interrupted execution by its ID.
     * <p>
     * An execution is <em>interrupted</em> when its snapshot has status {@code RUNNING} —
     * the process crashed while sub-steps were executing. With the new status model,
     * {@code RUNNING} means exclusively "actively processing sub-steps"; at-rest executions
     * are now saved as {@code WAITING}.
     * <p>
     * Behaviour by snapshot status:
     * <ul>
     *   <li><strong>RUNNING</strong> — interrupted mid-transition; completes the remaining
     *       sub-steps (skipping those already checkpointed) and returns the new state.</li>
     *   <li><strong>WAITING</strong> — at-rest between transitions; no-op, returns the
     *       current state without executing anything.</li>
     *   <li><strong>FAILED</strong> — throws {@link IllegalStateException}; use
     *       {@link #proceed(String)} instead.</li>
     *   <li><strong>TERMINATED</strong> — throws {@link IllegalStateException}.</li>
     * </ul>
     *
     * @param executionId the business entity ID whose snapshot is to be resumed
     * @return the result after completing (or skipping) the interrupted transition
     * @throws IllegalStateException    if the snapshot is FAILED or TERMINATED
     * @throws IllegalArgumentException if no snapshot exists for the given ID
     */
    ManagedTransitionResult<C> resume(String executionId);

    /**
     * Resume all interrupted executions in parallel using the consumer-supplied executor.
     * <p>
     * With the new status model, {@code RUNNING} means exclusively "actively processing
     * sub-steps" (a crash leaves the snapshot here). This method queries
     * {@code listInterrupted(limit, afterId)} via keyset pagination, collects all
     * interrupted execution IDs, submits a {@link #resume(String)} task for each to the
     * configured recovery executor, and returns the submitted count immediately (async).
     * <p>
     * <strong>Requires a {@code recoveryExecutor} configured on the builder.</strong>
     * If none is configured, throws {@link IllegalStateException} — use
     * {@link #resume(String)} per execution instead, or configure one via
     * {@link io.hypercell.fsm.builder.StateMachineBuilder#recoveryExecutor(java.util.concurrent.ExecutorService)}.
     * <p>
     * Recovery is best-effort: each execution is attempted independently; one failure does
     * not stop others from running.
     * <p>
     * <strong>WARNING — Single-instance only.</strong> Do not call this in a multi-replica
     * (clustered) deployment. A starting node could resume an execution being processed by a
     * live peer. Multi-replica deployments should rely on lazy resume instead: every
     * {@link #trigger(String, String)} and {@link #resume(String)} call automatically detects
     * and completes interrupted executions.
     *
     * @return the number of resume tasks submitted to the executor (async — may still be
     * in-flight when this method returns)
     * @throws IllegalStateException if no {@code recoveryExecutor} is configured
     */
    int recoverInterruptedExecutions();

    /**
     * Retry all {@code FAILED} executions whose {@code attempt_number} is below
     * {@code maxAttempts}, in parallel via the consumer-supplied {@code recoveryExecutor}.
     *
     * <p>This is the consumer-driven, distributed alternative to the in-process
     * coordinator ({@link #recoverPendingRetries()}). It does <em>not</em> require a
     * {@link io.hypercell.fsm.retry.RetryCoordinator} — instead the consumer decides when
     * and how often to run sweeps (e.g. from a scheduled task or a leader-elected cron job).
     *
     * <p><strong>Scope: {@code FAILED} only.</strong> {@code RETRY_SCHEDULED} rows are
     * managed by the in-process coordinator and are not touched by this sweep. Do
     * <em>not</em> run this sweep and an auto-retry coordinator on the same definition
     * simultaneously — with a coordinator, {@code FAILED} means "policy exhausted", and
     * this sweep would override that decision.
     *
     * <p><strong>Bounded by {@code maxAttempts}.</strong> Only executions with
     * {@code attempt_number < maxAttempts} are fetched. This cap is applied in the query
     * predicate ({@link io.hypercell.fsm.resume.SnapshotRepository#listFailed}), so
     * exhausted rows are never loaded. {@code maxAttempts} must be {@code > 0}; passing
     * {@code 0} or a negative value throws {@link IllegalArgumentException}.
     *
     * <p><strong>Attempt-number semantics.</strong> Each call to this method that retries
     * an execution increments its {@code attempt_number} by exactly 1 before invoking
     * {@code proceed()}. Generic {@link #proceed(String)} and the coordinator path do
     * <em>not</em> increment the counter — {@code attempt_number} therefore reads as
     * "number of sweep retry attempts made by {@code recoverFailedExecutions}".
     *
     * <p><strong>Requires a {@code recoveryExecutor}.</strong> Throws
     * {@link IllegalStateException} if none is configured on the definition builder. Use
     * {@link #proceed(String)} per execution as the alternative.
     *
     * <p><strong>Async — returns submitted count.</strong> Tasks are submitted to the
     * executor immediately; the method returns the count of tasks submitted (executions may
     * still be in-flight on return).
     *
     * <p><strong>Best-effort, per-execution isolation.</strong> One failing task does not
     * stop others from running. Errors are logged per execution.
     *
     * <p><strong>Single-instance or leader-election for distributed deployments.</strong>
     * Do not call this from every replica — multiple concurrent sweeps will race to retry
     * the same executions. Use a leader-election mechanism or a single designated scheduler
     * node to ensure only one sweep runs at a time.
     *
     * @param maxAttempts upper bound on {@code attempt_number} (exclusive); must be {@code > 0}
     * @return the number of retry tasks submitted to the executor (async — may still be
     * in-flight when this method returns)
     * @throws IllegalStateException    if no {@code recoveryExecutor} is configured
     * @throws IllegalArgumentException if {@code maxAttempts <= 0}
     */
    int recoverFailedExecutions(int maxAttempts);

    /**
     * Create a new manager with a different context loader.
     * Useful when the definition's context loader doesn't fit your use case,
     * but you don't want to pass contextOverride on every trigger/proceed call.
     * <p>
     * The returned manager delegates to this one for all operations
     * except context loading.
     *
     * @param contextLoader the context loader to use instead of the definition's
     * @return a new manager bound to the provided context loader
     */
    StateMachineManager<C> withContextLoader(ContextLoader<C> contextLoader);

    /**
     * The only way to create a StateMachineManager.
     * Keeps DefaultStateMachineManager invisible to consumers.
     * Uses the context loader from the definition.
     */
    static <C> StateMachineManager<C> create(StateMachineDefinition<C> definition,
                                             SnapshotRepository repository) {
        return new DefaultStateMachineManager<>(definition, repository, definition.contextLoader());
    }
}
