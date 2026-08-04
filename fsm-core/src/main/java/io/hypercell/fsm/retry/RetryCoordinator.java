package io.hypercell.fsm.retry;

import io.hypercell.fsm.core.ContextLoader;
import io.hypercell.fsm.core.StateMachineDefinition;
import io.hypercell.fsm.core.StateMachineInstance;
import io.hypercell.fsm.exception.ConcurrentRetryException;
import io.hypercell.fsm.exception.RetryException;
import io.hypercell.fsm.listener.InstanceOrigin;
import io.hypercell.fsm.resume.ExecutionSnapshot;
import io.hypercell.fsm.resume.SnapshotRepository;
import io.hypercell.fsm.resume.SnapshotStatus;
import io.hypercell.fsm.scope.ExecutionScope;
import io.hypercell.fsm.scope.ExecutionScopeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Orchestrates the full failure-and-retry lifecycle.
 * <p>
 * This class sits between the machine instance (which knows it failed) and the
 * retry infrastructure (policy + scheduler + repository). It ties them together.
 * <p>
 * RESPONSIBILITIES:
 * 1. On failure: save snapshot, check policy, schedule auto-retry if warranted
 * 2. On manual retry: cancel pending auto-retry, mark snapshot RUNNING, resume instance
 * 3. Guard against double-execution (ConcurrentRetryException)
 * <p>
 * LIFECYCLE:
 * machine fails
 * → onFailure() called
 * → snapshot saved with status=FAILED
 * → RetryPolicy.shouldRetry()? yes → snapshot updated to RETRY_SCHEDULED
 * → RetryScheduler.schedule()
 * no  → machine stays FAILED, awaiting manual retry
 * <p>
 * [time passes]
 * <p>
 * auto-retry fires OR developer calls manualRetry()
 * → snapshot status set to RUNNING
 * → definition.resume() creates new instance
 * → new instance.proceed() runs from failure point
 *
 * @param <C> the context type flowing through the machine
 */
public class RetryCoordinator<C> {
    private static final Logger log = LoggerFactory.getLogger(RetryCoordinator.class);

    private final RetryPolicy retryPolicy;
    private final RetryScheduler retryScheduler;
    private final SnapshotRepository repository;
    private final StateMachineDefinition<C> definition;

    /**
     * Loads a fresh context for retry attempts.
     * The original context object may be stale (it was created for the first run).
     * This loader is called with the executionId and should return a fresh ctx.
     * <p>
     * Example: executionId → orderService.loadOrderContext(executionId)
     */
    private final ContextLoader<C> contextLoader;

    public RetryCoordinator(RetryPolicy retryPolicy,
                            RetryScheduler retryScheduler,
                            SnapshotRepository repository,
                            StateMachineDefinition<C> definition,
                            ContextLoader<C> contextLoader) {
        this.retryPolicy = retryPolicy;
        this.retryScheduler = retryScheduler;
        this.repository = repository;
        this.definition = definition;
        this.contextLoader = contextLoader;
    }

    /**
     * Called by DefaultStateMachineInstance immediately after a failure is recorded.
     * <p>
     * NOTE: The snapshot has already been saved by the instance before this call.
     * Here we decide whether to schedule an auto-retry.
     */
    public void onFailure(StateMachineInstance<C> instance, String pendingEvent, Throwable error) {
        String executionId = instance.executionId();

        ExecutionSnapshot snapshot = repository.load(executionId).orElseGet(() -> {
            ExecutionSnapshot fresh = instance.takeSnapshot(pendingEvent);
            repository.save(executionId, fresh);
            return fresh;
        });

        int nextAttempt = snapshot.getAttemptNumber() + 1;

        log.info("[RetryCoordinator] Execution '{}' failed (attempt {}). Failed sub-step: '{}' in state '{}'",
                executionId, snapshot.getAttemptNumber(), snapshot.getFailedSubStepName(),
                snapshot.getFailedStateName());

        if (!snapshot.isAutoRecoverable()) {
            log.info("[RetryCoordinator] Execution '{}' is dispositioned {} — no auto-retry",
                    executionId, snapshot.getFailureDisposition());
            return;
        }

        if (!retryPolicy.shouldRetry(snapshot.getAttemptNumber(), error)) {
            log.info("[RetryCoordinator] RetryPolicy says stop — no auto-retry for '{}'", executionId);
            return;
        }

        java.time.Duration delay = retryPolicy.backoffFor(snapshot.getAttemptNumber());
        Instant retryAt = Instant.now().plus(delay);

        ExecutionSnapshot updated = snapshot
                .withAttemptNumber(nextAttempt)
                .withScheduledRetryAt(retryAt);
        repository.save(executionId, updated);

        log.info("[RetryCoordinator] Scheduling auto-retry for '{}' in {} ms (attempt {})", executionId,
                delay.toMillis(), nextAttempt);

        retryScheduler.schedule(executionId, delay, () -> executeRetry(executionId));
    }

    /**
     * Manually trigger a retry — called by the developer when they want to retry
     * without waiting for the auto-retry delay.
     * <p>
     * This cancels any pending auto-retry to prevent double-execution.
     *
     * @return the resumed machine instance, ready for further triggers if needed.
     * Call proceed() on it (the coordinator does this internally),
     * or just use the return value to check the new status.
     */
    public StateMachineInstance<C> manualRetry(String executionId) {
        ExecutionSnapshot snapshot = repository.load(executionId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "No snapshot found for executionId: " + executionId));

        if (snapshot.getStatus() == SnapshotStatus.RUNNING) {
            throw new ConcurrentRetryException(executionId);
        }

        retryScheduler.cancel(executionId);

        log.info("[RetryCoordinator] Manual retry triggered for '{}' (attempt {})", executionId,
                snapshot.getAttemptNumber());

        return executeRetry(executionId);
    }

    /**
     * The configured retry policy. Exposed for {@code StateMachineManager.recoverPendingRetries()},
     * which calls {@code shouldRetry(attemptNumber, null)} to decide whether each
     * pending snapshot should be re-scheduled on startup.
     */
    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    /**
     * The actual retry execution — shared by auto and manual paths.
     * Marks the snapshot RUNNING, creates the resumed instance, calls proceed().
     */
    private StateMachineInstance<C> executeRetry(String executionId) {
        ExecutionSnapshot snapshot = repository.load(executionId)
                .orElseThrow(() -> new IllegalStateException(
                        "Snapshot disappeared before retry could run for: " + executionId));

        repository.save(executionId, snapshot.withStatus(SnapshotStatus.RUNNING));

        try {
            C ctx = contextLoader.load(executionId);

            try (ExecutionScope scope = definition.executionScopeProvider()
                    .open(new ExecutionScopeInfo<>(executionId, definition.id(),
                            InstanceOrigin.RESUMED_FAILED, ctx))) {

                StateMachineInstance<C> resumed = definition.resume(ctx, snapshot, repository);

                resumed.proceed();

                log.info("[RetryCoordinator] Retry succeeded for '{}'", executionId);
                return resumed;
            }

        } catch (Exception e) {
            log.info("[RetryCoordinator] Retry failed for '{}': {}", executionId, e.getMessage());
            throw new RetryException(e);
        }
    }
}
