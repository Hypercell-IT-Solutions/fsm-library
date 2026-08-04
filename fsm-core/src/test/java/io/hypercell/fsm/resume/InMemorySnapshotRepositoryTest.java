package io.hypercell.fsm.resume;

import io.hypercell.fsm.failure.FailureDisposition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class InMemorySnapshotRepositoryTest {

    private SnapshotRepository repo;

    @BeforeEach
    void setUp() {
        repo = InMemorySnapshotRepository.create();
    }

    private ExecutionSnapshot failedSnapshot(String executionId) {
        return new ExecutionSnapshot.Builder()
                .executionId(executionId)
                .machineDefinitionId("order")
                .currentStateName("PROCESSING")
                .failedStateName("PROCESSING")
                .failedSubStepName("charge")
                .status(SnapshotStatus.FAILED)
                .capturedAt(Instant.now())
                .build();
    }

    @Test
    void saveAndLoad_roundTrip() {
        ExecutionSnapshot snap = failedSnapshot("exec-1");
        repo.save("exec-1", snap);

        assertThat(repo.load("exec-1")).isPresent()
                .hasValueSatisfying(s -> {
                    assertThat(s.getExecutionId()).isEqualTo("exec-1");
                    assertThat(s.isFailed()).isTrue();
                });
    }

    @Test
    void load_returnsEmpty_whenNotFound() {
        assertThat(repo.load("nonexistent")).isEmpty();
    }

    @Test
    void delete_removesSnapshot() {
        repo.save("exec-1", failedSnapshot("exec-1"));
        repo.delete("exec-1");
        assertThat(repo.load("exec-1")).isEmpty();
    }

    @Test
    void listPendingRetries_returnsFailedAndRetryScheduled_notRunningOrTerminated() {
        repo.save("failed-1", failedSnapshot("failed-1"));
        repo.save("scheduled-1", failedSnapshot("scheduled-1")
                .withScheduledRetryAt(Instant.now().plusSeconds(10)));
        repo.save("running-1", new ExecutionSnapshot.Builder()
                .executionId("running-1").status(SnapshotStatus.RUNNING).capturedAt(Instant.now()).build());
        repo.save("terminated-1", new ExecutionSnapshot.Builder()
                .executionId("terminated-1").status(SnapshotStatus.TERMINATED).capturedAt(Instant.now()).build());

        List<ExecutionSnapshot> pending = repo.listPendingRetries();

        assertThat(pending).hasSize(2);
        assertThat(pending).extracting(ExecutionSnapshot::getExecutionId)
                .containsExactlyInAnyOrder("failed-1", "scheduled-1");
    }

    @Test
    void save_overwritesExistingSnapshot() {
        repo.save("exec-1", failedSnapshot("exec-1"));
        ExecutionSnapshot updated = failedSnapshot("exec-1").withStatus(SnapshotStatus.RUNNING);
        repo.save("exec-1", updated);

        assertThat(repo.load("exec-1")).isPresent()
                .hasValueSatisfying(s -> assertThat(s.isRunning()).isTrue());
    }

    @Test
    void listFailed_returnsOnlyRetryDispositionedRows() {
        repo.save("retry-1", failedSnapshot("retry-1"));
        repo.save("manual-1", failedSnapshot("manual-1")
                .withFailureDisposition(FailureDisposition.MANUAL));
        repo.save("abort-1", failedSnapshot("abort-1")
                .withFailureDisposition(FailureDisposition.ABORT));

        List<ExecutionSnapshot> failed = repo.listFailed(10, null, 5);

        assertThat(failed).extracting(ExecutionSnapshot::getExecutionId)
                .as("only RETRY is eligible for the automatic sweep")
                .containsExactly("retry-1");
    }

    @Test
    void failureDisposition_defaultsToRetry_soUnclassifiedRowsStaySweepable() {
        repo.save("exec-1", failedSnapshot("exec-1"));

        ExecutionSnapshot loaded = repo.load("exec-1").orElseThrow();
        assertThat(loaded.getFailureDisposition()).isEqualTo(FailureDisposition.RETRY);
        assertThat(loaded.isAutoRecoverable()).isTrue();
    }
}
