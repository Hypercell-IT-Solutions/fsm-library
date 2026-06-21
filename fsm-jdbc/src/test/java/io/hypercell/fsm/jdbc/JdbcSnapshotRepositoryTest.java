package io.hypercell.fsm.jdbc;

import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.resume.ExecutionSnapshot;
import io.hypercell.fsm.resume.SnapshotRepository;
import io.hypercell.fsm.resume.SnapshotStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcSnapshotRepositoryTest {

    private DataSource dataSource;
    private SnapshotRepository repo;

    @BeforeEach
    void setUp() {
        dataSource = H2DataSourceHelper.newInMemoryDataSource();
        repo = new JdbcSnapshotRepository(dataSource, new TestH2Dialect());
    }

    private ExecutionSnapshot failedSnapshot(String executionId) {
        return new ExecutionSnapshot.Builder()
                .executionId(executionId)
                .machineDefinitionId("order")
                .currentStateName("PROCESSING")
                .failedStateName("PROCESSING")
                .failedSubStepName("charge")
                .lastTriggerEvent("APPROVE")
                .attemptNumber(1)
                .lastFailedAt(Instant.now())
                .lastErrorMessage("connection timeout")
                .status(SnapshotStatus.FAILED)
                .capturedAt(Instant.now())
                .build();
    }

    @Test
    void schemaCreated_onConstruction() throws Exception {
        try (Connection conn = dataSource.getConnection();
             ResultSet rs = conn.getMetaData().getTables(null, null, "%", null)) {
            boolean found = false;
            while (rs.next()) {
                if (rs.getString("TABLE_NAME").equalsIgnoreCase("fsm_snapshots")) {
                    found = true;
                    break;
                }
            }
            assertThat(found).as("Table fsm_snapshots should exist after construction").isTrue();
        }
    }

    @Test
    void save_and_load_roundTrip() {
        ExecutionSnapshot snap = failedSnapshot("exec-1");
        repo.save("exec-1", snap);

        assertThat(repo.load("exec-1")).isPresent().hasValueSatisfying(loaded -> {
            assertThat(loaded.getExecutionId()).isEqualTo("exec-1");
            assertThat(loaded.getMachineDefinitionId()).isEqualTo("order");
            assertThat(loaded.getCurrentStateName()).isEqualTo("PROCESSING");
            assertThat(loaded.getFailedStateName()).isEqualTo("PROCESSING");
            assertThat(loaded.getFailedSubStepName()).isEqualTo("charge");
            assertThat(loaded.getLastTriggerEvent()).isEqualTo("APPROVE");
            assertThat(loaded.getAttemptNumber()).isEqualTo(1);
            assertThat(loaded.getLastErrorMessage()).isEqualTo("connection timeout");
            assertThat(loaded.getStatus()).isEqualTo(SnapshotStatus.FAILED);
            assertThat(loaded.isFailed()).isTrue();
            assertThat(loaded.getLastFailedAt()).isNotNull();
            assertThat(loaded.getCapturedAt()).isNotNull();
        });
    }

    @Test
    void load_returnsEmpty_whenNotFound() {
        assertThat(repo.load("nonexistent")).isEmpty();
    }

    @Test
    void delete_removesSnapshot() {
        repo.save("exec-del", failedSnapshot("exec-del"));
        repo.delete("exec-del");
        assertThat(repo.load("exec-del")).isEmpty();
    }

    @Test
    void save_upserts_overwritesExistingStatus() {
        repo.save("exec-ver", failedSnapshot("exec-ver"));
        repo.save("exec-ver", failedSnapshot("exec-ver").withStatus(SnapshotStatus.RUNNING));

        assertThat(repo.load("exec-ver")).isPresent()
                .hasValueSatisfying(s -> assertThat(s.isRunning()).isTrue());
    }

    @Test
    void listPendingRetries_returnsOnlyFailedAndRetryScheduled() {
        repo.save("failed-1", failedSnapshot("failed-1"));
        repo.save("scheduled-1", failedSnapshot("scheduled-1")
                .withScheduledRetryAt(Instant.now().plusSeconds(30)));
        repo.save("running-1", new ExecutionSnapshot.Builder()
                .executionId("running-1").machineDefinitionId("order")
                .currentStateName("PROCESSING")
                .status(SnapshotStatus.RUNNING).capturedAt(Instant.now()).build());
        repo.save("terminated-1", new ExecutionSnapshot.Builder()
                .executionId("terminated-1").machineDefinitionId("order")
                .currentStateName("SHIPPED")
                .status(SnapshotStatus.TERMINATED).capturedAt(Instant.now()).build());

        List<ExecutionSnapshot> pending = repo.listPendingRetries();

        assertThat(pending).hasSize(2);
        assertThat(pending).extracting(ExecutionSnapshot::getExecutionId)
                .containsExactlyInAnyOrder("failed-1", "scheduled-1");
    }

    @Test
    void completedSubStepResults_serializedAsJson_andRoundTripped() {
        ExecutionSnapshot snap = new ExecutionSnapshot.Builder()
                .executionId("exec-steps")
                .machineDefinitionId("order")
                .currentStateName("PROCESSING")
                .failedStateName("PROCESSING")
                .failedSubStepName("charge")
                .status(SnapshotStatus.FAILED)
                .capturedAt(Instant.now())
                .completedSubStepResults(Map.of(
                        "PROCESSING::reserve", ActionResult.success(),
                        "PROCESSING::validate", ActionResult.success(Map.of("valid", true))
                ))
                .build();

        repo.save("exec-steps", snap);

        assertThat(repo.load("exec-steps")).isPresent().hasValueSatisfying(loaded -> {
            assertThat(loaded.getCompletedSubStepResults())
                    .containsKey("PROCESSING::reserve")
                    .containsKey("PROCESSING::validate");
            assertThat(loaded.getCompletedSubStepResults().get("PROCESSING::reserve").isSuccess()).isTrue();
        });
    }

    @Test
    void nullTimestamps_handledGracefully() {
        ExecutionSnapshot snap = new ExecutionSnapshot.Builder()
                .executionId("exec-null")
                .machineDefinitionId("order")
                .currentStateName("RUNNING")
                .status(SnapshotStatus.RUNNING)
                .capturedAt(Instant.now())
                .scheduledRetryAt(null)
                .build();

        repo.save("exec-null", snap);

        assertThat(repo.load("exec-null")).isPresent().hasValueSatisfying(loaded -> {
            assertThat(loaded.getScheduledRetryAt()).isNull();
            assertThat(loaded.getCapturedAt()).isNotNull();
        });
    }

    @Test
    void listInterrupted_returnsOnlyRunningRows_notWaitingOrTerminated() {
        // Insert snapshots with different statuses
        repo.save("running-1", new ExecutionSnapshot.Builder()
                .executionId("running-1").machineDefinitionId("order")
                .currentStateName("PROCESSING")
                .status(SnapshotStatus.RUNNING).capturedAt(Instant.now()).build());
        repo.save("running-2", new ExecutionSnapshot.Builder()
                .executionId("running-2").machineDefinitionId("order")
                .currentStateName("PROCESSING")
                .status(SnapshotStatus.RUNNING).capturedAt(Instant.now()).build());
        repo.save("failed-1", failedSnapshot("failed-1"));
        repo.save("terminated-1", new ExecutionSnapshot.Builder()
                .executionId("terminated-1").machineDefinitionId("order")
                .currentStateName("SHIPPED")
                .status(SnapshotStatus.TERMINATED).capturedAt(Instant.now()).build());
        repo.save("waiting-1", new ExecutionSnapshot.Builder()
                .executionId("waiting-1").machineDefinitionId("order")
                .currentStateName("PROCESSING")
                .status(SnapshotStatus.WAITING).capturedAt(Instant.now()).build());
        repo.save("retry-1", failedSnapshot("retry-1")
                .withScheduledRetryAt(Instant.now().plusSeconds(60)));

        List<ExecutionSnapshot> interrupted = repo.listInterrupted(10, null);

        assertThat(interrupted).hasSize(2);
        assertThat(interrupted).extracting(ExecutionSnapshot::getExecutionId)
                .containsExactlyInAnyOrder("running-1", "running-2");
        assertThat(interrupted).allMatch(ExecutionSnapshot::isRunning);
    }

    @Test
    void listInterrupted_emptyWhenNoRunningRows() {
        repo.save("failed-1", failedSnapshot("failed-1"));
        repo.save("waiting-1", new ExecutionSnapshot.Builder()
                .executionId("waiting-1").machineDefinitionId("order")
                .currentStateName("PROCESSING")
                .status(SnapshotStatus.WAITING).capturedAt(Instant.now()).build());

        List<ExecutionSnapshot> interrupted = repo.listInterrupted(10, null);

        assertThat(interrupted).isEmpty();
    }

    @Test
    void listInterrupted_keysetPagination_walksAllRows() {
        for (int i = 1; i <= 5; i++) {
            repo.save("exec-" + i, new ExecutionSnapshot.Builder()
                    .executionId("exec-" + i).machineDefinitionId("order")
                    .currentStateName("PROCESSING")
                    .status(SnapshotStatus.RUNNING).capturedAt(Instant.now()).build());
        }
        repo.save("waiting-1", new ExecutionSnapshot.Builder()
                .executionId("waiting-1").machineDefinitionId("order")
                .currentStateName("PROCESSING")
                .status(SnapshotStatus.WAITING).capturedAt(Instant.now()).build());

        // Page through with page size 2
        List<String> seen = new java.util.ArrayList<>();
        String lastId = null;
        while (true) {
            List<ExecutionSnapshot> page = repo.listInterrupted(2, lastId);
            if (page.isEmpty()) break;
            page.forEach(s -> seen.add(s.getExecutionId()));
            if (page.size() < 2) break;
            lastId = page.get(page.size() - 1).getExecutionId();
        }
        assertThat(seen).hasSize(5);
        assertThat(seen).doesNotContain("waiting-1");
    }

}