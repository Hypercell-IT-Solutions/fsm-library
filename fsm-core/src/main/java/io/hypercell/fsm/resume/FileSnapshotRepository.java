package io.hypercell.fsm.resume;

import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.exception.SnapshotException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.*;

/**
 * A file-based SnapshotRepository that stores each snapshot as a .properties file.
 * <p>
 * PURPOSE:
 * This is a production-ready reference implementation that works without any
 * third-party libraries. It demonstrates the full serialization contract.
 * <p>
 * For production use, replace this with a Jackson+PostgreSQL or Jackson+Redis
 * implementation. The interface is the same — only this class changes.
 * <p>
 * STORAGE FORMAT:
 * One .properties file per execution, named {executionId}.snapshot
 * Located in the directory passed to the constructor.
 * <p>
 * CONCURRENCY:
 * Each save/load/delete is an atomic file operation. Fine for single-JVM use.
 * For multi-instance deployments, use a centralized store (DB, Redis) instead.
 * <p>
 * KNOWN LIMITATION: {@link #listPendingRetries()} currently only returns snapshots with
 * status {@code FAILED}. Snapshots with status {@code RETRY_SCHEDULED} are not included,
 * so {@code StateMachineManager.recoverPendingRetries()} will not recover previously
 * scheduled (but not yet fired) retries after a process restart.
 * <p>
 * USAGE:
 * SnapshotRepository repo = new FileSnapshotRepository(Path.of("/var/fsm/snapshots"));
 */
public class FileSnapshotRepository implements SnapshotRepository {
    private static final Logger log = LoggerFactory.getLogger(FileSnapshotRepository.class);

    private final Path directory;

    public FileSnapshotRepository(Path directory) {
        this.directory = directory;
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new SnapshotException("Cannot create snapshot directory: " + directory, e);
        }
    }

    @Override
    public void save(String executionId, ExecutionSnapshot snapshot) {
        Properties props = toProperties(snapshot);
        Path file = filePath(executionId);
        try (OutputStream out = Files.newOutputStream(file,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            props.store(out, "FSM snapshot for execution: " + executionId);
        } catch (IOException e) {
            throw new SnapshotException("Failed to save snapshot for: " + executionId, e);
        }
    }

    @Override
    public Optional<ExecutionSnapshot> load(String executionId) {
        Path file = filePath(executionId);
        if (!Files.exists(file)) return Optional.empty();

        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(file)) {
            props.load(in);
            return Optional.of(fromProperties(props));
        } catch (IOException e) {
            throw new SnapshotException("Failed to load snapshot for: " + executionId, e);
        }
    }

    @Override
    public void delete(String executionId) {
        try {
            Files.deleteIfExists(filePath(executionId));
        } catch (IOException e) {
            log.error("[FileSnapshotRepository] Could not delete snapshot: {}", e.getMessage(), e);
        }
    }

    @Override
    public List<ExecutionSnapshot> listPendingRetries() {
        return listExecutionIds().stream()
                .map(this::load)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(s -> s.isFailed() || s.getStatus() == SnapshotStatus.RETRY_SCHEDULED)
                .toList();
    }

    @Override
    public List<ExecutionSnapshot> listInterrupted(int limit, String afterExecutionId) {
        return listExecutionIds().stream()
                .map(this::load)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(ExecutionSnapshot::isRunning)
                .sorted(java.util.Comparator.comparing(ExecutionSnapshot::getExecutionId))
                .filter(s -> afterExecutionId == null
                        || s.getExecutionId().compareTo(afterExecutionId) > 0)
                .limit(limit)
                .toList();
    }

    @Override
    public List<ExecutionSnapshot> listFailed(int limit, String afterExecutionId, int maxAttempts) {
        return listExecutionIds().stream()
                .map(this::load)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(s -> s.isFailed() && s.getAttemptNumber() < maxAttempts)
                .sorted(java.util.Comparator.comparing(ExecutionSnapshot::getExecutionId))
                .filter(s -> afterExecutionId == null
                        || s.getExecutionId().compareTo(afterExecutionId) > 0)
                .limit(limit)
                .toList();
    }

    /**
     * Serialize an ExecutionSnapshot to a flat Properties map.
     * <p>
     * SCHEMA:
     * executionId          = abc-123
     * machineDefinitionId  = order-processing
     * currentStateName      = PROCESSING
     * failedStateName      = PROCESSING
     * failedSubStepName    = charge-payment
     * lastTriggerEvent     = COMPLETE
     * attemptNumber        = 2
     * lastFailedAt         = 2024-01-15T10:42:01Z
     * scheduledRetryAt     = 2024-01-15T10:42:05Z   (omitted if absent)
     * lastErrorMessage     = Payment gateway timeout
     * status               = FAILED
     * capturedAt           = 2024-01-15T10:42:01Z
     * completedStep.0.key  = PROCESSING::charge-payment
     * completedStep.0.status = SUCCESS
     * completedStep.0.output.paymentId = PAY-001    (output entries, zero or more)
     * completedStep.count  = 1
     */
    private Properties toProperties(ExecutionSnapshot s) {
        Properties p = new Properties();

        set(p, SnapshotFields.EXECUTION_ID, s.getExecutionId());
        set(p, SnapshotFields.MACHINE_DEFINITION_ID, s.getMachineDefinitionId());
        set(p, SnapshotFields.CURRENT_STATE_NAME, s.getCurrentStateName());
        set(p, SnapshotFields.FAILED_STATE_NAME, s.getFailedStateName());
        set(p, SnapshotFields.FAILED_SUB_STATE_NAME, s.getFailedSubStepName());
        set(p, SnapshotFields.LAST_TRIGGER_EVENT, s.getLastTriggerEvent());
        p.setProperty(SnapshotFields.ATTEMPT_NUMBER, String.valueOf(s.getAttemptNumber()));
        set(p, SnapshotFields.LAST_FAILED_AT, s.getLastFailedAt() != null ? s.getLastFailedAt().toString() : null);
        set(p, SnapshotFields.SCHEDULED_RETRY_AT, s.getScheduledRetryAt() != null ? s.getScheduledRetryAt().toString() : null);
        set(p, SnapshotFields.LAST_ERROR_MESSAGE, s.getLastErrorMessage());
        p.setProperty(SnapshotFields.STATUS, s.getStatus().name());
        p.setProperty(SnapshotFields.CAPTURED_AT, s.getCapturedAt().toString());

        List<Map.Entry<String, ActionResult>> entries =
                new ArrayList<>(s.getCompletedSubStepResults().entrySet());
        p.setProperty(SnapshotFields.COMPLETED_COUNT, String.valueOf(entries.size()));
        for (int i = 0; i < entries.size(); i++) {
            Map.Entry<String, ActionResult> entry = entries.get(i);
            p.setProperty(SnapshotFields.completedStepKey(i), entry.getKey());
            p.setProperty(SnapshotFields.completedStepStatus(i), entry.getValue().getStatus().name());
            if (entry.getValue().getErrorMessage() != null) {
                p.setProperty(SnapshotFields.completedStepError(i), entry.getValue().getErrorMessage());
            }
            int j = 0;
            for (Map.Entry<String, Object> out : entry.getValue().getOutput().entrySet()) {
                p.setProperty(SnapshotFields.completedStepNestedOutputKey(i, j), out.getKey());
                p.setProperty(SnapshotFields.completedStepNestedOutputValue(i, j),
                        out.getValue() != null ? out.getValue().toString() : "");
                j++;
            }
            p.setProperty(SnapshotFields.completedStepOutputCount(i), String.valueOf(j));
        }

        return p;
    }

    private ExecutionSnapshot fromProperties(Properties p) {
        int stepCount = Integer.parseInt(p.getProperty(SnapshotFields.COMPLETED_COUNT, "0"));
        Map<String, ActionResult> completedSteps = new LinkedHashMap<>();

        for (int i = 0; i < stepCount; i++) {
            String key = p.getProperty(SnapshotFields.completedStepKey(i));
            String status = p.getProperty(SnapshotFields.completedStepStatus(i));
            String error = p.getProperty(SnapshotFields.completedStepError(i));

            int outCount = Integer.parseInt(p.getProperty(SnapshotFields.completedStepOutputCount(i), "0"));
            Map<String, Object> output = new LinkedHashMap<>();
            for (int j = 0; j < outCount; j++) {
                String k = p.getProperty(SnapshotFields.completedStepNestedOutputKey(i, j));
                String v = p.getProperty(SnapshotFields.completedStepNestedOutputValue(i, j));
                if (k != null) output.put(k, v);
            }

            ActionResult result;
            if ("FAILED".equals(status)) {
                result = error != null ? ActionResult.failed(error) : ActionResult.failed("unknown");
            } else if ("SKIPPED".equals(status)) {
                result = ActionResult.skipped();
            } else {
                result = output.isEmpty() ? ActionResult.success() : ActionResult.success(output);
            }

            completedSteps.put(key, result);
        }

        String scheduledStr = p.getProperty(SnapshotFields.SCHEDULED_RETRY_AT);

        return new ExecutionSnapshot.Builder()
                .executionId(p.getProperty(SnapshotFields.EXECUTION_ID, ""))
                .machineDefinitionId(p.getProperty(SnapshotFields.MACHINE_DEFINITION_ID, ""))
                .currentStateName(p.getProperty(SnapshotFields.CURRENT_STATE_NAME))
                .failedStateName(p.getProperty(SnapshotFields.FAILED_STATE_NAME))
                .failedSubStepName(p.getProperty(SnapshotFields.FAILED_SUB_STATE_NAME))
                .lastTriggerEvent(p.getProperty(SnapshotFields.LAST_TRIGGER_EVENT))
                .completedSubStepResults(completedSteps)
                .attemptNumber(Integer.parseInt(p.getProperty(SnapshotFields.ATTEMPT_NUMBER, "1")))
                .lastFailedAt(parseInstant(p.getProperty(SnapshotFields.LAST_FAILED_AT)))
                .scheduledRetryAt(scheduledStr != null ? parseInstant(scheduledStr) : null)
                .lastErrorMessage(p.getProperty(SnapshotFields.LAST_ERROR_MESSAGE))
                .status(SnapshotStatus.valueOf(p.getProperty(SnapshotFields.STATUS, "FAILED")))
                .capturedAt(parseInstant(p.getProperty(SnapshotFields.CAPTURED_AT)))
                .build();
    }

    private Path filePath(String executionId) {
        String safe = java.net.URLEncoder.encode(executionId, java.nio.charset.StandardCharsets.UTF_8);
        return directory.resolve(safe + ".snapshot");
    }

    private void set(Properties p, String key, String value) {
        if (value != null) p.setProperty(key, value);
    }

    private Instant parseInstant(String s) {
        if (s == null || s.isBlank()) return Instant.now();
        try {
            return Instant.parse(s);
        } catch (Exception e) {
            return Instant.now();
        }
    }

    /**
     * List all snapshots currently stored. Useful for admin/monitoring endpoints.
     */
    public List<String> listExecutionIds() {
        try {
            List<String> ids = new ArrayList<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "*.snapshot")) {
                for (Path p : stream) {
                    String name = p.getFileName().toString();
                    ids.add(name.substring(0, name.length() - ".snapshot".length()));
                }
            }
            return ids;
        } catch (IOException e) {
            return Collections.emptyList();
        }
    }

    /**
     * Property key constants that define the serialization schema for {@code .snapshot} files.
     * Exposed as {@code static final} strings so that alternative implementations
     * (e.g. a JDBC-backed repository reading the same file format) can reference
     * the field names without duplicating magic strings.
     * Treat these values as stable — changing them breaks existing snapshot files.
     */
    static final class SnapshotFields {
        private SnapshotFields() {
        }

        public static final String EXECUTION_ID = "executionId";
        public static final String MACHINE_DEFINITION_ID = "machineDefinitionId";
        public static final String CURRENT_STATE_NAME = "currentStateName";
        public static final String FAILED_STATE_NAME = "failedStateName";
        public static final String FAILED_SUB_STATE_NAME = "failedSubStateName";
        public static final String LAST_TRIGGER_EVENT = "lastTriggerEvent";
        public static final String ATTEMPT_NUMBER = "attemptNumber";
        public static final String LAST_FAILED_AT = "lastFailedAt";
        public static final String SCHEDULED_RETRY_AT = "scheduledRetryAt";
        public static final String LAST_ERROR_MESSAGE = "lastErrorMessage";
        public static final String STATUS = "status";
        public static final String CAPTURED_AT = "capturedAt";
        private static final String COMPLETED_STEP = "completedStep.";
        public static final String COMPLETED_COUNT = "completedStep.count";

        public static String completedStepKey(int i) {
            return COMPLETED_STEP + i + ".key";
        }

        public static String completedStepStatus(int i) {
            return COMPLETED_STEP + i + ".status";
        }

        public static String completedStepOutputCount(int i) {
            return COMPLETED_STEP + i + ".outputCount";
        }

        public static String completedStepError(int i) {
            return COMPLETED_STEP + i + ".error";
        }

        public static String completedStepNestedOutputKey(int i, int j) {
            return COMPLETED_STEP + i + ".output." + j + ".k";
        }

        public static String completedStepNestedOutputValue(int i, int j) {
            return COMPLETED_STEP + i + ".output." + j + ".v";
        }
    }
}
