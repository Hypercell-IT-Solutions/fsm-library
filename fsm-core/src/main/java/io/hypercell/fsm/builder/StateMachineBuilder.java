package io.hypercell.fsm.builder;

import io.hypercell.fsm.core.*;
import io.hypercell.fsm.exception.StateMachineConfigurationException;
import io.hypercell.fsm.execution.DefaultStateMachineDefinition;
import io.hypercell.fsm.listener.EventBus;
import io.hypercell.fsm.listener.MachineEventListener;
import io.hypercell.fsm.manager.StateMachineManager;
import io.hypercell.fsm.resume.DefaultResumePolicy;
import io.hypercell.fsm.resume.ExecutionSnapshot;
import io.hypercell.fsm.resume.ResumePolicy;
import io.hypercell.fsm.resume.SnapshotRepository;
import io.hypercell.fsm.retry.NoAutoRetryPolicy;
import io.hypercell.fsm.retry.RetryCoordinator;
import io.hypercell.fsm.retry.RetryPolicy;
import io.hypercell.fsm.retry.RetryScheduler;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Fluent builder for state machine definitions.
 * <p>
 * Usage:
 * <pre>{@code
 * StateMachine.<Ctx>define("machine-id")
 *     .initial("STATE_A")
 *     .listener(StateMachine.loggingListener("[TAG]"))
 *     .snapshotRepository(StateMachine.inMemoryRepository())
 *     .state("STATE_A")
 *         .subStep("step-1", ctx -> doWork(ctx))
 *         .on("EVENT").to("STATE_B").end()
 *         .and()
 *     .state("STATE_B").terminal().and()
 *     .build();
 * }</pre>
 *
 * @param <C> the context type flowing through the machine
 */
public class StateMachineBuilder<C> {

    private final String id;
    private String initialStateName;
    private final List<StateBuilder<C>> stateBuilders = new ArrayList<>();
    private final Set<String> registeredNames = new HashSet<>();
    private final List<MachineEventListener<C>> listeners = new ArrayList<>();

    private ResumePolicy<C> resumePolicy = DefaultResumePolicy.getInstance();
    private SnapshotRepository snapshotRepository = null;
    private RetryPolicy retryPolicy = NoAutoRetryPolicy.INSTANCE;
    private RetryScheduler retryScheduler = null;
    private ContextLoader<C> contextLoader = null;
    private ExecutorService recoveryExecutor = null;
    private int recoveryPageSize = DefaultStateMachineDefinition.DEFAULT_RECOVERY_PAGE_SIZE;

    public StateMachineBuilder(String id) {
        this.id = id;
    }

    /**
     * Set the name of the state the machine enters when a new instance is created.
     * Required — {@link #build()} throws if not called.
     */
    public StateMachineBuilder<C> initial(String s) {
        initialStateName = s;
        return this;
    }

    /**
     * Set the repository used to persist snapshots on failure and load them on resume.
     * Required when using any retry policy other than {@link NoAutoRetryPolicy}.
     * Optional for manual-only recovery (you may call {@code instance.proceed()} yourself).
     */
    public StateMachineBuilder<C> snapshotRepository(SnapshotRepository r) {
        snapshotRepository = r;
        return this;
    }

    /**
     * Set the retry policy applied after a sub-step failure.
     * Defaults to {@link NoAutoRetryPolicy} (snapshot saved but no auto-retry).
     * If set to any other policy, both {@link #snapshotRepository} and
     * {@link #contextLoader} must also be configured; {@link #build()} will throw otherwise.
     */
    public StateMachineBuilder<C> retryPolicy(RetryPolicy p) {
        retryPolicy = p;
        return this;
    }

    /**
     * Set the scheduler used to execute retries after the backoff delay.
     * Defaults to a built-in 2-thread daemon pool when not specified.
     * Use {@link io.hypercell.fsm.retry.ThreadPoolRetryScheduler} or a custom
     * implementation for higher throughput or distributed scheduling.
     */
    public StateMachineBuilder<C> retryScheduler(RetryScheduler s) {
        retryScheduler = s;
        return this;
    }

    /**
     * Set the loader that restores a fresh context for a given execution ID.
     * Called by the retry coordinator before each auto-retry attempt, and by
     * the manager on every request.
     * <p>
     * The loader must restore all intermediate results that completed sub-steps
     * wrote to durable storage, so that the resumed context is equivalent to the
     * context at the point of failure. See the
     * <a href="https://github.com/hypercell/fsm-library/blob/main/docs/05-persistence-and-retry.md#ctx-on-resume">Context on resume</a>
     * documentation.
     * <p>
     * Implementations may throw checked exceptions (e.g., database errors) without
     * wrapping them in {@code RuntimeException}.
     * <p>
     * Required when using any retry policy other than {@link NoAutoRetryPolicy}.
     */
    public StateMachineBuilder<C> contextLoader(ContextLoader<C> l) {
        contextLoader = l;
        return this;
    }

    /**
     * Override the default sub-step skip logic used during {@code proceed()}.
     * The default ({@link io.hypercell.fsm.resume.DefaultResumePolicy}) skips
     * sub-steps that have a success record in the execution record. Override only
     * if you need custom skip logic (e.g. always re-run certain steps).
     */
    public StateMachineBuilder<C> resumePolicy(ResumePolicy<C> p) {
        resumePolicy = p;
        return this;
    }

    /**
     * Set the executor used by
     * {@link io.hypercell.fsm.manager.StateMachineManager#recoverInterruptedExecutions()}
     * to resume interrupted executions in parallel.
     * <p>
     * If not configured, {@code recoverInterruptedExecutions()} throws
     * {@link IllegalStateException}. Use {@link java.util.concurrent.Executors} to create a
     * suitable pool (e.g. {@code Executors.newFixedThreadPool(4)}). The consumer owns the
     * executor's lifecycle — the library never shuts it down.
     * <p>
     * {@link io.hypercell.fsm.manager.StateMachineManager#resume(String)} and lazy resume
     * via {@code trigger()} do NOT require this executor and work with no configuration.
     */
    public StateMachineBuilder<C> recoveryExecutor(ExecutorService executor) {
        recoveryExecutor = executor;
        return this;
    }

    /**
     * Page size used by
     * {@link io.hypercell.fsm.manager.StateMachineManager#recoverInterruptedExecutions()} when
     * keyset-paginating interrupted executions from the repository. Defaults to {@code 100}.
     *
     * @param pageSize the page size; must be {@code > 0}
     * @throws IllegalArgumentException if {@code pageSize <= 0}
     */
    public StateMachineBuilder<C> recoveryPageSize(int pageSize) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("recoveryPageSize must be > 0, got " + pageSize);
        }
        recoveryPageSize = pageSize;
        return this;
    }

    /**
     * Register an event listener. Multiple calls add multiple listeners;
     * events are dispatched in registration order.
     */
    public StateMachineBuilder<C> listener(MachineEventListener<C> listener) {
        listeners.add(listener);
        return this;
    }

    /**
     * Register a state from a configurer class.
     * The configurer's stateName() becomes the builder key.
     * The configurer's configure() call adds sub-steps, hooks, and transitions.
     */
    public StateMachineBuilder<C> state(StateConfigurer<C> configurer) {
        StateBuilder<C> stateBuilder = new StateBuilder<>(this, configurer.stateName());
        configurer.configure(stateBuilder);
        registerState(stateBuilder);
        return this;
    }

    /**
     * Begin defining a state inline using the fluent DSL.
     * Chain sub-step, hook, and transition calls on the returned {@link StateBuilder},
     * then close with {@code .and()} to return here.
     *
     * @param name unique state name; must not contain {@code ::}
     * @throws StateMachineConfigurationException if the name contains {@code ::}
     */
    public StateBuilder<C> state(String name) {
        if (name.contains("::")) throw new StateMachineConfigurationException(
                "State name '" + name + "' contains reserved separator '::'.");
        return new StateBuilder<>(this, name);
    }

    void registerState(StateBuilder<C> sb) {
        if (!registeredNames.add(sb.getName())) throw new StateMachineConfigurationException(
                "Duplicate state name: '" + sb.getName() + "'");
        stateBuilders.add(sb);
    }

    @SuppressWarnings("unchecked")
    public StateMachineDefinition<C> build() {
        if (stateBuilders.isEmpty()) throw new StateMachineConfigurationException(
                "No states defined. Call .state(\"NAME\")...and().");
        if (initialStateName == null) throw new StateMachineConfigurationException(
                "No initial state. Call .initial(\"STATE_NAME\").");
        if (!registeredNames.contains(initialStateName)) throw new StateMachineConfigurationException(
                "Initial state '" + initialStateName + "' is not defined.");

        boolean hasAutoRetry = !(retryPolicy instanceof NoAutoRetryPolicy);
        if (hasAutoRetry) {
            if (snapshotRepository == null) throw new StateMachineConfigurationException(
                    "retryPolicy requires snapshotRepository.");
            if (contextLoader == null) throw new StateMachineConfigurationException(
                    "retryPolicy requires contextLoader.");
        }

        Map<String, StateDefinition<C>> stateMap = new LinkedHashMap<>();
        for (StateBuilder<C> sb : stateBuilders) {
            stateMap.put(sb.getName(), new SimpleStateDef<>(
                    sb.getName(), sb.isTerminal(), sb.getSubSteps(), sb.getCompositeHook()));
        }

        Map<String, List<TransitionDefinition<C>>> transitionMap = new LinkedHashMap<>();
        for (StateBuilder<C> sb : stateBuilders) {
            if (sb.isTerminal() && !sb.transitionData.isEmpty())
                throw new StateMachineConfigurationException(
                        "Terminal state '" + sb.getName() + "' has outgoing transitions.");
            List<TransitionDefinition<C>> ts = new ArrayList<>();
            for (Object[] td : sb.transitionData) {
                String target = (String) td[1];
                if (!stateMap.containsKey(target)) throw new StateMachineConfigurationException(
                        "Transition from '" + sb.getName() + "' targets unknown state '" + target + "'.");
                ts.add(new SimpleTransDef<>((String) td[0], sb.getName(), target,
                        (Guard<C>) td[2], (Action<C>) td[3]));
            }
            transitionMap.put(sb.getName(), ts);
        }

        EventBus<C> bus = listeners.isEmpty() ? EventBus.empty() : new EventBus<>(listeners);

        DefinitionRef<C> ref = new DefinitionRef<>();
        RetryCoordinator<C> coordinator = null;
        if (hasAutoRetry) {
            RetryScheduler sched = retryScheduler != null ? retryScheduler : new InlineScheduler();
            coordinator = new RetryCoordinator<>(retryPolicy, sched, snapshotRepository, ref, contextLoader);
        }

        DefaultStateMachineDefinition<C> def = new DefaultStateMachineDefinition<>(
                id, stateMap.get(initialStateName), stateMap, transitionMap,
                resumePolicy, snapshotRepository, coordinator, bus, contextLoader, recoveryExecutor,
                recoveryPageSize);
        ref.set(def);
        return def;
    }

    private static final class DefinitionRef<C> implements StateMachineDefinition<C> {
        private StateMachineDefinition<C> d;

        void set(StateMachineDefinition<C> def) {
            d = def;
        }

        @Override
        public String id() {
            return d.id();
        }

        @Override
        public StateDefinition<C> initialState() {
            return d.initialState();
        }

        @Override
        public SnapshotRepository repository() {
            return d.repository();
        }

        @Override
        public StateDefinition<C> stateByName(String n) {
            return d.stateByName(n);
        }

        @Override
        public List<TransitionDefinition<C>> transitionsFrom(String n) {
            return d.transitionsFrom(n);
        }

        @Override
        public boolean isInitialState(String stateName) {
            return d.isInitialState(stateName);
        }

        @Override
        public boolean isTerminal(String stateName) {
            return d.isTerminal(stateName);
        }

        @Override
        public ResumePolicy<C> resumePolicy() {
            return d.resumePolicy();
        }

        @Override
        public RetryCoordinator<C> retryCoordinator() {
            return d.retryCoordinator();
        }

        @Override
        public ContextLoader<C> contextLoader() {
            return d.contextLoader();
        }

        @Override
        public ExecutorService recoveryExecutor() {
            return d.recoveryExecutor();
        }

        @Override
        public int recoveryPageSize() {
            return d.recoveryPageSize();
        }

        @Override
        public StateMachineInstance<C> newInstance(C c) {
            return d.newInstance(c);
        }

        @Override
        public StateMachineInstance<C> newInstance(C ctx, String executionId) {
            return d.newInstance(ctx, executionId);
        }

        @Override
        public StateMachineManager<C> newManager() {
            return d.newManager();
        }

        @Override
        public StateMachineManager<C> newManager(SnapshotRepository repository) {
            return d.newManager(repository);
        }

        @Override
        public StateMachineInstance<C> reconstitute(C ctx, ExecutionSnapshot snapshot) {
            return d.reconstitute(ctx, snapshot);
        }

        @Override
        public StateMachineInstance<C> reconstitute(C ctx, ExecutionSnapshot snapshot, SnapshotRepository repository) {
            return d.reconstitute(ctx, snapshot, repository);
        }

        @Override
        public StateMachineInstance<C> resume(C ctx, ExecutionSnapshot snapshot) {
            return d.resume(ctx, snapshot);
        }

        @Override
        public StateMachineInstance<C> resume(C c, ExecutionSnapshot s, SnapshotRepository r) {
            return d.resume(c, s, r);
        }
    }

    static final class SimpleStateDef<C> implements StateDefinition<C> {
        private final String n;
        private final boolean t;
        private final List<SubStepDefinition<C>> s;
        private final StateHook<C> h;

        SimpleStateDef(String n, boolean t, List<SubStepDefinition<C>> s, StateHook<C> h) {
            this.n = n;
            this.t = t;
            this.s = Collections.unmodifiableList(s);
            this.h = h;
        }

        @Override
        public String name() {
            return n;
        }

        @Override
        public boolean isTerminal() {
            return t;
        }

        @Override
        public List<SubStepDefinition<C>> subSteps() {
            return s;
        }

        @Override
        public Optional<StateHook<C>> hook() {
            return Optional.ofNullable(h);
        }
    }

    static final class SimpleTransDef<C> implements TransitionDefinition<C> {
        private final String e;
        private final String src;
        private final String tgt;
        private final Guard<C> g;
        private final Action<C> a;

        SimpleTransDef(String e, String s, String t, Guard<C> g, Action<C> a) {
            this.e = e;
            src = s;
            tgt = t;
            this.g = g;
            this.a = a;
        }

        @Override
        public String event() {
            return e;
        }

        @Override
        public String sourceState() {
            return src;
        }

        @Override
        public String targetState() {
            return tgt;
        }

        @Override
        public Optional<Guard<C>> guard() {
            return Optional.ofNullable(g);
        }

        @Override
        public Optional<Action<C>> action() {
            return Optional.ofNullable(a);
        }
    }

    private static final class InlineScheduler implements RetryScheduler {
        private final ScheduledExecutorService ex = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "fsm-retry");
            t.setDaemon(true);
            return t;
        });
        private final Map<String, ScheduledFuture<?>> p = new ConcurrentHashMap<>();

        @Override
        public void schedule(String id, Duration d, Runnable a) {
            p.put(id, ex.schedule(() -> {
                p.remove(id);
                a.run();
            }, d.toMillis(), TimeUnit.MILLISECONDS));
        }

        @Override
        public void cancel(String id) {
            ScheduledFuture<?> f = p.remove(id);
            if (f != null) f.cancel(false);
        }

        @Override
        public void shutdown() {
            ex.shutdown();
        }
    }
}
