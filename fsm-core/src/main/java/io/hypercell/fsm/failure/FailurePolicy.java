package io.hypercell.fsm.failure;

/**
 * Decides how a sub-step failure should be handled.
 * <p>
 * Without a policy the library treats every failure identically: save a {@code FAILED} snapshot
 * and let {@code recoverFailedExecutions(int)} retry it from the failure point. That is right for
 * transient failures and wrong for everything else. A {@code FailurePolicy} lets a machine say
 * "this failure is different" with knowledge of which state, which sub-step, and which exception.
 * <p>
 * <strong>Where policies attach.</strong> A policy can be set at three levels, and the runtime
 * consults them most-specific first:
 * <pre>
 *   sub-step policy  →  state policy  →  machine policy  →  {@link FailureDisposition#RETRY}
 * </pre>
 * A policy that returns {@code null} has no opinion and the next level is consulted. This makes
 * partial policies natural — a state-level policy can single out one sub-step and let everything
 * else fall through to the machine default.
 * <p>
 * <strong>Example.</strong> A state whose first sub-step reserves an external resource: if that
 * reservation fails nothing was committed, so the whole transition should be abandoned and the
 * caller should re-fire the event. Later sub-steps have committed work, so they want the normal
 * resume-from-failure-point behaviour.
 * <pre>{@code
 * state.failurePolicy(FailurePolicy.onFirstSubStep(FailureDisposition.REWIND))
 *      .subStep("reserve-resource", ctx -> reserve(ctx))
 *      .subStep("apply-change",     ctx -> apply(ctx))
 * }</pre>
 * <p>
 * Policies are consulted on the execution thread while the failure is being recorded. Keep them
 * fast and side-effect free; a policy that throws is treated as "no opinion" and the failure is
 * logged.
 *
 * @param <C> the context type flowing through the machine
 * @see FailureDisposition
 * @see FailureContext
 */
@FunctionalInterface
public interface FailurePolicy<C> {

    /**
     * Classify a failure.
     *
     * @param failure everything known about the failure
     * @return the disposition to apply, or {@code null} to defer to the next level in the chain
     */
    FailureDisposition decide(FailureContext<C> failure);

    /**
     * Return a policy that consults this one first and falls back to {@code next} when this one
     * has no opinion.
     * <p>
     * Chains left-to-right, so the most specific rule comes first:
     * <pre>{@code
     * FailurePolicy.<Ctx>onSubStep("reserve", FailureDisposition.REWIND)
     *     .orElse(FailurePolicy.onErrorType(IllegalArgumentException.class, FailureDisposition.ABORT))
     *     .orElse(FailurePolicy.always(FailureDisposition.RETRY))
     * }</pre>
     *
     * @param next the fallback policy; must not be {@code null}
     */
    default FailurePolicy<C> orElse(FailurePolicy<C> next) {
        if (next == null) throw new IllegalArgumentException("next policy must not be null");
        return failure -> {
            FailureDisposition decided = decide(failure);
            return decided != null ? decided : next.decide(failure);
        };
    }

    /**
     * A policy that returns the same disposition for every failure.
     */
    static <C> FailurePolicy<C> always(FailureDisposition disposition) {
        requireDisposition(disposition);
        return failure -> disposition;
    }

    /**
     * A policy that applies {@code disposition} only when the named sub-step failed, and defers
     * otherwise.
     *
     * @param subStepName the sub-step name as declared on the state
     */
    static <C> FailurePolicy<C> onSubStep(String subStepName, FailureDisposition disposition) {
        if (subStepName == null) throw new IllegalArgumentException("subStepName must not be null");
        requireDisposition(disposition);
        return failure -> subStepName.equals(failure.subStepName()) ? disposition : null;
    }

    /**
     * A policy that applies {@code disposition} only when the state's <em>first</em> sub-step
     * failed, and defers otherwise.
     * <p>
     * This is the common shape for {@link FailureDisposition#REWIND}: a failure in the first
     * sub-step means the state was never really entered.
     */
    static <C> FailurePolicy<C> onFirstSubStep(FailureDisposition disposition) {
        requireDisposition(disposition);
        return failure -> failure.isFirstSubStep() ? disposition : null;
    }

    /**
     * A policy that applies {@code disposition} when the failure was caused by an exception of
     * the given type (or a subtype), and defers otherwise.
     * <p>
     * Defers when the sub-step reported failure without throwing — that is, it returned
     * {@link io.hypercell.fsm.core.ActionResult#failed(String)}, which carries no exception.
     *
     * @param errorType the exception type to match; subtypes match too
     */
    static <C> FailurePolicy<C> onErrorType(Class<? extends Throwable> errorType,
                                            FailureDisposition disposition) {
        if (errorType == null) throw new IllegalArgumentException("errorType must not be null");
        requireDisposition(disposition);
        return failure -> {
            Throwable error = failure.error();
            return error != null && errorType.isInstance(error) ? disposition : null;
        };
    }

    private static void requireDisposition(FailureDisposition disposition) {
        if (disposition == null) throw new IllegalArgumentException("disposition must not be null");
    }
}
