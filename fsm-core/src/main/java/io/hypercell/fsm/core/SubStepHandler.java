package io.hypercell.fsm.core;

import io.hypercell.fsm.failure.FailurePolicy;

/**
 * A named sub-step that can be implemented as a standalone class.
 * <p>
 * This is the class-based alternative to passing an inline lambda to
 * StateBuilder.subStep(name, action). The two are interchangeable:
 * <p>
 * // Lambda style (good for simple/short logic)
 * .subStep("charge-payment", ctx -> chargePayment(ctx))
 * <p>
 * // Class style (good for complex logic, Spring injection, unit testing)
 * .subStep(new ChargePaymentStep(paymentService))
 * <p>
 * Implementing this as a class means:
 * - It can have constructor-injected dependencies (services, repositories)
 * - It can be a Spring @Component and autowired
 * - It can be unit-tested in isolation without building a full machine
 * - The name() method makes the snapshot key explicit and visible
 *
 * @param <C> the context type flowing through the machine
 */
public interface SubStepHandler<C> {

    /**
     * The stable snapshot key for this sub-step.
     * Treat this like a database column name — renaming it breaks
     * existing snapshots that were saved with the old name.
     */
    String name();

    @SuppressWarnings("java:S112")
    ActionResult execute(C ctx) throws Exception;

    /**
     * Adapts this handler to the {@link Action} interface when needed.
     */
    default Action<C> asAction() {
        return this::execute;
    }

    /**
     * The <em>default</em> recovery behaviour for failures of this sub-step.
     * <p>
     * Override when the failure semantics are intrinsic to the step itself rather than to the
     * workflow hosting it — "this step reserves an external resource, so failing it commits
     * nothing" is a property of the step, and travels with the class wherever it is registered.
     * Returning {@code null} (the default) means "no opinion": the state policy is consulted next.
     * <pre>{@code
     * @Override
     * public FailurePolicy<SimSwapHelper> failurePolicy() {
     *     return FailurePolicy.always(FailureDisposition.REWIND);
     * }
     * }</pre>
     * Whatever policy ends up attached to the registration occupies the sub-step level of the
     * chain, which wins over the state's and the machine's policies. Because a handler is usually a
     * shared, injected singleton, the state doing the registering may know better: a policy passed
     * to {@code StateBuilder.subStep(handler, policy)} replaces the one declared here for that
     * registration only.
     */
    default FailurePolicy<C> failurePolicy() {
        return null;
    }
}
