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
     * How failures of this sub-step should be handled.
     * <p>
     * Override to give this specific step its own recovery behaviour — it is the most specific
     * level of the policy chain and wins over the state's and machine's policies. Returning
     * {@code null} (the default) means "no opinion": the state policy is consulted next.
     * <pre>{@code
     * @Override
     * public FailurePolicy<SimSwapHelper> failurePolicy() {
     *     return FailurePolicy.always(FailureDisposition.REWIND);
     * }
     * }</pre>
     */
    default FailurePolicy<C> failurePolicy() {
        return null;
    }
}
