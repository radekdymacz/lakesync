// ---------------------------------------------------------------------------
// Stripe customer and subscription lifecycle management
// ---------------------------------------------------------------------------

import { Err, Ok, type Result } from "@lakesync/core";
import type { Organisation, PlanId } from "../entities";
import { ControlPlaneError } from "../errors";
import { PLANS } from "../plans";
import type { OrgRepository } from "../repositories";
import type { StripeClient, StripeSubscription } from "./stripe-types";

/** Dependencies for Stripe billing operations */
export interface StripeBillingDeps {
	readonly stripe: StripeClient;
	readonly orgRepo: OrgRepository;
}

/**
 * Create a Stripe customer for an organisation.
 *
 * Stores the `stripeCustomerId` on the org record. If the org already
 * has a customer ID, returns it without creating a duplicate.
 */
export async function createCustomer(
	org: Organisation,
	deps: StripeBillingDeps,
): Promise<Result<string, ControlPlaneError>> {
	if (org.stripeCustomerId) {
		return Ok(org.stripeCustomerId);
	}

	try {
		const customer = await deps.stripe.customers.create({
			name: org.name,
			metadata: { orgId: org.id, slug: org.slug },
		});

		const updateResult = await deps.orgRepo.update(org.id, {
			stripeCustomerId: customer.id,
		});
		if (!updateResult.ok) return updateResult;

		return Ok(customer.id);
	} catch (error) {
		return Err(
			new ControlPlaneError(
				`Failed to create Stripe customer for org "${org.id}"`,
				"INTERNAL",
				error instanceof Error ? error : undefined,
			),
		);
	}
}

/**
 * Create a Stripe subscription for an organisation.
 *
 * The organisation must already have a stripeCustomerId. The plan must
 * have a `stripePriceId` (free plan has none — no subscription needed).
 */
export async function createSubscription(
	orgId: string,
	planId: PlanId,
	deps: StripeBillingDeps,
): Promise<Result<StripeSubscription, ControlPlaneError>> {
	const plan = PLANS[planId];
	if (!plan.stripePriceId) {
		return Err(
			new ControlPlaneError(
				`Plan "${planId}" does not have a Stripe price — no subscription needed`,
				"INVALID_INPUT",
			),
		);
	}

	const orgResult = await deps.orgRepo.getById(orgId);
	if (!orgResult.ok) return orgResult;
	if (orgResult.value === null) {
		return Err(new ControlPlaneError(`Organisation "${orgId}" not found`, "NOT_FOUND"));
	}

	const org = orgResult.value;
	if (!org.stripeCustomerId) {
		return Err(
			new ControlPlaneError(
				`Organisation "${orgId}" has no Stripe customer — call createCustomer first`,
				"INVALID_INPUT",
			),
		);
	}

	try {
		const subscription = await deps.stripe.subscriptions.create({
			customer: org.stripeCustomerId,
			items: [{ price: plan.stripePriceId }],
			metadata: { orgId, planId },
		});

		const updateResult = await deps.orgRepo.update(orgId, {
			plan: planId,
			stripeSubscriptionId: subscription.id,
		});
		if (!updateResult.ok) return updateResult;

		return Ok(subscription);
	} catch (error) {
		return Err(
			new ControlPlaneError(
				`Failed to create Stripe subscription for org "${orgId}"`,
				"INTERNAL",
				error instanceof Error ? error : undefined,
			),
		);
	}
}

/**
 * Update an existing subscription to a different plan (prorated).
 *
 * Looks up the current subscription, swaps the price item, and updates
 * the org record.
 */
export async function updateSubscription(
	orgId: string,
	newPlanId: PlanId,
	deps: StripeBillingDeps,
): Promise<Result<StripeSubscription, ControlPlaneError>> {
	const newPlan = PLANS[newPlanId];
	if (!newPlan.stripePriceId) {
		return Err(
			new ControlPlaneError(
				`Target plan "${newPlanId}" has no Stripe price. Use cancelSubscription to downgrade to free.`,
				"INVALID_INPUT",
			),
		);
	}

	const orgResult = await deps.orgRepo.getById(orgId);
	if (!orgResult.ok) return orgResult;
	if (orgResult.value === null) {
		return Err(new ControlPlaneError(`Organisation "${orgId}" not found`, "NOT_FOUND"));
	}

	const org = orgResult.value;
	if (!org.stripeSubscriptionId) {
		return Err(
			new ControlPlaneError(
				`Organisation "${orgId}" has no active subscription`,
				"INVALID_INPUT",
			),
		);
	}

	try {
		const currentSub = await deps.stripe.subscriptions.retrieve(org.stripeSubscriptionId);
		const itemId = currentSub.items.data[0]?.id;
		if (!itemId) {
			return Err(
				new ControlPlaneError(
					"Current subscription has no items — cannot update",
					"INTERNAL",
				),
			);
		}

		const updated = await deps.stripe.subscriptions.update(org.stripeSubscriptionId, {
			items: [{ id: itemId, price: newPlan.stripePriceId }],
			proration_behavior: "create_prorations",
			metadata: { orgId, planId: newPlanId },
		});

		const updateResult = await deps.orgRepo.update(orgId, { plan: newPlanId });
		if (!updateResult.ok) return updateResult;

		return Ok(updated);
	} catch (error) {
		return Err(
			new ControlPlaneError(
				`Failed to update subscription for org "${orgId}"`,
				"INTERNAL",
				error instanceof Error ? error : undefined,
			),
		);
	}
}

/**
 * Cancel a subscription at the end of the current billing period.
 *
 * The org remains on its current plan until the period ends, then the
 * webhook handler downgrades to free.
 */
export async function cancelSubscription(
	orgId: string,
	deps: StripeBillingDeps,
): Promise<Result<StripeSubscription, ControlPlaneError>> {
	const orgResult = await deps.orgRepo.getById(orgId);
	if (!orgResult.ok) return orgResult;
	if (orgResult.value === null) {
		return Err(new ControlPlaneError(`Organisation "${orgId}" not found`, "NOT_FOUND"));
	}

	const org = orgResult.value;
	if (!org.stripeSubscriptionId) {
		return Err(
			new ControlPlaneError(
				`Organisation "${orgId}" has no active subscription to cancel`,
				"INVALID_INPUT",
			),
		);
	}

	try {
		const updated = await deps.stripe.subscriptions.update(org.stripeSubscriptionId, {
			cancel_at_period_end: true,
		});

		return Ok(updated);
	} catch (error) {
		return Err(
			new ControlPlaneError(
				`Failed to cancel subscription for org "${orgId}"`,
				"INTERNAL",
				error instanceof Error ? error : undefined,
			),
		);
	}
}
