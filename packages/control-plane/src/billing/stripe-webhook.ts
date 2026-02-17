// ---------------------------------------------------------------------------
// Stripe webhook handler — processes billing lifecycle events
// ---------------------------------------------------------------------------

import { Err, Ok, type Result } from "@lakesync/core";
import type { PlanId } from "../entities";
import { ControlPlaneError } from "../errors";
import { PLANS } from "../plans";
import type { GatewayRepository, OrgRepository } from "../repositories";
import type { StripeClient, StripeWebhookEvent } from "./stripe-types";

/** Grace period before suspending an org after payment failure (ms) */
const PAYMENT_GRACE_PERIOD_MS = 3 * 24 * 60 * 60 * 1000; // 3 days

/** Dependencies for the Stripe webhook handler */
export interface StripeWebhookDeps {
	readonly stripe: StripeClient;
	readonly orgRepo: OrgRepository;
	readonly gatewayRepo: GatewayRepository;
	/**
	 * Optional callback when an org should be suspended after the grace period.
	 * The implementation should schedule suspension for `suspendAt`.
	 * When not provided, suspension happens immediately on webhook receipt
	 * (assumes the webhook arrives after the grace period).
	 */
	readonly scheduleSuspension?: (orgId: string, suspendAt: Date) => void;
}

/** Result of processing a webhook event */
export interface WebhookProcessResult {
	readonly eventType: string;
	readonly handled: boolean;
	readonly orgId?: string;
}

/** Supported Stripe webhook event types */
export type StripeWebhookEventType =
	| "customer.subscription.updated"
	| "customer.subscription.deleted"
	| "invoice.payment_succeeded"
	| "invoice.payment_failed";

/**
 * Verify a Stripe webhook signature and parse the event.
 *
 * Delegates to the injected Stripe client's `webhooks.constructEvent`.
 */
export function verifyWebhookSignature(
	payload: string,
	signature: string,
	webhookSecret: string,
	stripe: StripeClient,
): Result<StripeWebhookEvent, ControlPlaneError> {
	try {
		const event = stripe.webhooks.constructEvent(payload, signature, webhookSecret);
		return Ok(event);
	} catch (error) {
		return Err(
			new ControlPlaneError(
				"Invalid webhook signature",
				"INVALID_INPUT",
				error instanceof Error ? error : undefined,
			),
		);
	}
}

/**
 * Process a verified Stripe webhook event.
 *
 * Handles subscription updates, deletions, payment successes, and
 * payment failures. Unknown event types are acknowledged but not processed.
 */
export async function processWebhookEvent(
	event: StripeWebhookEvent,
	deps: StripeWebhookDeps,
): Promise<Result<WebhookProcessResult, ControlPlaneError>> {
	switch (event.type) {
		case "customer.subscription.updated":
			return handleSubscriptionUpdated(event, deps);
		case "customer.subscription.deleted":
			return handleSubscriptionDeleted(event, deps);
		case "invoice.payment_succeeded":
			return handlePaymentSucceeded(event, deps);
		case "invoice.payment_failed":
			return handlePaymentFailed(event, deps);
		default:
			return Ok({ eventType: event.type, handled: false });
	}
}

// ---------------------------------------------------------------------------
// Internal handlers
// ---------------------------------------------------------------------------

/** Resolve org by Stripe customer ID from subscription metadata or customer lookup */
async function resolveOrgFromMetadata(
	data: Record<string, unknown>,
	deps: StripeWebhookDeps,
): Promise<Result<{ orgId: string }, ControlPlaneError>> {
	const metadata = data.metadata as Record<string, string> | undefined;
	const orgId = metadata?.orgId;
	if (orgId) {
		return Ok({ orgId });
	}

	return Err(
		new ControlPlaneError(
			"Webhook event missing orgId in metadata — cannot process",
			"INVALID_INPUT",
		),
	);
}

/**
 * Look up the plan ID by Stripe price ID.
 * Returns undefined if no matching plan is found.
 */
function planIdByPriceId(priceId: string): PlanId | undefined {
	for (const [id, plan] of Object.entries(PLANS)) {
		if (plan.stripePriceId === priceId) {
			return id as PlanId;
		}
	}
	return undefined;
}

/** Handle `customer.subscription.updated` — sync plan from Stripe */
async function handleSubscriptionUpdated(
	event: StripeWebhookEvent,
	deps: StripeWebhookDeps,
): Promise<Result<WebhookProcessResult, ControlPlaneError>> {
	const data = event.data.object;
	const orgResult = await resolveOrgFromMetadata(data, deps);
	if (!orgResult.ok) return orgResult;

	const { orgId } = orgResult.value;

	// Extract the price ID from the subscription items
	const items = data.items as { data: Array<{ price: { id: string } }> } | undefined;
	const priceId = items?.data[0]?.price?.id;
	if (priceId) {
		const newPlanId = planIdByPriceId(priceId);
		if (newPlanId) {
			const updateResult = await deps.orgRepo.update(orgId, { plan: newPlanId });
			if (!updateResult.ok) return updateResult;
		}
	}

	return Ok({ eventType: event.type, handled: true, orgId });
}

/** Handle `customer.subscription.deleted` — downgrade to free, suspend if over limits */
async function handleSubscriptionDeleted(
	event: StripeWebhookEvent,
	deps: StripeWebhookDeps,
): Promise<Result<WebhookProcessResult, ControlPlaneError>> {
	const data = event.data.object;
	const orgResult = await resolveOrgFromMetadata(data, deps);
	if (!orgResult.ok) return orgResult;

	const { orgId } = orgResult.value;

	// Downgrade to free plan and clear subscription ID
	const updateResult = await deps.orgRepo.update(orgId, {
		plan: "free",
		stripeSubscriptionId: undefined,
	});
	if (!updateResult.ok) return updateResult;

	// Check if the org is over free-plan gateway limits and suspend if so
	const freePlan = PLANS.free;
	const gwResult = await deps.gatewayRepo.listByOrg(orgId);
	if (gwResult.ok) {
		const activeGateways = gwResult.value.filter((gw) => gw.status === "active");
		if (activeGateways.length > freePlan.maxGateways) {
			// Suspend excess gateways (keep the oldest ones active up to the limit)
			const sorted = [...activeGateways].sort(
				(a, b) => a.createdAt.getTime() - b.createdAt.getTime(),
			);
			for (let i = freePlan.maxGateways; i < sorted.length; i++) {
				const gw = sorted[i];
				if (gw) {
					await deps.gatewayRepo.update(gw.id, { status: "suspended" });
				}
			}
		}
	}

	return Ok({ eventType: event.type, handled: true, orgId });
}

/** Handle `invoice.payment_succeeded` — update org billing status */
async function handlePaymentSucceeded(
	event: StripeWebhookEvent,
	deps: StripeWebhookDeps,
): Promise<Result<WebhookProcessResult, ControlPlaneError>> {
	const data = event.data.object;
	const customerId = data.customer as string | undefined;
	if (!customerId) {
		return Ok({ eventType: event.type, handled: false });
	}

	// Reactivate any suspended gateways on successful payment
	const metadata = data.metadata as Record<string, string> | undefined;
	const orgId = metadata?.orgId;
	if (orgId) {
		const gwResult = await deps.gatewayRepo.listByOrg(orgId);
		if (gwResult.ok) {
			for (const gw of gwResult.value) {
				if (gw.status === "suspended") {
					await deps.gatewayRepo.update(gw.id, { status: "active" });
				}
			}
		}
	}

	return Ok({ eventType: event.type, handled: true, orgId });
}

/** Handle `invoice.payment_failed` — schedule org suspension after grace period */
async function handlePaymentFailed(
	event: StripeWebhookEvent,
	deps: StripeWebhookDeps,
): Promise<Result<WebhookProcessResult, ControlPlaneError>> {
	const data = event.data.object;
	const metadata = data.metadata as Record<string, string> | undefined;
	const orgId = metadata?.orgId;

	if (!orgId) {
		return Ok({ eventType: event.type, handled: false });
	}

	if (deps.scheduleSuspension) {
		// Schedule suspension after grace period
		const suspendAt = new Date(Date.now() + PAYMENT_GRACE_PERIOD_MS);
		deps.scheduleSuspension(orgId, suspendAt);
	} else {
		// No scheduler — suspend all active gateways immediately
		const gwResult = await deps.gatewayRepo.listByOrg(orgId);
		if (gwResult.ok) {
			for (const gw of gwResult.value) {
				if (gw.status === "active") {
					await deps.gatewayRepo.update(gw.id, { status: "suspended" });
				}
			}
		}
	}

	return Ok({ eventType: event.type, handled: true, orgId });
}
