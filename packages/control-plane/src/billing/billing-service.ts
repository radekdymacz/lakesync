// ---------------------------------------------------------------------------
// High-level billing service — orchestrates Stripe, usage, and org data
// ---------------------------------------------------------------------------

import { Err, Ok, type Result } from "@lakesync/core";
import type { Organisation, PlanId } from "../entities";
import { ControlPlaneError } from "../errors";
import { PLANS } from "../plans";
import type { OrgRepository, UsageRepository } from "../repositories";
import type {
	StripeBillingPortalSession,
	StripeCheckoutSession,
	StripeClient,
} from "./stripe-types";

/** Billing info for an organisation */
export interface BillingInfo {
	readonly orgId: string;
	readonly plan: PlanId;
	readonly planName: string;
	readonly price: number;
	readonly stripeCustomerId?: string;
	readonly stripeSubscriptionId?: string;
	readonly currentPeriodEnd?: number;
	readonly cancelAtPeriodEnd?: boolean;
	readonly usage: UsageSummary;
}

/** Summarised usage for the current billing period */
export interface UsageSummary {
	readonly deltasThisPeriod: number;
	readonly storageBytes: number;
	readonly apiCalls: number;
}

/** Dependencies for the billing service */
export interface BillingServiceDeps {
	readonly stripe: StripeClient;
	readonly orgRepo: OrgRepository;
	readonly usageRepo: UsageRepository;
	/** Base URL for redirect after Stripe checkout/portal (e.g. "https://dashboard.lakesync.dev") */
	readonly dashboardBaseUrl: string;
}

/**
 * Get billing information for an organisation.
 *
 * Returns the current plan, usage summary, and Stripe subscription details.
 */
export async function getBillingInfo(
	orgId: string,
	deps: BillingServiceDeps,
): Promise<Result<BillingInfo, ControlPlaneError>> {
	const orgResult = await deps.orgRepo.getById(orgId);
	if (!orgResult.ok) return orgResult;
	if (orgResult.value === null) {
		return Err(new ControlPlaneError(`Organisation "${orgId}" not found`, "NOT_FOUND"));
	}

	const org = orgResult.value;
	const plan = PLANS[org.plan];

	// Query usage for the current billing period (last 30 days)
	const now = new Date();
	const periodStart = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

	const usageResult = await deps.usageRepo.queryUsage({
		orgId,
		from: periodStart,
		to: now,
	});

	let usage: UsageSummary = { deltasThisPeriod: 0, storageBytes: 0, apiCalls: 0 };
	if (usageResult.ok) {
		let deltas = 0;
		let storage = 0;
		let api = 0;
		for (const row of usageResult.value) {
			if (row.eventType === "push_deltas" || row.eventType === "pull_deltas") {
				deltas += row.count;
			} else if (row.eventType === "storage_bytes") {
				// Use the latest storage snapshot
				storage = Math.max(storage, row.count);
			} else if (row.eventType === "api_call") {
				api += row.count;
			}
		}
		usage = { deltasThisPeriod: deltas, storageBytes: storage, apiCalls: api };
	}

	// Fetch subscription details from Stripe if available
	let currentPeriodEnd: number | undefined;
	let cancelAtPeriodEnd: boolean | undefined;
	if (org.stripeSubscriptionId) {
		try {
			const sub = await deps.stripe.subscriptions.retrieve(org.stripeSubscriptionId);
			currentPeriodEnd = sub.current_period_end;
			cancelAtPeriodEnd = sub.cancel_at_period_end;
		} catch {
			// Non-fatal — subscription might have been deleted externally
		}
	}

	return Ok({
		orgId,
		plan: org.plan,
		planName: plan.name,
		price: plan.price,
		stripeCustomerId: org.stripeCustomerId,
		stripeSubscriptionId: org.stripeSubscriptionId,
		currentPeriodEnd,
		cancelAtPeriodEnd,
		usage,
	});
}

/**
 * Create a Stripe Checkout session for plan upgrade.
 *
 * Returns a URL to redirect the user to Stripe's hosted checkout page.
 */
export async function createCheckoutSession(
	orgId: string,
	planId: PlanId,
	deps: BillingServiceDeps,
): Promise<Result<StripeCheckoutSession, ControlPlaneError>> {
	const plan = PLANS[planId];
	if (!plan.stripePriceId) {
		return Err(
			new ControlPlaneError(`Plan "${planId}" does not support Stripe checkout`, "INVALID_INPUT"),
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
				`Organisation "${orgId}" has no Stripe customer — create one first`,
				"INVALID_INPUT",
			),
		);
	}

	try {
		const session = await deps.stripe.checkout.sessions.create({
			customer: org.stripeCustomerId,
			mode: "subscription",
			line_items: [{ price: plan.stripePriceId, quantity: 1 }],
			success_url: `${deps.dashboardBaseUrl}/settings/billing?checkout=success`,
			cancel_url: `${deps.dashboardBaseUrl}/settings/billing?checkout=cancelled`,
			metadata: { orgId, planId },
		});

		return Ok(session);
	} catch (error) {
		return Err(
			new ControlPlaneError(
				`Failed to create checkout session for org "${orgId}"`,
				"INTERNAL",
				error instanceof Error ? error : undefined,
			),
		);
	}
}

/**
 * Create a Stripe Billing Portal session for self-service management.
 *
 * Returns a URL to redirect the user to Stripe's billing portal where
 * they can manage payment methods, view invoices, and cancel subscriptions.
 */
export async function createPortalSession(
	orgId: string,
	deps: BillingServiceDeps,
): Promise<Result<StripeBillingPortalSession, ControlPlaneError>> {
	const orgResult = await deps.orgRepo.getById(orgId);
	if (!orgResult.ok) return orgResult;
	if (orgResult.value === null) {
		return Err(new ControlPlaneError(`Organisation "${orgId}" not found`, "NOT_FOUND"));
	}

	const org = orgResult.value;
	if (!org.stripeCustomerId) {
		return Err(
			new ControlPlaneError(
				`Organisation "${orgId}" has no Stripe customer — create one first`,
				"INVALID_INPUT",
			),
		);
	}

	try {
		const session = await deps.stripe.billingPortal.sessions.create({
			customer: org.stripeCustomerId,
			return_url: `${deps.dashboardBaseUrl}/settings/billing`,
		});

		return Ok(session);
	} catch (error) {
		return Err(
			new ControlPlaneError(
				`Failed to create portal session for org "${orgId}"`,
				"INTERNAL",
				error instanceof Error ? error : undefined,
			),
		);
	}
}
