// ---------------------------------------------------------------------------
// Usage reporting — daily aggregation from UsageRepository to Stripe
// ---------------------------------------------------------------------------

import { Err, Ok, type Result } from "@lakesync/core";
import type { Organisation } from "../entities";
import { ControlPlaneError } from "../errors";
import type { OrgRepository, UsageRepository } from "../repositories";
import type { StripeClient } from "./stripe-types";

/** Dependencies for usage reporting */
export interface UsageReportingDeps {
	readonly stripe: StripeClient;
	readonly orgRepo: OrgRepository;
	readonly usageRepo: UsageRepository;
}

/** Result of a single org's usage report */
export interface UsageReportResult {
	readonly orgId: string;
	readonly deltasReported: number;
	readonly subscriptionItemId: string;
}

/**
 * Report usage for a single organisation to Stripe.
 *
 * Queries the UsageRepository for delta counts in the given time window,
 * then reports the total to Stripe via the Usage Records API.
 */
export async function reportOrgUsage(
	org: Organisation,
	from: Date,
	to: Date,
	deps: UsageReportingDeps,
): Promise<Result<UsageReportResult | null, ControlPlaneError>> {
	// Skip orgs without a subscription (free plan)
	if (!org.stripeSubscriptionId) {
		return Ok(null);
	}

	// Fetch the subscription to get the subscription item ID
	let subscriptionItemId: string;
	try {
		const sub = await deps.stripe.subscriptions.retrieve(org.stripeSubscriptionId);
		const item = sub.items.data[0];
		if (!item) {
			return Err(
				new ControlPlaneError(
					`Subscription "${org.stripeSubscriptionId}" has no items`,
					"INTERNAL",
				),
			);
		}
		subscriptionItemId = item.id;
	} catch (error) {
		return Err(
			new ControlPlaneError(
				`Failed to retrieve subscription for org "${org.id}"`,
				"INTERNAL",
				error instanceof Error ? error : undefined,
			),
		);
	}

	// Query delta usage for the period
	const usageResult = await deps.usageRepo.queryUsage({
		orgId: org.id,
		from,
		to,
		eventType: "push_deltas",
	});
	if (!usageResult.ok) return usageResult;

	let totalDeltas = 0;
	for (const row of usageResult.value) {
		totalDeltas += row.count;
	}

	if (totalDeltas === 0) {
		return Ok(null);
	}

	// Report to Stripe
	try {
		await deps.stripe.subscriptionItems.createUsageRecord(subscriptionItemId, {
			quantity: totalDeltas,
			timestamp: Math.floor(to.getTime() / 1000),
			action: "set",
		});
	} catch (error) {
		return Err(
			new ControlPlaneError(
				`Failed to report usage to Stripe for org "${org.id}"`,
				"INTERNAL",
				error instanceof Error ? error : undefined,
			),
		);
	}

	return Ok({
		orgId: org.id,
		deltasReported: totalDeltas,
		subscriptionItemId,
	});
}

/**
 * Run the daily usage reporting job for all organisations.
 *
 * Iterates over all orgs with active subscriptions and reports their
 * usage for the previous day to Stripe. Returns a summary of successes
 * and failures.
 */
export async function runDailyUsageReport(
	orgs: ReadonlyArray<Organisation>,
	deps: UsageReportingDeps,
): Promise<Result<DailyReportSummary, ControlPlaneError>> {
	const now = new Date();
	const yesterday = new Date(now.getTime() - 24 * 60 * 60 * 1000);
	// Align to day boundaries (UTC)
	const from = new Date(Date.UTC(yesterday.getUTCFullYear(), yesterday.getUTCMonth(), yesterday.getUTCDate()));
	const to = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));

	let reported = 0;
	let skipped = 0;
	let failed = 0;
	const errors: Array<{ orgId: string; error: string }> = [];

	for (const org of orgs) {
		const result = await reportOrgUsage(org, from, to, deps);
		if (!result.ok) {
			failed++;
			errors.push({ orgId: org.id, error: result.error.message });
			continue;
		}
		if (result.value === null) {
			skipped++;
		} else {
			reported++;
		}
	}

	return Ok({ reported, skipped, failed, errors });
}

/** Summary of the daily usage report job */
export interface DailyReportSummary {
	/** Number of orgs whose usage was successfully reported */
	readonly reported: number;
	/** Number of orgs skipped (no subscription or zero usage) */
	readonly skipped: number;
	/** Number of orgs that failed to report */
	readonly failed: number;
	/** Error details for failed orgs */
	readonly errors: ReadonlyArray<{ orgId: string; error: string }>;
}
