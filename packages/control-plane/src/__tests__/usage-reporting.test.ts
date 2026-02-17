import { Err, Ok } from "@lakesync/core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { UsageReportingDeps } from "../billing/usage-reporting";
import { reportOrgUsage, runDailyUsageReport } from "../billing/usage-reporting";
import type { StripeClient } from "../billing/stripe-types";
import type { Organisation } from "../entities";
import { ControlPlaneError } from "../errors";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function mockOrg(overrides: Partial<Organisation> = {}): Organisation {
	return {
		id: "org_abc",
		name: "Test Org",
		slug: "test-org",
		plan: "starter",
		stripeCustomerId: "cus_abc",
		stripeSubscriptionId: "sub_123",
		createdAt: new Date(),
		updatedAt: new Date(),
		...overrides,
	};
}

function createMockStripe(): StripeClient {
	return {
		customers: { create: vi.fn(), retrieve: vi.fn() },
		subscriptions: {
			create: vi.fn(),
			retrieve: vi.fn().mockResolvedValue({
				id: "sub_123",
				customer: "cus_abc",
				status: "active",
				items: { data: [{ id: "si_item1", price: { id: "price_starter" } }] },
				current_period_end: 0,
				cancel_at_period_end: false,
			}),
			update: vi.fn(),
			cancel: vi.fn(),
		},
		checkout: { sessions: { create: vi.fn() } },
		billingPortal: { sessions: { create: vi.fn() } },
		subscriptionItems: {
			createUsageRecord: vi.fn().mockResolvedValue({
				id: "ur_123",
				quantity: 100,
				timestamp: 0,
				subscription_item: "si_item1",
			}),
		},
		webhooks: { constructEvent: vi.fn() },
	};
}

function createMockDeps(overrides: Partial<UsageReportingDeps> = {}): UsageReportingDeps {
	return {
		stripe: createMockStripe(),
		orgRepo: {
			create: vi.fn(),
			getById: vi.fn().mockResolvedValue(Ok(mockOrg())),
			getBySlug: vi.fn(),
			update: vi.fn(),
			delete: vi.fn(),
		},
		usageRepo: {
			recordAggregates: vi.fn().mockResolvedValue(Ok(undefined)),
			queryUsage: vi.fn().mockResolvedValue(Ok([
				{ gatewayId: "gw_1", orgId: "org_abc", eventType: "push_deltas", count: 500, windowStart: new Date() },
				{ gatewayId: "gw_1", orgId: "org_abc", eventType: "push_deltas", count: 300, windowStart: new Date() },
			])),
			queryGatewayUsage: vi.fn().mockResolvedValue(Ok([])),
		},
		...overrides,
	};
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Usage Reporting", () => {
	let deps: UsageReportingDeps;

	beforeEach(() => {
		deps = createMockDeps();
	});

	describe("reportOrgUsage", () => {
		it("reports delta usage to Stripe for a subscribed org", async () => {
			const org = mockOrg();
			const from = new Date("2025-06-01");
			const to = new Date("2025-06-02");

			const result = await reportOrgUsage(org, from, to, deps);
			expect(result.ok).toBe(true);
			if (result.ok && result.value) {
				expect(result.value.deltasReported).toBe(800);
				expect(result.value.subscriptionItemId).toBe("si_item1");
			}

			expect(deps.stripe.subscriptionItems.createUsageRecord).toHaveBeenCalledWith(
				"si_item1",
				{
					quantity: 800,
					timestamp: Math.floor(to.getTime() / 1000),
					action: "set",
				},
			);
		});

		it("skips orgs without a subscription (free plan)", async () => {
			const org = mockOrg({ stripeSubscriptionId: undefined });
			const result = await reportOrgUsage(org, new Date(), new Date(), deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBeNull();
			}
			expect(deps.stripe.subscriptionItems.createUsageRecord).not.toHaveBeenCalled();
		});

		it("returns null when usage is zero", async () => {
			(deps.usageRepo.queryUsage as ReturnType<typeof vi.fn>).mockResolvedValue(Ok([]));

			const org = mockOrg();
			const result = await reportOrgUsage(org, new Date(), new Date(), deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBeNull();
			}
		});

		it("returns error when Stripe subscription retrieval fails", async () => {
			(deps.stripe.subscriptions.retrieve as ReturnType<typeof vi.fn>).mockRejectedValue(
				new Error("Not found"),
			);

			const org = mockOrg();
			const result = await reportOrgUsage(org, new Date(), new Date(), deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INTERNAL");
			}
		});

		it("returns error when usage query fails", async () => {
			(deps.usageRepo.queryUsage as ReturnType<typeof vi.fn>).mockResolvedValue(
				Err(new ControlPlaneError("DB error", "INTERNAL")),
			);

			const org = mockOrg();
			const result = await reportOrgUsage(org, new Date(), new Date(), deps);
			expect(result.ok).toBe(false);
		});

		it("returns error when Stripe usage record creation fails", async () => {
			(deps.stripe.subscriptionItems.createUsageRecord as ReturnType<typeof vi.fn>)
				.mockRejectedValue(new Error("Stripe error"));

			const org = mockOrg();
			const result = await reportOrgUsage(org, new Date(), new Date(), deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INTERNAL");
			}
		});

		it("returns error when subscription has no items", async () => {
			(deps.stripe.subscriptions.retrieve as ReturnType<typeof vi.fn>).mockResolvedValue({
				id: "sub_123",
				customer: "cus_abc",
				status: "active",
				items: { data: [] },
				current_period_end: 0,
				cancel_at_period_end: false,
			});

			const org = mockOrg();
			const result = await reportOrgUsage(org, new Date(), new Date(), deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.message).toContain("no items");
			}
		});
	});

	describe("runDailyUsageReport", () => {
		it("reports usage for all subscribed orgs", async () => {
			const orgs = [
				mockOrg({ id: "org_1" }),
				mockOrg({ id: "org_2" }),
			];

			const result = await runDailyUsageReport(orgs, deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.reported).toBe(2);
				expect(result.value.skipped).toBe(0);
				expect(result.value.failed).toBe(0);
			}
		});

		it("skips free-plan orgs", async () => {
			const orgs = [
				mockOrg({ id: "org_1" }),
				mockOrg({ id: "org_2", stripeSubscriptionId: undefined, plan: "free" }),
			];

			const result = await runDailyUsageReport(orgs, deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.reported).toBe(1);
				expect(result.value.skipped).toBe(1);
			}
		});

		it("counts failures and continues processing remaining orgs", async () => {
			// First org fails at Stripe, second succeeds
			let callCount = 0;
			(deps.stripe.subscriptions.retrieve as ReturnType<typeof vi.fn>).mockImplementation(() => {
				callCount++;
				if (callCount === 1) {
					return Promise.reject(new Error("Stripe error"));
				}
				return Promise.resolve({
					id: "sub_123",
					customer: "cus_abc",
					status: "active",
					items: { data: [{ id: "si_item1", price: { id: "price_starter" } }] },
					current_period_end: 0,
					cancel_at_period_end: false,
				});
			});

			const orgs = [
				mockOrg({ id: "org_1" }),
				mockOrg({ id: "org_2" }),
			];

			const result = await runDailyUsageReport(orgs, deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.failed).toBe(1);
				expect(result.value.reported).toBe(1);
				expect(result.value.errors).toHaveLength(1);
				const firstError = result.value.errors[0] as { orgId: string; error: string } | undefined;
				if (!firstError) throw new Error("Expected errors to contain an entry");
				expect(firstError.orgId).toBe("org_1");
			}
		});

		it("returns empty summary for no orgs", async () => {
			const result = await runDailyUsageReport([], deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.reported).toBe(0);
				expect(result.value.skipped).toBe(0);
				expect(result.value.failed).toBe(0);
			}
		});
	});
});
