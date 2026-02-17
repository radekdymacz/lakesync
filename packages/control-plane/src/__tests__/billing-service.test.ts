import { Err, Ok } from "@lakesync/core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { BillingServiceDeps } from "../billing/billing-service";
import {
	createCheckoutSession,
	createPortalSession,
	getBillingInfo,
} from "../billing/billing-service";
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
		customers: {
			create: vi.fn(),
			retrieve: vi.fn(),
		},
		subscriptions: {
			create: vi.fn(),
			retrieve: vi.fn().mockResolvedValue({
				id: "sub_123",
				customer: "cus_abc",
				status: "active",
				items: { data: [{ id: "si_item1", price: { id: "price_starter" } }] },
				current_period_end: Math.floor(Date.now() / 1000) + 30 * 24 * 3600,
				cancel_at_period_end: false,
			}),
			update: vi.fn(),
			cancel: vi.fn(),
		},
		checkout: {
			sessions: {
				create: vi.fn().mockResolvedValue({
					id: "cs_123",
					url: "https://checkout.stripe.com/pay/cs_123",
				}),
			},
		},
		billingPortal: {
			sessions: {
				create: vi.fn().mockResolvedValue({
					id: "bps_123",
					url: "https://billing.stripe.com/session/bps_123",
				}),
			},
		},
		subscriptionItems: { createUsageRecord: vi.fn() },
		webhooks: { constructEvent: vi.fn() },
	};
}

function createMockDeps(overrides: Partial<BillingServiceDeps> = {}): BillingServiceDeps {
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
			queryUsage: vi.fn().mockResolvedValue(Ok([])),
			queryGatewayUsage: vi.fn().mockResolvedValue(Ok([])),
		},
		dashboardBaseUrl: "https://dashboard.lakesync.dev",
		...overrides,
	};
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Billing Service", () => {
	let deps: BillingServiceDeps;

	beforeEach(() => {
		deps = createMockDeps();
	});

	describe("getBillingInfo", () => {
		it("returns billing info with usage summary", async () => {
			(deps.usageRepo.queryUsage as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok([
					{
						gatewayId: "gw_1",
						orgId: "org_abc",
						eventType: "push_deltas",
						count: 500,
						windowStart: new Date(),
					},
					{
						gatewayId: "gw_1",
						orgId: "org_abc",
						eventType: "pull_deltas",
						count: 300,
						windowStart: new Date(),
					},
					{
						gatewayId: "gw_1",
						orgId: "org_abc",
						eventType: "api_call",
						count: 150,
						windowStart: new Date(),
					},
					{
						gatewayId: "gw_1",
						orgId: "org_abc",
						eventType: "storage_bytes",
						count: 1024,
						windowStart: new Date(),
					},
				]),
			);

			const result = await getBillingInfo("org_abc", deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.plan).toBe("starter");
				expect(result.value.planName).toBe("Starter");
				expect(result.value.usage.deltasThisPeriod).toBe(800);
				expect(result.value.usage.apiCalls).toBe(150);
				expect(result.value.usage.storageBytes).toBe(1024);
				expect(result.value.currentPeriodEnd).toBeDefined();
				expect(result.value.cancelAtPeriodEnd).toBe(false);
			}
		});

		it("returns NOT_FOUND for nonexistent org", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(Ok(null));

			const result = await getBillingInfo("org_unknown", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});

		it("returns billing info without subscription details for free plan", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(mockOrg({ plan: "free", stripeSubscriptionId: undefined })),
			);

			const result = await getBillingInfo("org_abc", deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.plan).toBe("free");
				expect(result.value.currentPeriodEnd).toBeUndefined();
			}
		});

		it("handles Stripe subscription retrieval failure gracefully", async () => {
			(deps.stripe.subscriptions.retrieve as ReturnType<typeof vi.fn>).mockRejectedValue(
				new Error("Stripe down"),
			);

			const result = await getBillingInfo("org_abc", deps);
			// Should still succeed — subscription details are non-fatal
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.currentPeriodEnd).toBeUndefined();
			}
		});

		it("handles usage query failure gracefully", async () => {
			(deps.usageRepo.queryUsage as ReturnType<typeof vi.fn>).mockResolvedValue(
				Err(new ControlPlaneError("DB timeout", "INTERNAL")),
			);

			const result = await getBillingInfo("org_abc", deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.usage.deltasThisPeriod).toBe(0);
			}
		});
	});

	describe("createCheckoutSession", () => {
		it("creates a Stripe checkout session for upgrade", async () => {
			const result = await createCheckoutSession("org_abc", "pro", deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.url).toContain("checkout.stripe.com");
			}
			expect(deps.stripe.checkout.sessions.create).toHaveBeenCalledWith({
				customer: "cus_abc",
				mode: "subscription",
				line_items: [{ price: "price_pro", quantity: 1 }],
				success_url: "https://dashboard.lakesync.dev/settings/billing?checkout=success",
				cancel_url: "https://dashboard.lakesync.dev/settings/billing?checkout=cancelled",
				metadata: { orgId: "org_abc", planId: "pro" },
			});
		});

		it("returns error for free plan", async () => {
			const result = await createCheckoutSession("org_abc", "free", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INVALID_INPUT");
			}
		});

		it("returns error when org has no Stripe customer", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(mockOrg({ stripeCustomerId: undefined })),
			);

			const result = await createCheckoutSession("org_abc", "pro", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INVALID_INPUT");
			}
		});

		it("returns error when Stripe API fails", async () => {
			(deps.stripe.checkout.sessions.create as ReturnType<typeof vi.fn>).mockRejectedValue(
				new Error("Stripe error"),
			);

			const result = await createCheckoutSession("org_abc", "pro", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INTERNAL");
			}
		});
	});

	describe("createPortalSession", () => {
		it("creates a Stripe billing portal session", async () => {
			const result = await createPortalSession("org_abc", deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.url).toContain("billing.stripe.com");
			}
			expect(deps.stripe.billingPortal.sessions.create).toHaveBeenCalledWith({
				customer: "cus_abc",
				return_url: "https://dashboard.lakesync.dev/settings/billing",
			});
		});

		it("returns error when org has no Stripe customer", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(mockOrg({ stripeCustomerId: undefined })),
			);

			const result = await createPortalSession("org_abc", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INVALID_INPUT");
			}
		});

		it("returns NOT_FOUND for nonexistent org", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(Ok(null));

			const result = await createPortalSession("org_unknown", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});
	});
});
