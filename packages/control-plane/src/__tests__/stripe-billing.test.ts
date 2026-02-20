import { Err, Ok } from "@lakesync/core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { StripeBillingDeps } from "../billing/stripe-billing";
import {
	cancelSubscription,
	createCustomer,
	createSubscription,
	updateSubscription,
} from "../billing/stripe-billing";
import type { StripeClient, StripeSubscription } from "../billing/stripe-types";
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
		plan: "free",
		createdAt: new Date(),
		updatedAt: new Date(),
		...overrides,
	};
}

function mockSubscription(overrides: Partial<StripeSubscription> = {}): StripeSubscription {
	return {
		id: "sub_123",
		customer: "cus_abc",
		status: "active",
		items: {
			data: [{ id: "si_item1", price: { id: "price_starter" } }],
		},
		current_period_end: Math.floor(Date.now() / 1000) + 30 * 24 * 3600,
		cancel_at_period_end: false,
		...overrides,
	};
}

function createMockStripe(): StripeClient {
	return {
		customers: {
			create: vi
				.fn()
				.mockResolvedValue({ id: "cus_new", email: null, name: "Test Org", metadata: {} }),
			retrieve: vi
				.fn()
				.mockResolvedValue({ id: "cus_abc", email: null, name: "Test Org", metadata: {} }),
		},
		subscriptions: {
			create: vi.fn().mockResolvedValue(mockSubscription()),
			retrieve: vi.fn().mockResolvedValue(mockSubscription()),
			update: vi.fn().mockResolvedValue(mockSubscription({ cancel_at_period_end: true })),
			cancel: vi.fn().mockResolvedValue(mockSubscription({ status: "canceled" })),
		},
		checkout: {
			sessions: {
				create: vi
					.fn()
					.mockResolvedValue({ id: "cs_123", url: "https://checkout.stripe.com/pay/cs_123" }),
			},
		},
		billingPortal: {
			sessions: {
				create: vi
					.fn()
					.mockResolvedValue({ id: "bps_123", url: "https://billing.stripe.com/session/bps_123" }),
			},
		},
		subscriptionItems: {
			createUsageRecord: vi.fn().mockResolvedValue({
				id: "ur_123",
				quantity: 100,
				timestamp: 0,
				subscription_item: "si_item1",
			}),
		},
		webhooks: {
			constructEvent: vi.fn(),
		},
	};
}

function createMockDeps(overrides: Partial<StripeBillingDeps> = {}): StripeBillingDeps {
	return {
		stripe: createMockStripe(),
		orgRepo: {
			create: vi.fn().mockResolvedValue(Ok(mockOrg())),
			getById: vi.fn().mockResolvedValue(Ok(mockOrg())),
			getBySlug: vi.fn(),
			update: vi.fn().mockImplementation((_id, input) => Promise.resolve(Ok(mockOrg(input)))),
			delete: vi.fn(),
		},
		...overrides,
	};
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Stripe Billing", () => {
	let deps: StripeBillingDeps;

	beforeEach(() => {
		deps = createMockDeps();
	});

	describe("createCustomer", () => {
		it("creates a Stripe customer and stores the ID on the org", async () => {
			const org = mockOrg();
			const result = await createCustomer(org, deps);

			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBe("cus_new");
			}
			expect(deps.stripe.customers.create).toHaveBeenCalledWith({
				name: "Test Org",
				metadata: { orgId: "org_abc", slug: "test-org" },
			});
			expect(deps.orgRepo.update).toHaveBeenCalledWith("org_abc", {
				stripeCustomerId: "cus_new",
			});
		});

		it("returns existing customer ID without creating a new one", async () => {
			const org = mockOrg({ stripeCustomerId: "cus_existing" });
			const result = await createCustomer(org, deps);

			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBe("cus_existing");
			}
			expect(deps.stripe.customers.create).not.toHaveBeenCalled();
		});

		it("returns error when Stripe API fails", async () => {
			(deps.stripe.customers.create as ReturnType<typeof vi.fn>).mockRejectedValue(
				new Error("Stripe is down"),
			);

			const result = await createCustomer(mockOrg(), deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INTERNAL");
				expect(result.error.message).toContain("Failed to create Stripe customer");
			}
		});

		it("returns error when org update fails", async () => {
			(deps.orgRepo.update as ReturnType<typeof vi.fn>).mockResolvedValue(
				Err(new ControlPlaneError("DB error", "INTERNAL")),
			);

			const result = await createCustomer(mockOrg(), deps);
			expect(result.ok).toBe(false);
		});
	});

	describe("createSubscription", () => {
		it("creates a subscription for a paid plan", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(mockOrg({ stripeCustomerId: "cus_abc" })),
			);

			const result = await createSubscription("org_abc", "starter", deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.id).toBe("sub_123");
			}
			expect(deps.stripe.subscriptions.create).toHaveBeenCalled();
			expect(deps.orgRepo.update).toHaveBeenCalledWith("org_abc", {
				plan: "starter",
				stripeSubscriptionId: "sub_123",
			});
		});

		it("returns error for free plan (no stripePriceId)", async () => {
			const result = await createSubscription("org_abc", "free", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INVALID_INPUT");
				expect(result.error.message).toContain("no subscription needed");
			}
		});

		it("returns error when org has no Stripe customer", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(mockOrg()), // no stripeCustomerId
			);

			const result = await createSubscription("org_abc", "starter", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INVALID_INPUT");
				expect(result.error.message).toContain("no Stripe customer");
			}
		});

		it("returns NOT_FOUND for nonexistent org", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(Ok(null));

			const result = await createSubscription("org_unknown", "starter", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});
	});

	describe("updateSubscription", () => {
		it("updates subscription to a new plan with proration", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(
					mockOrg({
						stripeCustomerId: "cus_abc",
						stripeSubscriptionId: "sub_123",
						plan: "starter",
					}),
				),
			);

			const result = await updateSubscription("org_abc", "pro", deps);
			expect(result.ok).toBe(true);
			expect(deps.stripe.subscriptions.update).toHaveBeenCalledWith("sub_123", {
				items: [{ id: "si_item1", price: "price_pro" }],
				proration_behavior: "create_prorations",
				metadata: { orgId: "org_abc", planId: "pro" },
			});
			expect(deps.orgRepo.update).toHaveBeenCalledWith("org_abc", { plan: "pro" });
		});

		it("returns error for free plan target (use cancelSubscription instead)", async () => {
			const result = await updateSubscription("org_abc", "free", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INVALID_INPUT");
				expect(result.error.message).toContain("cancelSubscription");
			}
		});

		it("returns error when org has no subscription", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(mockOrg({ stripeCustomerId: "cus_abc" })), // no subscriptionId
			);

			const result = await updateSubscription("org_abc", "pro", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INVALID_INPUT");
				expect(result.error.message).toContain("no active subscription");
			}
		});

		it("returns error when Stripe API fails", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(
					mockOrg({
						stripeCustomerId: "cus_abc",
						stripeSubscriptionId: "sub_123",
					}),
				),
			);
			(deps.stripe.subscriptions.update as ReturnType<typeof vi.fn>).mockRejectedValue(
				new Error("Stripe error"),
			);

			const result = await updateSubscription("org_abc", "pro", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INTERNAL");
			}
		});
	});

	describe("cancelSubscription", () => {
		it("cancels subscription at period end", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(
					mockOrg({
						stripeCustomerId: "cus_abc",
						stripeSubscriptionId: "sub_123",
					}),
				),
			);

			const result = await cancelSubscription("org_abc", deps);
			expect(result.ok).toBe(true);
			expect(deps.stripe.subscriptions.update).toHaveBeenCalledWith("sub_123", {
				cancel_at_period_end: true,
			});
		});

		it("returns error when org has no subscription", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(mockOrg()), // no subscriptionId
			);

			const result = await cancelSubscription("org_abc", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INVALID_INPUT");
			}
		});

		it("returns NOT_FOUND for nonexistent org", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(Ok(null));

			const result = await cancelSubscription("org_unknown", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});

		it("returns error when Stripe API fails", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(
					mockOrg({
						stripeCustomerId: "cus_abc",
						stripeSubscriptionId: "sub_123",
					}),
				),
			);
			(deps.stripe.subscriptions.update as ReturnType<typeof vi.fn>).mockRejectedValue(
				new Error("Network error"),
			);

			const result = await cancelSubscription("org_abc", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INTERNAL");
			}
		});
	});
});
