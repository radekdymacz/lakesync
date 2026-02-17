import { Ok } from "@lakesync/core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { StripeWebhookDeps } from "../billing/stripe-webhook";
import { processWebhookEvent, verifyWebhookSignature } from "../billing/stripe-webhook";
import type { StripeClient, StripeWebhookEvent } from "../billing/stripe-types";
import type { Gateway, Organisation } from "../entities";

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

function mockGw(overrides: Partial<Gateway> = {}): Gateway {
	return {
		id: "gw_123",
		orgId: "org_abc",
		name: "Test GW",
		status: "active",
		createdAt: new Date("2025-01-01"),
		updatedAt: new Date(),
		...overrides,
	};
}

function mockEvent(type: string, data: Record<string, unknown> = {}): StripeWebhookEvent {
	return {
		id: "evt_123",
		type,
		data: {
			object: {
				metadata: { orgId: "org_abc" },
				...data,
			},
		},
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
			retrieve: vi.fn(),
			update: vi.fn(),
			cancel: vi.fn(),
		},
		checkout: { sessions: { create: vi.fn() } },
		billingPortal: { sessions: { create: vi.fn() } },
		subscriptionItems: { createUsageRecord: vi.fn() },
		webhooks: {
			constructEvent: vi.fn(),
		},
	};
}

function createMockDeps(overrides: Partial<StripeWebhookDeps> = {}): StripeWebhookDeps {
	return {
		stripe: createMockStripe(),
		orgRepo: {
			create: vi.fn(),
			getById: vi.fn().mockResolvedValue(Ok(mockOrg())),
			getBySlug: vi.fn(),
			update: vi.fn().mockImplementation((_id, input) =>
				Promise.resolve(Ok(mockOrg(input))),
			),
			delete: vi.fn(),
		},
		gatewayRepo: {
			create: vi.fn(),
			getById: vi.fn().mockResolvedValue(Ok(mockGw())),
			listByOrg: vi.fn().mockResolvedValue(Ok([])),
			update: vi.fn().mockImplementation((_id, input) =>
				Promise.resolve(Ok(mockGw(input))),
			),
			delete: vi.fn(),
		},
		...overrides,
	};
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Stripe Webhook", () => {
	let deps: StripeWebhookDeps;

	beforeEach(() => {
		deps = createMockDeps();
	});

	describe("verifyWebhookSignature", () => {
		it("returns Ok with event when signature is valid", () => {
			const event: StripeWebhookEvent = { id: "evt_1", type: "test", data: { object: {} } };
			(deps.stripe.webhooks.constructEvent as ReturnType<typeof vi.fn>).mockReturnValue(event);

			const result = verifyWebhookSignature("payload", "sig", "whsec_test", deps.stripe);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.id).toBe("evt_1");
			}
		});

		it("returns Err when signature is invalid", () => {
			(deps.stripe.webhooks.constructEvent as ReturnType<typeof vi.fn>).mockImplementation(
				() => {
					throw new Error("Invalid signature");
				},
			);

			const result = verifyWebhookSignature("payload", "bad_sig", "whsec_test", deps.stripe);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INVALID_INPUT");
				expect(result.error.message).toContain("Invalid webhook signature");
			}
		});
	});

	describe("processWebhookEvent", () => {
		describe("customer.subscription.updated", () => {
			it("updates org plan based on subscription price", async () => {
				const event = mockEvent("customer.subscription.updated", {
					items: { data: [{ price: { id: "price_pro" } }] },
				});

				const result = await processWebhookEvent(event, deps);
				expect(result.ok).toBe(true);
				if (result.ok) {
					expect(result.value.handled).toBe(true);
					expect(result.value.orgId).toBe("org_abc");
				}
				expect(deps.orgRepo.update).toHaveBeenCalledWith("org_abc", { plan: "pro" });
			});

			it("handles unknown price ID gracefully", async () => {
				const event = mockEvent("customer.subscription.updated", {
					items: { data: [{ price: { id: "price_unknown" } }] },
				});

				const result = await processWebhookEvent(event, deps);
				expect(result.ok).toBe(true);
				if (result.ok) {
					expect(result.value.handled).toBe(true);
				}
				// No plan update when price doesn't match
				expect(deps.orgRepo.update).not.toHaveBeenCalled();
			});
		});

		describe("customer.subscription.deleted", () => {
			it("downgrades org to free plan and clears subscription ID", async () => {
				const event = mockEvent("customer.subscription.deleted");

				const result = await processWebhookEvent(event, deps);
				expect(result.ok).toBe(true);
				if (result.ok) {
					expect(result.value.handled).toBe(true);
				}
				expect(deps.orgRepo.update).toHaveBeenCalledWith("org_abc", {
					plan: "free",
					stripeSubscriptionId: undefined,
				});
			});

			it("suspends excess gateways when over free plan limit", async () => {
				// Free plan allows 1 gateway, org has 3 active ones
				(deps.gatewayRepo.listByOrg as ReturnType<typeof vi.fn>).mockResolvedValue(
					Ok([
						mockGw({ id: "gw_1", createdAt: new Date("2025-01-01") }),
						mockGw({ id: "gw_2", createdAt: new Date("2025-02-01") }),
						mockGw({ id: "gw_3", createdAt: new Date("2025-03-01") }),
					]),
				);

				const event = mockEvent("customer.subscription.deleted");
				const result = await processWebhookEvent(event, deps);
				expect(result.ok).toBe(true);

				// Should suspend gw_2 and gw_3 (keep oldest gw_1)
				expect(deps.gatewayRepo.update).toHaveBeenCalledWith("gw_2", { status: "suspended" });
				expect(deps.gatewayRepo.update).toHaveBeenCalledWith("gw_3", { status: "suspended" });
			});

			it("does not suspend when under free plan limit", async () => {
				(deps.gatewayRepo.listByOrg as ReturnType<typeof vi.fn>).mockResolvedValue(
					Ok([mockGw({ id: "gw_1" })]),
				);

				const event = mockEvent("customer.subscription.deleted");
				await processWebhookEvent(event, deps);

				// Only the org update call, no gateway suspend calls
				expect(deps.gatewayRepo.update).not.toHaveBeenCalled();
			});
		});

		describe("invoice.payment_succeeded", () => {
			it("reactivates suspended gateways", async () => {
				(deps.gatewayRepo.listByOrg as ReturnType<typeof vi.fn>).mockResolvedValue(
					Ok([
						mockGw({ id: "gw_1", status: "suspended" }),
						mockGw({ id: "gw_2", status: "active" }),
					]),
				);

				const event = mockEvent("invoice.payment_succeeded", {
					customer: "cus_abc",
				});
				const result = await processWebhookEvent(event, deps);
				expect(result.ok).toBe(true);

				// Only suspended gateway should be reactivated
				expect(deps.gatewayRepo.update).toHaveBeenCalledWith("gw_1", { status: "active" });
				expect(deps.gatewayRepo.update).toHaveBeenCalledTimes(1);
			});
		});

		describe("invoice.payment_failed", () => {
			it("suspends all active gateways when no scheduler is provided", async () => {
				(deps.gatewayRepo.listByOrg as ReturnType<typeof vi.fn>).mockResolvedValue(
					Ok([
						mockGw({ id: "gw_1", status: "active" }),
						mockGw({ id: "gw_2", status: "active" }),
					]),
				);

				const event = mockEvent("invoice.payment_failed");
				const result = await processWebhookEvent(event, deps);
				expect(result.ok).toBe(true);

				expect(deps.gatewayRepo.update).toHaveBeenCalledWith("gw_1", { status: "suspended" });
				expect(deps.gatewayRepo.update).toHaveBeenCalledWith("gw_2", { status: "suspended" });
			});

			it("schedules suspension when scheduler is provided", async () => {
				const scheduleSuspension = vi.fn();
				deps = createMockDeps({ scheduleSuspension });

				const event = mockEvent("invoice.payment_failed");
				const result = await processWebhookEvent(event, deps);
				expect(result.ok).toBe(true);

				expect(scheduleSuspension).toHaveBeenCalledWith("org_abc", expect.any(Date));
				// Grace period should be ~3 days from now
				const firstCall = scheduleSuspension.mock.calls[0] as [string, Date] | undefined;
				if (!firstCall) throw new Error("Expected scheduleSuspension to have been called");
				const suspendAt = firstCall[1];
				const threeDaysMs = 3 * 24 * 60 * 60 * 1000;
				const diff = suspendAt.getTime() - Date.now();
				expect(diff).toBeGreaterThan(threeDaysMs - 5000);
				expect(diff).toBeLessThan(threeDaysMs + 5000);

				// No direct gateway suspension
				expect(deps.gatewayRepo.update).not.toHaveBeenCalled();
			});

			it("returns handled=false when no orgId in metadata", async () => {
				const event: StripeWebhookEvent = {
					id: "evt_1",
					type: "invoice.payment_failed",
					data: { object: { customer: "cus_abc" } },
				};

				const result = await processWebhookEvent(event, deps);
				expect(result.ok).toBe(true);
				if (result.ok) {
					expect(result.value.handled).toBe(false);
				}
			});
		});

		describe("unknown event types", () => {
			it("acknowledges but does not process unknown events", async () => {
				const event = mockEvent("charge.succeeded");
				const result = await processWebhookEvent(event, deps);
				expect(result.ok).toBe(true);
				if (result.ok) {
					expect(result.value.handled).toBe(false);
					expect(result.value.eventType).toBe("charge.succeeded");
				}
			});
		});

		describe("missing metadata", () => {
			it("returns error when orgId is missing from subscription event", async () => {
				const event: StripeWebhookEvent = {
					id: "evt_1",
					type: "customer.subscription.updated",
					data: { object: { items: { data: [] } } },
				};

				const result = await processWebhookEvent(event, deps);
				expect(result.ok).toBe(false);
				if (!result.ok) {
					expect(result.error.code).toBe("INVALID_INPUT");
					expect(result.error.message).toContain("missing orgId");
				}
			});
		});
	});
});
