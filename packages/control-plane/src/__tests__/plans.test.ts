import { describe, expect, it } from "vitest";
import type { PlanId } from "../entities";
import { getPlan, PLANS } from "../plans";

describe("Plans", () => {
	it("defines all four plan tiers", () => {
		const ids: PlanId[] = ["free", "starter", "pro", "enterprise"];
		for (const id of ids) {
			expect(PLANS[id]).toBeDefined();
			expect(PLANS[id].id).toBe(id);
		}
	});

	it("free plan has correct limits", () => {
		const plan = PLANS.free;
		expect(plan.maxGateways).toBe(1);
		expect(plan.maxDeltasPerMonth).toBe(10_000);
		expect(plan.maxStorageBytes).toBe(100 * 1024 * 1024);
		expect(plan.price).toBe(0);
	});

	it("starter plan has correct limits", () => {
		const plan = PLANS.starter;
		expect(plan.maxGateways).toBe(3);
		expect(plan.maxDeltasPerMonth).toBe(100_000);
		expect(plan.maxStorageBytes).toBe(1024 * 1024 * 1024);
		expect(plan.price).toBe(2900);
	});

	it("pro plan has correct limits", () => {
		const plan = PLANS.pro;
		expect(plan.maxGateways).toBe(10);
		expect(plan.maxDeltasPerMonth).toBe(1_000_000);
		expect(plan.maxStorageBytes).toBe(10 * 1024 * 1024 * 1024);
		expect(plan.price).toBe(9900);
	});

	it("enterprise plan uses -1 for unlimited", () => {
		const plan = PLANS.enterprise;
		expect(plan.maxGateways).toBe(-1);
		expect(plan.maxDeltasPerMonth).toBe(-1);
		expect(plan.maxStorageBytes).toBe(-1);
		expect(plan.price).toBe(-1);
	});

	it("getPlan returns the correct plan", () => {
		expect(getPlan("free").name).toBe("Free");
		expect(getPlan("pro").name).toBe("Pro");
	});

	it("each plan has a valid name", () => {
		for (const plan of Object.values(PLANS)) {
			expect(plan.name).toBeTruthy();
			expect(typeof plan.name).toBe("string");
		}
	});

	it("non-enterprise plans have positive rate limits", () => {
		expect(PLANS.free.rateLimit).toBeGreaterThan(0);
		expect(PLANS.starter.rateLimit).toBeGreaterThan(0);
		expect(PLANS.pro.rateLimit).toBeGreaterThan(0);
	});
});
