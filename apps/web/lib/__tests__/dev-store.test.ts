import { beforeEach, describe, expect, it } from "vitest";
import { devStore } from "@/lib/dev-store";

// Clear the global store between tests to avoid cross-test pollution
const g = globalThis as unknown as { __devGateways?: Map<string, unknown> };

beforeEach(() => {
	g.__devGateways?.clear();
});

describe("devStore gateway CRUD", () => {
	it("creates a gateway with generated id", () => {
		const gw = devStore.createGateway({ orgId: "org-1", name: "Test GW" });
		expect(gw.id).toMatch(/^gw-/);
		expect(gw.orgId).toBe("org-1");
		expect(gw.name).toBe("Test GW");
		expect(gw.status).toBe("active");
		expect(gw.createdAt).toBeTruthy();
	});

	it("gets a gateway by id", () => {
		const created = devStore.createGateway({ orgId: "org-1", name: "GW" });
		const fetched = devStore.getGateway(created.id);
		expect(fetched).toEqual(created);
	});

	it("returns undefined for non-existent gateway", () => {
		expect(devStore.getGateway("nonexistent")).toBeUndefined();
	});

	it("lists gateways filtered by orgId", () => {
		devStore.createGateway({ orgId: "org-1", name: "GW A" });
		devStore.createGateway({ orgId: "org-2", name: "GW B" });
		devStore.createGateway({ orgId: "org-1", name: "GW C" });

		const org1 = devStore.listGateways("org-1");
		expect(org1).toHaveLength(2);
		expect(org1.every((gw) => gw.orgId === "org-1")).toBe(true);

		const org2 = devStore.listGateways("org-2");
		expect(org2).toHaveLength(1);
		expect(org2[0].name).toBe("GW B");
	});

	it("updates a gateway and returns a new object", () => {
		const original = devStore.createGateway({ orgId: "org-1", name: "Old" });
		const updated = devStore.updateGateway(original.id, { name: "New" });

		expect(updated).toBeDefined();
		expect(updated!.name).toBe("New");
		expect(updated!.orgId).toBe("org-1");

		// The store contains the updated version
		const fetched = devStore.getGateway(original.id);
		expect(fetched!.name).toBe("New");
	});

	it("returns undefined when updating non-existent gateway", () => {
		expect(devStore.updateGateway("nonexistent", { name: "X" })).toBeUndefined();
	});

	it("deletes a gateway", () => {
		const gw = devStore.createGateway({ orgId: "org-1", name: "Doomed" });
		expect(devStore.deleteGateway(gw.id)).toBe(true);
		expect(devStore.getGateway(gw.id)).toBeUndefined();
	});

	it("returns false when deleting non-existent gateway", () => {
		expect(devStore.deleteGateway("nonexistent")).toBe(false);
	});
});

describe("devStore.getUsage", () => {
	it("returns days in the correct date range", () => {
		const result = devStore.getUsage("org-1", "2026-02-10", "2026-02-14");
		expect(result.data).toHaveLength(5);
		expect(result.data[0].date).toBe("2026-02-10");
		expect(result.data[4].date).toBe("2026-02-14");
	});

	it("totals match sum of daily values", () => {
		const result = devStore.getUsage("org-1", "2026-02-01", "2026-02-03");
		const sumPush = result.data.reduce((s, d) => s + d.pushDeltas, 0);
		const sumPull = result.data.reduce((s, d) => s + d.pullDeltas, 0);
		const sumApi = result.data.reduce((s, d) => s + d.apiCalls, 0);
		const sumStorage = result.data.reduce((s, d) => s + d.storageBytes, 0);

		expect(result.totals.pushDeltas).toBe(sumPush);
		expect(result.totals.pullDeltas).toBe(sumPull);
		expect(result.totals.apiCalls).toBe(sumApi);
		expect(result.totals.storageBytes).toBe(sumStorage);
	});
});

describe("devStore.getBilling", () => {
	it("returns expected billing shape", () => {
		const billing = devStore.getBilling("org-1");
		expect(billing.plan).toBe("free");
		expect(billing.planName).toBe("Free");
		expect(billing.price).toBe(0);
		expect(billing.maxDeltasPerMonth).toBeGreaterThan(0);
		expect(billing.maxStorageBytes).toBeGreaterThan(0);
		expect(billing.usage).toBeDefined();
		expect(billing.usage.deltasThisPeriod).toBeGreaterThanOrEqual(0);
		expect(billing.usage.storageBytes).toBeGreaterThanOrEqual(0);
		expect(billing.usage.apiCalls).toBeGreaterThanOrEqual(0);
	});
});
