import { beforeEach, describe, expect, it } from "vitest";
import { createBackend, resolveOrgId } from "@/lib/backend";
import { DEV_ORG_ID } from "@/lib/auth-config";

// Clear the global dev store between tests
const g = globalThis as unknown as { __devGateways?: Map<string, unknown> };

beforeEach(() => {
	g.__devGateways?.clear();
});

describe("createBackend (dev mode)", () => {
	const backend = createBackend();

	it("returns a BackendProvider", () => {
		expect(backend).toBeDefined();
		expect(backend.gateways).toBeDefined();
		expect(backend.usage).toBeDefined();
		expect(backend.billing).toBeDefined();
	});

	describe("gateways", () => {
		it("list returns empty array initially", async () => {
			const list = await backend.gateways.list("org-1");
			expect(list).toEqual([]);
		});

		it("create returns a gateway with generated id", async () => {
			const gw = await backend.gateways.create({ orgId: "org-1", name: "Test" });
			expect(gw.id).toMatch(/^gw-/);
			expect(gw.orgId).toBe("org-1");
			expect(gw.name).toBe("Test");
			expect(gw.status).toBe("active");
		});

		it("get returns created gateway", async () => {
			const created = await backend.gateways.create({ orgId: "org-1", name: "Test" });
			const fetched = await backend.gateways.get(created.id);
			expect(fetched).toEqual(created);
		});

		it("list filters by orgId", async () => {
			await backend.gateways.create({ orgId: "org-1", name: "A" });
			await backend.gateways.create({ orgId: "org-2", name: "B" });

			const org1 = await backend.gateways.list("org-1");
			expect(org1).toHaveLength(1);
			expect(org1[0].name).toBe("A");
		});

		it("update returns updated gateway", async () => {
			const gw = await backend.gateways.create({ orgId: "org-1", name: "Old" });
			const updated = await backend.gateways.update(gw.id, { name: "New" });
			expect(updated).toBeDefined();
			expect(updated!.name).toBe("New");
		});

		it("delete returns true for existing gateway", async () => {
			const gw = await backend.gateways.create({ orgId: "org-1", name: "Doomed" });
			const result = await backend.gateways.delete(gw.id);
			expect(result).toBe(true);

			const fetched = await backend.gateways.get(gw.id);
			expect(fetched).toBeUndefined();
		});
	});

	describe("usage", () => {
		it("returns data array and totals", async () => {
			const result = await backend.usage.get("org-1", "2026-02-10", "2026-02-12");
			expect(result.data).toHaveLength(3);
			expect(result.totals).toBeDefined();
		});
	});

	describe("billing", () => {
		it("get returns billing info", async () => {
			const billing = await backend.billing.get("org-1");
			expect(billing).toBeDefined();
			expect((billing as { plan: string }).plan).toBe("free");
		});

		it("checkout returns dev mode message", async () => {
			const result = await backend.billing.checkout("org-1", "pro");
			expect(result.message).toContain("not available in dev mode");
		});

		it("portal returns dev mode message", async () => {
			const result = await backend.billing.portal("org-1");
			expect(result.message).toContain("not available in dev mode");
		});
	});
});

describe("resolveOrgId", () => {
	it("returns provided orgId when given", () => {
		expect(resolveOrgId("org-abc")).toBe("org-abc");
	});

	it("returns DEV_ORG_ID when null and CLERK_ENABLED is false", () => {
		expect(resolveOrgId(null)).toBe(DEV_ORG_ID);
	});
});
