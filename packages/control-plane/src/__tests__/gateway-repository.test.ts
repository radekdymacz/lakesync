import type { Pool } from "pg";
import { beforeEach, describe, expect, it } from "vitest";
import { PgGatewayRepository } from "../postgres/gateway-repository";
import { createMockPool, mockGatewayRow } from "./test-helpers";

describe("PgGatewayRepository", () => {
	let repo: PgGatewayRepository;
	let mock: ReturnType<typeof createMockPool>;

	beforeEach(() => {
		mock = createMockPool();
		repo = new PgGatewayRepository(mock.pool as unknown as Pool);
	});

	describe("create", () => {
		it("creates a gateway and returns Ok", async () => {
			mock.queueResult([mockGatewayRow({ name: "prod-gw", region: "us" })]);

			const result = await repo.create({
				orgId: "org_abc123",
				name: "prod-gw",
				region: "us",
			});
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.name).toBe("prod-gw");
				expect(result.value.region).toBe("us");
				expect(result.value.status).toBe("active");
			}
		});

		it("creates a gateway without region", async () => {
			mock.queueResult([mockGatewayRow()]);

			const result = await repo.create({
				orgId: "org_abc123",
				name: "Test Gateway",
			});
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.region).toBeUndefined();
			}
		});
	});

	describe("getById", () => {
		it("returns the gateway when found", async () => {
			mock.queueResult([mockGatewayRow()]);

			const result = await repo.getById("gw_abc123");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value?.id).toBe("gw_abc123");
			}
		});

		it("returns null when not found", async () => {
			mock.queueResult([]);

			const result = await repo.getById("nonexistent");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBeNull();
			}
		});
	});

	describe("listByOrg", () => {
		it("returns all gateways for an org", async () => {
			mock.queueResult([
				mockGatewayRow({ id: "gw_1", name: "GW 1" }),
				mockGatewayRow({ id: "gw_2", name: "GW 2" }),
			]);

			const result = await repo.listByOrg("org_abc123");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(2);
				expect(result.value[0]?.name).toBe("GW 1");
				expect(result.value[1]?.name).toBe("GW 2");
			}
		});

		it("returns empty array when org has no gateways", async () => {
			mock.queueResult([]);

			const result = await repo.listByOrg("org_empty");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(0);
			}
		});
	});

	describe("update", () => {
		it("updates gateway status", async () => {
			mock.queueResult([mockGatewayRow({ status: "suspended" })]);

			const result = await repo.update("gw_abc123", { status: "suspended" });
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.status).toBe("suspended");
			}
		});

		it("returns NOT_FOUND when gateway does not exist", async () => {
			mock.queueResult([]);

			const result = await repo.update("nonexistent", { name: "X" });
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});

		it("returns current gateway when no fields to update", async () => {
			mock.queueResult([mockGatewayRow()]);

			const result = await repo.update("gw_abc123", {});
			expect(result.ok).toBe(true);
		});
	});

	describe("delete", () => {
		it("deletes a gateway", async () => {
			mock.mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 1 });

			const result = await repo.delete("gw_abc123");
			expect(result.ok).toBe(true);
		});

		it("returns NOT_FOUND when gateway does not exist", async () => {
			mock.mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 });

			const result = await repo.delete("nonexistent");
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});
	});
});
