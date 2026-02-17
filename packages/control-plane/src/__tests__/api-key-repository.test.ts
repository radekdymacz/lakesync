import type { Pool } from "pg";
import { beforeEach, describe, expect, it } from "vitest";
import { ControlPlaneError } from "../errors";
import { PgApiKeyRepository } from "../postgres/api-key-repository";
import { createMockPool, mockApiKeyRow } from "./test-helpers";

describe("PgApiKeyRepository", () => {
	let repo: PgApiKeyRepository;
	let mock: ReturnType<typeof createMockPool>;

	beforeEach(() => {
		mock = createMockPool();
		repo = new PgApiKeyRepository(mock.pool as unknown as Pool);
	});

	describe("create", () => {
		it("creates an API key and returns rawKey plus stored key", async () => {
			mock.queueResult([mockApiKeyRow()]);

			const result = await repo.create({
				orgId: "org_abc123",
				name: "My Key",
				role: "client",
			});
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.rawKey).toMatch(/^lk_/);
				expect(result.value.rawKey.length).toBeGreaterThan(10);
				expect(result.value.apiKey.name).toBe("Test Key");
				expect(result.value.apiKey.role).toBe("client");
			}
		});

		it("stores hash, not raw key", async () => {
			mock.queueResult([mockApiKeyRow()]);

			await repo.create({
				orgId: "org_abc123",
				name: "My Key",
				role: "admin",
			});

			// The query should have been called with a SHA-256 hash (hex, 64 chars)
			const queryArgs = mock.mockQuery.mock.calls[0];
			const keyHash = queryArgs?.[1]?.[4] as string;
			expect(keyHash).toMatch(/^[0-9a-f]{64}$/);
		});

		it("creates gateway-scoped key", async () => {
			mock.queueResult([mockApiKeyRow({ gateway_id: "gw_123" })]);

			const result = await repo.create({
				orgId: "org_abc123",
				gatewayId: "gw_123",
				name: "GW Key",
				role: "client",
			});
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.apiKey.gatewayId).toBe("gw_123");
			}
		});
	});

	describe("getByHash", () => {
		it("returns API key when found", async () => {
			mock.queueResult([mockApiKeyRow()]);

			const result = await repo.getByHash("abc123hash");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value?.keyHash).toBe("abc123hash");
			}
		});

		it("returns null when not found", async () => {
			mock.queueResult([]);

			const result = await repo.getByHash("nonexistent");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBeNull();
			}
		});
	});

	describe("listByOrg", () => {
		it("returns all keys for an org", async () => {
			mock.queueResult([
				mockApiKeyRow({ id: "key_1", name: "Key 1" }),
				mockApiKeyRow({ id: "key_2", name: "Key 2" }),
			]);

			const result = await repo.listByOrg("org_abc123");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(2);
			}
		});
	});

	describe("revoke", () => {
		it("revokes an existing key", async () => {
			mock.mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 1 });

			const result = await repo.revoke("key_abc123");
			expect(result.ok).toBe(true);
		});

		it("returns NOT_FOUND for nonexistent key", async () => {
			mock.mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 });

			const result = await repo.revoke("nonexistent");
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});
	});

	describe("updateLastUsed", () => {
		it("updates last_used_at timestamp", async () => {
			mock.mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 1 });

			const result = await repo.updateLastUsed("key_abc123");
			expect(result.ok).toBe(true);
			expect(mock.mockQuery).toHaveBeenCalledWith(
				expect.stringContaining("last_used_at"),
				["key_abc123"],
			);
		});
	});
});
