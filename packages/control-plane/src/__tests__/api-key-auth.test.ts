import { Ok } from "@lakesync/core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { authenticateApiKey } from "../auth/api-key-auth";
import type { ApiKeyRepository } from "../repositories";

function createMockApiKeyRepo(): ApiKeyRepository {
	return {
		create: vi.fn(),
		getByHash: vi.fn().mockResolvedValue(Ok(null)),
		listByOrg: vi.fn().mockResolvedValue(Ok([])),
		revoke: vi.fn().mockResolvedValue(Ok(undefined)),
		updateLastUsed: vi.fn().mockResolvedValue(Ok(undefined)),
	};
}

describe("authenticateApiKey", () => {
	let repo: ApiKeyRepository;

	beforeEach(() => {
		repo = createMockApiKeyRepo();
	});

	it("rejects keys without lk_ prefix", async () => {
		const result = await authenticateApiKey("invalid_key", repo);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("INVALID_INPUT");
		}
	});

	it("returns NOT_FOUND for unknown keys", async () => {
		const result = await authenticateApiKey("lk_unknown_key_value", repo);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("NOT_FOUND");
		}
	});

	it("returns auth context for valid key", async () => {
		(repo.getByHash as ReturnType<typeof vi.fn>).mockResolvedValue(
			Ok({
				id: "key_123",
				orgId: "org_abc",
				gatewayId: "gw_xyz",
				name: "Test Key",
				keyHash: "hash",
				keyPrefix: "lk_ABCDEFgh",
				role: "admin",
				createdAt: new Date(),
			}),
		);

		const result = await authenticateApiKey("lk_some_valid_key", repo);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.apiKeyId).toBe("key_123");
			expect(result.value.orgId).toBe("org_abc");
			expect(result.value.gatewayId).toBe("gw_xyz");
			expect(result.value.role).toBe("admin");
		}
	});

	it("updates lastUsedAt on successful auth", async () => {
		(repo.getByHash as ReturnType<typeof vi.fn>).mockResolvedValue(
			Ok({
				id: "key_123",
				orgId: "org_abc",
				name: "Test Key",
				keyHash: "hash",
				keyPrefix: "lk_ABCDEFgh",
				role: "client",
				createdAt: new Date(),
			}),
		);

		await authenticateApiKey("lk_some_valid_key", repo);
		// updateLastUsed is fire-and-forget, but should have been called
		expect(repo.updateLastUsed).toHaveBeenCalledWith("key_123");
	});

	it("rejects expired keys", async () => {
		(repo.getByHash as ReturnType<typeof vi.fn>).mockResolvedValue(
			Ok({
				id: "key_123",
				orgId: "org_abc",
				name: "Expired Key",
				keyHash: "hash",
				keyPrefix: "lk_ABCDEFgh",
				role: "client",
				expiresAt: new Date("2020-01-01"),
				createdAt: new Date("2019-01-01"),
			}),
		);

		const result = await authenticateApiKey("lk_expired_key", repo);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("NOT_FOUND");
			expect(result.error.message).toContain("expired");
		}
	});
});
