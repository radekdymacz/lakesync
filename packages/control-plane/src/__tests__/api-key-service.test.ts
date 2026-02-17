import { Err, Ok } from "@lakesync/core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ApiKeyServiceDeps } from "../api/api-key-service";
import {
	createApiKey,
	listApiKeys,
	revokeApiKey,
	rotateApiKeyWithInput,
} from "../api/api-key-service";
import type { ApiKey, Gateway } from "../entities";
import { ControlPlaneError } from "../errors";

function mockApiKey(overrides: Partial<ApiKey> = {}): ApiKey {
	return {
		id: "key_123",
		orgId: "org_abc",
		name: "Test Key",
		keyHash: "abc",
		keyPrefix: "lk_ABCDEFgh",
		role: "client",
		createdAt: new Date(),
		...overrides,
	};
}

function mockGw(orgId = "org_abc"): Gateway {
	return {
		id: "gw_123",
		orgId,
		name: "Test GW",
		status: "active",
		createdAt: new Date(),
		updatedAt: new Date(),
	};
}

function createMockDeps(): ApiKeyServiceDeps {
	return {
		apiKeyRepo: {
			create: vi.fn().mockResolvedValue(Ok({ apiKey: mockApiKey(), rawKey: "lk_test_raw_key" })),
			getByHash: vi.fn().mockResolvedValue(Ok(null)),
			listByOrg: vi.fn().mockResolvedValue(Ok([])),
			revoke: vi.fn().mockResolvedValue(Ok(undefined)),
			updateLastUsed: vi.fn().mockResolvedValue(Ok(undefined)),
		},
		gatewayRepo: {
			create: vi.fn(),
			getById: vi.fn().mockResolvedValue(Ok(mockGw())),
			listByOrg: vi.fn(),
			update: vi.fn(),
			delete: vi.fn(),
		},
	};
}

describe("API Key Service", () => {
	let deps: ApiKeyServiceDeps;

	beforeEach(() => {
		deps = createMockDeps();
	});

	describe("createApiKey", () => {
		it("creates an org-wide API key", async () => {
			const result = await createApiKey({ orgId: "org_abc", name: "My Key", role: "client" }, deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.rawKey).toContain("lk_");
			}
		});

		it("creates a gateway-scoped API key", async () => {
			const result = await createApiKey(
				{ orgId: "org_abc", gatewayId: "gw_123", name: "GW Key", role: "admin" },
				deps,
			);
			expect(result.ok).toBe(true);
		});

		it("rejects gateway-scoped key when gateway does not exist", async () => {
			(deps.gatewayRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(Ok(null));

			const result = await createApiKey(
				{ orgId: "org_abc", gatewayId: "gw_unknown", name: "Key", role: "client" },
				deps,
			);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});

		it("rejects gateway-scoped key when gateway belongs to different org", async () => {
			(deps.gatewayRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(mockGw("org_other")),
			);

			const result = await createApiKey(
				{ orgId: "org_abc", gatewayId: "gw_123", name: "Key", role: "client" },
				deps,
			);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INVALID_INPUT");
			}
		});
	});

	describe("listApiKeys", () => {
		it("lists all keys for an org", async () => {
			(deps.apiKeyRepo.listByOrg as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok([mockApiKey({ id: "key_1" }), mockApiKey({ id: "key_2" })]),
			);

			const result = await listApiKeys("org_abc", deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(2);
			}
		});
	});

	describe("revokeApiKey", () => {
		it("revokes a key", async () => {
			const result = await revokeApiKey("key_123", deps);
			expect(result.ok).toBe(true);
			expect(deps.apiKeyRepo.revoke).toHaveBeenCalledWith("key_123");
		});
	});

	describe("rotateApiKeyWithInput", () => {
		it("revokes old key and creates new one", async () => {
			const result = await rotateApiKeyWithInput(
				"key_old",
				{ orgId: "org_abc", name: "New Key", role: "admin" },
				deps,
			);
			expect(result.ok).toBe(true);
			expect(deps.apiKeyRepo.revoke).toHaveBeenCalledWith("key_old");
			expect(deps.apiKeyRepo.create).toHaveBeenCalledOnce();
		});

		it("returns error if revocation fails", async () => {
			(deps.apiKeyRepo.revoke as ReturnType<typeof vi.fn>).mockResolvedValue(
				Err(new ControlPlaneError("Not found", "NOT_FOUND")),
			);

			const result = await rotateApiKeyWithInput(
				"key_nonexistent",
				{ orgId: "org_abc", name: "New Key", role: "admin" },
				deps,
			);
			expect(result.ok).toBe(false);
		});
	});
});
