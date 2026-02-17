import { Err, Ok } from "@lakesync/core";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ControlPlaneError } from "../errors";
import type { DeletionServiceDeps } from "../gdpr/deletion-service";
import {
	clearDeletionRequests,
	createDeletionRequest,
	exportOrgData,
	getDeletionRequest,
	processDeletionRequest,
} from "../gdpr/deletion-service";
import { mockApiKeyRow, mockGatewayRow, mockMemberRow, mockOrgRow } from "./test-helpers";

function toOrg(row: Record<string, unknown>) {
	return {
		id: row.id,
		name: row.name,
		slug: row.slug,
		plan: row.plan,
		createdAt: new Date(row.created_at as string),
		updatedAt: new Date(row.updated_at as string),
	};
}

function createMockDeps(): DeletionServiceDeps {
	return {
		orgRepo: {
			create: vi.fn(),
			getById: vi.fn().mockResolvedValue(Ok(toOrg(mockOrgRow()))),
			getBySlug: vi.fn(),
			update: vi.fn(),
			delete: vi.fn().mockResolvedValue(Ok(undefined)),
		},
		gatewayRepo: {
			create: vi.fn(),
			getById: vi.fn(),
			listByOrg: vi.fn().mockResolvedValue(Ok([])),
			update: vi.fn(),
			delete: vi.fn().mockResolvedValue(Ok(undefined)),
		},
		apiKeyRepo: {
			create: vi.fn(),
			getByHash: vi.fn(),
			listByOrg: vi.fn().mockResolvedValue(Ok([])),
			revoke: vi.fn(),
			updateLastUsed: vi.fn(),
		},
		memberRepo: {
			add: vi.fn(),
			remove: vi.fn().mockResolvedValue(Ok(undefined)),
			listByOrg: vi.fn().mockResolvedValue(Ok([])),
			getRole: vi.fn(),
			updateRole: vi.fn(),
		},
	};
}

describe("GDPR Deletion Service", () => {
	let deps: DeletionServiceDeps;

	beforeEach(() => {
		deps = createMockDeps();
	});

	afterEach(() => {
		clearDeletionRequests();
	});

	describe("createDeletionRequest", () => {
		it("creates a pending deletion request", async () => {
			const result = await createDeletionRequest(
				{ orgId: "org_abc123", scope: "user", targetId: "user_123" },
				deps,
			);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.status).toBe("pending");
				expect(result.value.scope).toBe("user");
				expect(result.value.targetId).toBe("user_123");
				expect(result.value.id).toBeTruthy();
			}
		});

		it("rejects missing scope", async () => {
			const result = await createDeletionRequest(
				{ orgId: "org_abc123", scope: "" as "user", targetId: "user_123" },
				deps,
			);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INVALID_INPUT");
			}
		});

		it("rejects missing targetId", async () => {
			const result = await createDeletionRequest(
				{ orgId: "org_abc123", scope: "user", targetId: "" },
				deps,
			);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INVALID_INPUT");
			}
		});

		it("returns NOT_FOUND for nonexistent org", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(Ok(null));

			const result = await createDeletionRequest(
				{ orgId: "org_unknown", scope: "user", targetId: "user_123" },
				deps,
			);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});
	});

	describe("getDeletionRequest", () => {
		it("returns the request by ID", async () => {
			const created = await createDeletionRequest(
				{ orgId: "org_abc123", scope: "user", targetId: "user_123" },
				deps,
			);
			expect(created.ok).toBe(true);
			if (!created.ok) return;

			const result = await getDeletionRequest(created.value.id);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value?.id).toBe(created.value.id);
			}
		});

		it("returns null for nonexistent request", async () => {
			const result = await getDeletionRequest("nonexistent");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBeNull();
			}
		});
	});

	describe("processDeletionRequest — user scope", () => {
		it("removes user from org membership", async () => {
			const created = await createDeletionRequest(
				{ orgId: "org_abc123", scope: "user", targetId: "user_123" },
				deps,
			);
			expect(created.ok).toBe(true);
			if (!created.ok) return;

			const result = await processDeletionRequest(created.value.id, deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.status).toBe("completed");
				expect(result.value.completedAt).toBeDefined();
			}
			expect(deps.memberRepo.remove).toHaveBeenCalledWith("org_abc123", "user_123");
		});
	});

	describe("processDeletionRequest — gateway scope", () => {
		it("deletes the gateway", async () => {
			const created = await createDeletionRequest(
				{ orgId: "org_abc123", scope: "gateway", targetId: "gw_123" },
				deps,
			);
			expect(created.ok).toBe(true);
			if (!created.ok) return;

			const result = await processDeletionRequest(created.value.id, deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.status).toBe("completed");
			}
			expect(deps.gatewayRepo.delete).toHaveBeenCalledWith("gw_123");
		});
	});

	describe("processDeletionRequest — org scope", () => {
		it("deletes the entire org (cascades)", async () => {
			const created = await createDeletionRequest(
				{ orgId: "org_abc123", scope: "org", targetId: "org_abc123" },
				deps,
			);
			expect(created.ok).toBe(true);
			if (!created.ok) return;

			const result = await processDeletionRequest(created.value.id, deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.status).toBe("completed");
			}
			expect(deps.orgRepo.delete).toHaveBeenCalledWith("org_abc123");
		});
	});

	describe("processDeletionRequest — failure", () => {
		it("marks request as failed on error", async () => {
			(deps.orgRepo.delete as ReturnType<typeof vi.fn>).mockResolvedValue(
				Err(new ControlPlaneError("DB down", "INTERNAL")),
			);

			const created = await createDeletionRequest(
				{ orgId: "org_abc123", scope: "org", targetId: "org_abc123" },
				deps,
			);
			expect(created.ok).toBe(true);
			if (!created.ok) return;

			const result = await processDeletionRequest(created.value.id, deps);
			expect(result.ok).toBe(false);

			// Verify the request was marked as failed
			const status = await getDeletionRequest(created.value.id);
			expect(status.ok).toBe(true);
			if (status.ok && status.value) {
				expect(status.value.status).toBe("failed");
			}
		});

		it("returns NOT_FOUND for nonexistent request", async () => {
			const result = await processDeletionRequest("nonexistent", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});
	});

	describe("exportOrgData", () => {
		it("exports all org data", async () => {
			(deps.memberRepo.listByOrg as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok([{ orgId: "org_abc123", userId: "user_1", role: "owner", createdAt: new Date() }]),
			);
			(deps.gatewayRepo.listByOrg as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok([{ id: "gw_1", orgId: "org_abc123", name: "GW", status: "active" }]),
			);
			(deps.apiKeyRepo.listByOrg as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok([{ id: "key_1", orgId: "org_abc123", name: "Key", keyHash: "SECRET", keyPrefix: "lk_" }]),
			);

			const result = await exportOrgData("org_abc123", deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.organisation).toBeDefined();
				expect(result.value.members).toHaveLength(1);
				expect(result.value.gateways).toHaveLength(1);
				expect(result.value.apiKeys).toHaveLength(1);
				// keyHash must NOT be in the export
				const exportedKey = result.value.apiKeys[0] as Record<string, unknown>;
				expect(exportedKey.keyHash).toBeUndefined();
			}
		});

		it("returns NOT_FOUND for nonexistent org", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(Ok(null));

			const result = await exportOrgData("org_unknown", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});
	});
});
