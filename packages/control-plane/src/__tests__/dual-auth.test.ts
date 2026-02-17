import { Err, Ok } from "@lakesync/core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { authenticateRequest, type DualAuthDeps } from "../auth/dual-auth";
import { ControlPlaneError } from "../errors";

function createMockDeps(): DualAuthDeps {
	return {
		memberRepo: {
			add: vi.fn(),
			remove: vi.fn(),
			listByOrg: vi.fn(),
			getRole: vi.fn().mockResolvedValue(Ok("admin")),
			updateRole: vi.fn(),
		},
		apiKeyRepo: {
			create: vi.fn(),
			getByHash: vi.fn().mockResolvedValue(
				Ok({
					id: "key_123",
					orgId: "org_abc",
					name: "Test",
					keyHash: "hash",
					keyPrefix: "lk_ABCDEFgh",
					role: "admin",
					createdAt: new Date(),
				}),
			),
			listByOrg: vi.fn(),
			revoke: vi.fn(),
			updateLastUsed: vi.fn().mockResolvedValue(Ok(undefined)),
		},
		verifyClerkSession: vi.fn().mockResolvedValue(Ok("user_123")),
	};
}

describe("authenticateRequest", () => {
	let deps: DualAuthDeps;

	beforeEach(() => {
		deps = createMockDeps();
	});

	it("rejects missing Authorization header", async () => {
		const result = await authenticateRequest(undefined, "org_abc", deps);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("INVALID_INPUT");
		}
	});

	it("authenticates API key (lk_ prefix)", async () => {
		const result = await authenticateRequest("Bearer lk_some_key", "org_abc", deps);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.actorType).toBe("api_key");
			expect(result.value.orgId).toBe("org_abc");
			expect(result.value.role).toBe("admin");
		}
	});

	it("rejects API key from wrong org", async () => {
		const result = await authenticateRequest("Bearer lk_some_key", "org_other", deps);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("NOT_FOUND");
		}
	});

	it("authenticates Clerk session token", async () => {
		const result = await authenticateRequest("Bearer clerk_session_token", "org_abc", deps);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.actorType).toBe("user");
			expect(result.value.actorId).toBe("user_123");
			expect(result.value.orgId).toBe("org_abc");
			expect(result.value.role).toBe("admin");
		}
	});

	it("rejects Clerk session when user is not a member", async () => {
		(deps.memberRepo.getRole as ReturnType<typeof vi.fn>).mockResolvedValue(Ok(null));

		const result = await authenticateRequest("Bearer clerk_session_token", "org_abc", deps);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("NOT_FOUND");
		}
	});

	it("returns error when Clerk verification fails", async () => {
		(deps.verifyClerkSession as ReturnType<typeof vi.fn>).mockResolvedValue(
			Err(new ControlPlaneError("Invalid session", "INVALID_INPUT")),
		);

		const result = await authenticateRequest("Bearer invalid_clerk_token", "org_abc", deps);
		expect(result.ok).toBe(false);
	});

	it("returns error when Clerk verification is not configured", async () => {
		const depsNoClerk: DualAuthDeps = {
			...deps,
			verifyClerkSession: undefined,
		};

		const result = await authenticateRequest("Bearer clerk_session_token", "org_abc", depsNoClerk);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("INTERNAL");
		}
	});

	it("handles Authorization header without Bearer prefix", async () => {
		const result = await authenticateRequest("lk_some_key", "org_abc", deps);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.actorType).toBe("api_key");
		}
	});
});
