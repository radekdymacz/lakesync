import { Ok, verifyToken } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import { exchangeToken } from "../auth/token-exchange";
import type { MemberRepository } from "../repositories";

const JWT_SECRET = "test-secret-for-token-exchange";

function createMockMemberRepo(role: string | null = "admin"): MemberRepository {
	return {
		add: vi.fn().mockResolvedValue(Ok({})),
		remove: vi.fn().mockResolvedValue(Ok(undefined)),
		listByOrg: vi.fn().mockResolvedValue(Ok([])),
		getRole: vi.fn().mockResolvedValue(Ok(role)),
		updateRole: vi.fn().mockResolvedValue(Ok(undefined)),
	};
}

describe("exchangeToken", () => {
	it("exchanges session for a signed JWT", async () => {
		const memberRepo = createMockMemberRepo("admin");

		const result = await exchangeToken(
			{
				userId: "user_123",
				orgId: "org_abc",
				gatewayId: "gw_xyz",
				jwtSecret: JWT_SECRET,
			},
			memberRepo,
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.token).toBeTruthy();
			expect(result.value.expiresAt).toBeGreaterThan(Date.now() / 1000);

			// Verify the token is valid
			const verified = await verifyToken(result.value.token, JWT_SECRET);
			expect(verified.ok).toBe(true);
			if (verified.ok) {
				expect(verified.value.clientId).toBe("user_123");
				expect(verified.value.gatewayId).toBe("gw_xyz");
				expect(verified.value.role).toBe("admin");
			}
		}
	});

	it("maps owner role to admin JWT role", async () => {
		const memberRepo = createMockMemberRepo("owner");

		const result = await exchangeToken(
			{
				userId: "user_123",
				orgId: "org_abc",
				gatewayId: "gw_xyz",
				jwtSecret: JWT_SECRET,
			},
			memberRepo,
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			const verified = await verifyToken(result.value.token, JWT_SECRET);
			expect(verified.ok).toBe(true);
			if (verified.ok) {
				expect(verified.value.role).toBe("admin");
			}
		}
	});

	it("maps member role to client JWT role", async () => {
		const memberRepo = createMockMemberRepo("member");

		const result = await exchangeToken(
			{
				userId: "user_123",
				orgId: "org_abc",
				gatewayId: "gw_xyz",
				jwtSecret: JWT_SECRET,
			},
			memberRepo,
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			const verified = await verifyToken(result.value.token, JWT_SECRET);
			expect(verified.ok).toBe(true);
			if (verified.ok) {
				expect(verified.value.role).toBe("client");
			}
		}
	});

	it("maps viewer role to client JWT role", async () => {
		const memberRepo = createMockMemberRepo("viewer");

		const result = await exchangeToken(
			{
				userId: "user_123",
				orgId: "org_abc",
				gatewayId: "gw_xyz",
				jwtSecret: JWT_SECRET,
			},
			memberRepo,
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			const verified = await verifyToken(result.value.token, JWT_SECRET);
			expect(verified.ok).toBe(true);
			if (verified.ok) {
				expect(verified.value.role).toBe("client");
			}
		}
	});

	it("returns NOT_FOUND when user is not a member", async () => {
		const memberRepo = createMockMemberRepo(null);

		const result = await exchangeToken(
			{
				userId: "user_123",
				orgId: "org_abc",
				gatewayId: "gw_xyz",
				jwtSecret: JWT_SECRET,
			},
			memberRepo,
		);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("NOT_FOUND");
		}
	});

	it("respects custom TTL", async () => {
		const memberRepo = createMockMemberRepo("admin");

		const result = await exchangeToken(
			{
				userId: "user_123",
				orgId: "org_abc",
				gatewayId: "gw_xyz",
				jwtSecret: JWT_SECRET,
				ttlSeconds: 300,
			},
			memberRepo,
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			const now = Math.floor(Date.now() / 1000);
			// Should expire within ~300 seconds from now (with some slack)
			expect(result.value.expiresAt).toBeGreaterThan(now + 290);
			expect(result.value.expiresAt).toBeLessThan(now + 310);
		}
	});

	it("includes org claim in the JWT", async () => {
		const memberRepo = createMockMemberRepo("admin");

		const result = await exchangeToken(
			{
				userId: "user_123",
				orgId: "org_abc",
				gatewayId: "gw_xyz",
				jwtSecret: JWT_SECRET,
			},
			memberRepo,
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			const verified = await verifyToken(result.value.token, JWT_SECRET);
			expect(verified.ok).toBe(true);
			if (verified.ok) {
				expect(verified.value.customClaims.org).toBe("org_abc");
			}
		}
	});
});
