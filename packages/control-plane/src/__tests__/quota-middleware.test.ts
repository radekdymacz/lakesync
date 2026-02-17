import { describe, expect, it, vi } from "vitest";
import { enforceQuota, type QuotaChecker, type QuotaResult } from "../quota";

function createMockChecker(overrides: Partial<QuotaChecker> = {}): QuotaChecker {
	return {
		checkPush: vi.fn().mockResolvedValue({ allowed: true, remaining: 100 }),
		checkConnection: vi.fn().mockResolvedValue({ allowed: true, remaining: 10 }),
		checkGatewayCreate: vi.fn().mockResolvedValue({ allowed: true, remaining: 5 }),
		...overrides,
	};
}

describe("enforceQuota", () => {
	it("returns allowed with X-Quota-Remaining for push", async () => {
		const checker = createMockChecker();
		const result = await enforceQuota(checker, {
			orgId: "org-1",
			gatewayId: "gw-1",
			type: "push",
			deltaCount: 50,
		});

		expect(result.allowed).toBe(true);
		expect(result.status).toBe(200);
		expect(result.headers["X-Quota-Remaining"]).toBe("100");
		expect(checker.checkPush).toHaveBeenCalledWith("org-1", 50);
	});

	it("returns 429 with Retry-After when push quota exceeded", async () => {
		const resetAt = new Date(Date.now() + 3600_000); // 1 hour from now
		const checker = createMockChecker({
			checkPush: vi.fn().mockResolvedValue({
				allowed: false,
				reason: "Monthly delta quota exceeded",
				resetAt,
			} satisfies QuotaResult),
		});

		const result = await enforceQuota(checker, {
			orgId: "org-1",
			gatewayId: "gw-1",
			type: "push",
			deltaCount: 100,
		});

		expect(result.allowed).toBe(false);
		expect(result.status).toBe(429);
		expect(result.message).toContain("Monthly delta quota exceeded");
		expect(result.headers["Retry-After"]).toBeDefined();
		const retryAfter = Number(result.headers["Retry-After"]);
		expect(retryAfter).toBeGreaterThan(0);
		expect(retryAfter).toBeLessThanOrEqual(3600);
	});

	it("calls checkConnection for connection type", async () => {
		const checker = createMockChecker();
		const result = await enforceQuota(checker, {
			orgId: "org-1",
			gatewayId: "gw-1",
			type: "connection",
		});

		expect(result.allowed).toBe(true);
		expect(checker.checkConnection).toHaveBeenCalledWith("org-1", "gw-1");
	});

	it("calls checkGatewayCreate for gateway_create type", async () => {
		const checker = createMockChecker();
		const result = await enforceQuota(checker, {
			orgId: "org-1",
			gatewayId: "gw-1",
			type: "gateway_create",
		});

		expect(result.allowed).toBe(true);
		expect(checker.checkGatewayCreate).toHaveBeenCalledWith("org-1");
	});

	it("returns 429 without Retry-After when resetAt is not set", async () => {
		const checker = createMockChecker({
			checkConnection: vi.fn().mockResolvedValue({
				allowed: false,
				reason: "Connection limit reached",
			} satisfies QuotaResult),
		});

		const result = await enforceQuota(checker, {
			orgId: "org-1",
			gatewayId: "gw-1",
			type: "connection",
		});

		expect(result.allowed).toBe(false);
		expect(result.status).toBe(429);
		expect(result.headers["Retry-After"]).toBeUndefined();
	});

	it("defaults deltaCount to 0 when not provided for push", async () => {
		const checker = createMockChecker();
		await enforceQuota(checker, {
			orgId: "org-1",
			gatewayId: "gw-1",
			type: "push",
		});

		expect(checker.checkPush).toHaveBeenCalledWith("org-1", 0);
	});
});
