import { Err, Ok } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import type { Gateway, Organisation } from "../entities";
import { ControlPlaneError } from "../errors";
import { CachedQuotaChecker, type QuotaCheckerDeps } from "../quota";
import type { GatewayRepository, OrgRepository, UsageRepository, UsageRow } from "../repositories";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeOrg(overrides: Partial<Organisation> = {}): Organisation {
	return {
		id: "org-1",
		name: "Test Org",
		slug: "test-org",
		plan: "free",
		createdAt: new Date(),
		updatedAt: new Date(),
		...overrides,
	};
}

function makeGateway(overrides: Partial<Gateway> = {}): Gateway {
	return {
		id: "gw-1",
		orgId: "org-1",
		name: "Test Gateway",
		status: "active",
		createdAt: new Date(),
		updatedAt: new Date(),
		...overrides,
	};
}

function makeUsageRow(overrides: Partial<UsageRow> = {}): UsageRow {
	return {
		gatewayId: "gw-1",
		orgId: "org-1",
		eventType: "push_deltas",
		count: 0,
		windowStart: new Date(),
		...overrides,
	};
}

function createMockDeps(
	overrides: {
		org?: Organisation | null;
		gateways?: Gateway[];
		usageRows?: UsageRow[];
		orgError?: boolean;
		gatewayError?: boolean;
		usageError?: boolean;
	} = {},
): QuotaCheckerDeps {
	const orgRepo: OrgRepository = {
		create: vi.fn(),
		getById: vi.fn().mockImplementation(async () => {
			if (overrides.orgError) {
				return Err(new ControlPlaneError("db error", "INTERNAL"));
			}
			return Ok(overrides.org ?? makeOrg());
		}),
		getBySlug: vi.fn(),
		update: vi.fn(),
		delete: vi.fn(),
	};

	const gatewayRepo: GatewayRepository = {
		create: vi.fn(),
		getById: vi.fn(),
		listByOrg: vi.fn().mockImplementation(async () => {
			if (overrides.gatewayError) {
				return Err(new ControlPlaneError("db error", "INTERNAL"));
			}
			return Ok(overrides.gateways ?? []);
		}),
		update: vi.fn(),
		delete: vi.fn(),
	};

	const usageRepo: UsageRepository = {
		recordAggregates: vi.fn().mockResolvedValue(Ok(undefined)),
		queryUsage: vi.fn().mockImplementation(async () => {
			if (overrides.usageError) {
				return Err(new ControlPlaneError("db error", "INTERNAL"));
			}
			return Ok(overrides.usageRows ?? []);
		}),
		queryGatewayUsage: vi.fn().mockResolvedValue(Ok([])),
	};

	return { orgRepo, gatewayRepo, usageRepo };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("CachedQuotaChecker", () => {
	describe("checkPush", () => {
		it("allows push when under quota", async () => {
			const deps = createMockDeps({
				org: makeOrg({ plan: "free" }),
				usageRows: [makeUsageRow({ count: 5000 })],
			});
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkPush("org-1", 100);
			expect(result.allowed).toBe(true);
			if (result.allowed) {
				expect(result.remaining).toBe(4900);
			}
		});

		it("rejects push when over quota", async () => {
			const deps = createMockDeps({
				org: makeOrg({ plan: "free" }),
				usageRows: [makeUsageRow({ count: 10_000 })],
			});
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkPush("org-1", 1);
			expect(result.allowed).toBe(false);
			if (!result.allowed) {
				expect(result.reason).toContain("Monthly delta quota exceeded");
				expect(result.reason).toContain("10000");
				expect(result.resetAt).toBeInstanceOf(Date);
			}
		});

		it("rejects push when push itself would exceed quota", async () => {
			const deps = createMockDeps({
				org: makeOrg({ plan: "free" }),
				usageRows: [makeUsageRow({ count: 9_990 })],
			});
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkPush("org-1", 20);
			expect(result.allowed).toBe(false);
		});

		it("allows unlimited plan", async () => {
			const deps = createMockDeps({
				org: makeOrg({ plan: "enterprise" }),
			});
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkPush("org-1", 1_000_000);
			expect(result.allowed).toBe(true);
		});

		it("tracks in-flight deltas optimistically", async () => {
			const deps = createMockDeps({
				org: makeOrg({ plan: "free" }),
				usageRows: [makeUsageRow({ count: 9_990 })],
			});
			const checker = new CachedQuotaChecker(deps);

			// First push: 5 deltas -> 9995 total
			const r1 = await checker.checkPush("org-1", 5);
			expect(r1.allowed).toBe(true);
			if (r1.allowed) {
				expect(r1.remaining).toBe(5);
			}

			// Second push: 6 deltas -> would be 10001 total
			const r2 = await checker.checkPush("org-1", 6);
			expect(r2.allowed).toBe(false);
		});

		it("fails open when org lookup fails", async () => {
			const deps = createMockDeps();
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockRejectedValue(new Error("db down"));
			const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkPush("org-1", 100);
			expect(result.allowed).toBe(true);
			expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("fail-open"));
			warnSpy.mockRestore();
		});

		it("fails open when usage query fails", async () => {
			const deps = createMockDeps({
				org: makeOrg({ plan: "free" }),
			});
			(deps.usageRepo.queryUsage as ReturnType<typeof vi.fn>).mockRejectedValue(
				new Error("timeout"),
			);
			const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkPush("org-1", 100);
			expect(result.allowed).toBe(true);
			warnSpy.mockRestore();
		});

		it("allows when org not found", async () => {
			const deps = createMockDeps({ org: null });
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkPush("org-1", 100);
			expect(result.allowed).toBe(true);
		});

		it("uses cached usage within TTL", async () => {
			const deps = createMockDeps({
				org: makeOrg({ plan: "free" }),
				usageRows: [makeUsageRow({ count: 5000 })],
			});
			const checker = new CachedQuotaChecker(deps, { cacheTtlMs: 60_000 });

			await checker.checkPush("org-1", 10);
			await checker.checkPush("org-1", 10);

			// Usage should only be queried once (second call uses cache)
			expect(deps.usageRepo.queryUsage).toHaveBeenCalledTimes(1);
		});

		it("refreshes cache after TTL expires", async () => {
			const deps = createMockDeps({
				org: makeOrg({ plan: "free" }),
				usageRows: [makeUsageRow({ count: 5000 })],
			});
			const checker = new CachedQuotaChecker(deps, { cacheTtlMs: 10 });

			await checker.checkPush("org-1", 10);

			// Wait for cache to expire
			await new Promise((resolve) => setTimeout(resolve, 20));

			await checker.checkPush("org-1", 10);

			expect(deps.usageRepo.queryUsage).toHaveBeenCalledTimes(2);
		});
	});

	describe("checkConnection", () => {
		it("allows connection when under limit", async () => {
			const deps = createMockDeps({
				org: makeOrg({ plan: "free" }),
				usageRows: [makeUsageRow({ eventType: "ws_connection", count: 3 })],
			});
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkConnection("org-1", "gw-1");
			expect(result.allowed).toBe(true);
		});

		it("rejects connection when at limit", async () => {
			const deps = createMockDeps({
				org: makeOrg({ plan: "free" }),
				usageRows: [makeUsageRow({ eventType: "ws_connection", count: 5, gatewayId: "gw-1" })],
			});
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkConnection("org-1", "gw-1");
			expect(result.allowed).toBe(false);
			if (!result.allowed) {
				expect(result.reason).toContain("Connection limit reached");
			}
		});

		it("allows unlimited plan connections", async () => {
			const deps = createMockDeps({
				org: makeOrg({ plan: "enterprise" }),
			});
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkConnection("org-1", "gw-1");
			expect(result.allowed).toBe(true);
		});

		it("fails open on error", async () => {
			const deps = createMockDeps();
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockRejectedValue(new Error("db down"));
			const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkConnection("org-1", "gw-1");
			expect(result.allowed).toBe(true);
			warnSpy.mockRestore();
		});
	});

	describe("checkGatewayCreate", () => {
		it("allows when under gateway limit", async () => {
			const deps = createMockDeps({
				org: makeOrg({ plan: "free" }),
				gateways: [],
			});
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkGatewayCreate("org-1");
			expect(result.allowed).toBe(true);
			if (result.allowed) {
				expect(result.remaining).toBe(0); // 1 max - 0 current - 1 = 0
			}
		});

		it("rejects when at gateway limit", async () => {
			const deps = createMockDeps({
				org: makeOrg({ plan: "free" }),
				gateways: [makeGateway()],
			});
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkGatewayCreate("org-1");
			expect(result.allowed).toBe(false);
			if (!result.allowed) {
				expect(result.reason).toContain("Gateway limit reached");
				expect(result.reason).toContain("1 gateways");
			}
		});

		it("excludes deleted gateways from count", async () => {
			const deps = createMockDeps({
				org: makeOrg({ plan: "free" }),
				gateways: [makeGateway({ status: "deleted" })],
			});
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkGatewayCreate("org-1");
			expect(result.allowed).toBe(true);
		});

		it("allows unlimited plan", async () => {
			const deps = createMockDeps({
				org: makeOrg({ plan: "enterprise" }),
			});
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkGatewayCreate("org-1");
			expect(result.allowed).toBe(true);
		});

		it("allows pro plan with multiple gateways", async () => {
			const deps = createMockDeps({
				org: makeOrg({ plan: "pro" }),
				gateways: Array.from({ length: 9 }, (_, i) => makeGateway({ id: `gw-${i}` })),
			});
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkGatewayCreate("org-1");
			expect(result.allowed).toBe(true);
			if (result.allowed) {
				expect(result.remaining).toBe(0); // 10 max - 9 current - 1 = 0
			}
		});

		it("fails open on error", async () => {
			const deps = createMockDeps();
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockRejectedValue(new Error("db down"));
			const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
			const checker = new CachedQuotaChecker(deps);

			const result = await checker.checkGatewayCreate("org-1");
			expect(result.allowed).toBe(true);
			warnSpy.mockRestore();
		});
	});
});
