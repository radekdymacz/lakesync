import { Err, Ok } from "@lakesync/core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { GatewayServiceDeps } from "../api/gateway-service";
import {
	checkGatewayStatus,
	createGateway,
	deleteGateway,
	listGateways,
	reactivateOrgGateways,
	suspendOrgGateways,
	updateGateway,
} from "../api/gateway-service";
import type { Gateway, Organisation } from "../entities";
import { ControlPlaneError } from "../errors";

function mockOrg(plan = "free"): Organisation {
	return {
		id: "org_abc",
		name: "Test Org",
		slug: "test-org",
		plan: plan as Organisation["plan"],
		createdAt: new Date(),
		updatedAt: new Date(),
	};
}

function mockGw(overrides: Partial<Gateway> = {}): Gateway {
	return {
		id: "gw_123",
		orgId: "org_abc",
		name: "Test GW",
		status: "active",
		createdAt: new Date(),
		updatedAt: new Date(),
		...overrides,
	};
}

function createMockDeps(): GatewayServiceDeps {
	return {
		gatewayRepo: {
			create: vi.fn().mockResolvedValue(Ok(mockGw())),
			getById: vi.fn().mockResolvedValue(Ok(mockGw())),
			listByOrg: vi.fn().mockResolvedValue(Ok([])),
			update: vi.fn().mockImplementation((_id, input) => Promise.resolve(Ok(mockGw(input)))),
			delete: vi.fn().mockResolvedValue(Ok(undefined)),
		},
		orgRepo: {
			create: vi.fn(),
			getById: vi.fn().mockResolvedValue(Ok(mockOrg())),
			getBySlug: vi.fn(),
			update: vi.fn(),
			delete: vi.fn(),
		},
	};
}

describe("Gateway Service", () => {
	let deps: GatewayServiceDeps;

	beforeEach(() => {
		deps = createMockDeps();
	});

	describe("createGateway", () => {
		it("creates a gateway when under quota", async () => {
			const result = await createGateway("org_abc", { name: "My GW", region: "us" }, deps);
			expect(result.ok).toBe(true);
			expect(deps.gatewayRepo.create).toHaveBeenCalledOnce();
		});

		it("returns QUOTA_EXCEEDED when at gateway limit (free plan = 1)", async () => {
			(deps.gatewayRepo.listByOrg as ReturnType<typeof vi.fn>).mockResolvedValue(Ok([mockGw()]));

			const result = await createGateway("org_abc", { name: "Second GW" }, deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("QUOTA_EXCEEDED");
				expect(result.error.message).toContain("Gateway limit reached");
			}
		});

		it("allows creation for enterprise plan (unlimited)", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(mockOrg("enterprise")),
			);
			(deps.gatewayRepo.listByOrg as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(Array(100).fill(mockGw())),
			);

			const result = await createGateway("org_abc", { name: "Another GW" }, deps);
			expect(result.ok).toBe(true);
		});

		it("allows pro plan up to 10 gateways", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(Ok(mockOrg("pro")));
			(deps.gatewayRepo.listByOrg as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(Array(9).fill(mockGw())),
			);

			const result = await createGateway("org_abc", { name: "GW 10" }, deps);
			expect(result.ok).toBe(true);
		});

		it("returns NOT_FOUND for nonexistent org", async () => {
			(deps.orgRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(Ok(null));

			const result = await createGateway("org_unknown", { name: "GW" }, deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});

		it("excludes deleted gateways from quota count", async () => {
			(deps.gatewayRepo.listByOrg as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok([mockGw({ status: "deleted" })]),
			);

			const result = await createGateway("org_abc", { name: "New GW" }, deps);
			expect(result.ok).toBe(true);
		});
	});

	describe("listGateways", () => {
		it("delegates to repository", async () => {
			const gateways = [mockGw({ id: "gw_1" }), mockGw({ id: "gw_2" })];
			(deps.gatewayRepo.listByOrg as ReturnType<typeof vi.fn>).mockResolvedValue(Ok(gateways));

			const result = await listGateways("org_abc", deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(2);
			}
		});
	});

	describe("updateGateway", () => {
		it("updates gateway name", async () => {
			const result = await updateGateway("gw_123", { name: "Renamed" }, deps);
			expect(result.ok).toBe(true);
		});
	});

	describe("deleteGateway", () => {
		it("soft-deletes by setting status to deleted", async () => {
			const result = await deleteGateway("gw_123", deps);
			expect(result.ok).toBe(true);
			expect(deps.gatewayRepo.update).toHaveBeenCalledWith("gw_123", {
				status: "deleted",
			});
		});
	});

	describe("suspendOrgGateways", () => {
		it("suspends all active gateways", async () => {
			(deps.gatewayRepo.listByOrg as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok([
					mockGw({ id: "gw_1", status: "active" }),
					mockGw({ id: "gw_2", status: "active" }),
					mockGw({ id: "gw_3", status: "deleted" }),
				]),
			);

			const result = await suspendOrgGateways("org_abc", deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBe(2); // only active ones
			}
		});

		it("returns 0 when no active gateways", async () => {
			(deps.gatewayRepo.listByOrg as ReturnType<typeof vi.fn>).mockResolvedValue(Ok([]));

			const result = await suspendOrgGateways("org_abc", deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBe(0);
			}
		});
	});

	describe("reactivateOrgGateways", () => {
		it("reactivates all suspended gateways", async () => {
			(deps.gatewayRepo.listByOrg as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok([mockGw({ id: "gw_1", status: "suspended" }), mockGw({ id: "gw_2", status: "active" })]),
			);

			const result = await reactivateOrgGateways("org_abc", deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBe(1); // only suspended ones
			}
		});
	});

	describe("checkGatewayStatus", () => {
		it("returns gateway when active", async () => {
			const result = await checkGatewayStatus("gw_123", deps);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value.status).toBe("active");
			}
		});

		it("returns QUOTA_EXCEEDED for suspended gateway", async () => {
			(deps.gatewayRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(mockGw({ status: "suspended" })),
			);

			const result = await checkGatewayStatus("gw_123", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("QUOTA_EXCEEDED");
				expect(result.error.message).toContain("suspended");
			}
		});

		it("returns NOT_FOUND for deleted gateway", async () => {
			(deps.gatewayRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok(mockGw({ status: "deleted" })),
			);

			const result = await checkGatewayStatus("gw_123", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});

		it("returns NOT_FOUND for nonexistent gateway", async () => {
			(deps.gatewayRepo.getById as ReturnType<typeof vi.fn>).mockResolvedValue(Ok(null));

			const result = await checkGatewayStatus("gw_unknown", deps);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("NOT_FOUND");
			}
		});
	});
});
