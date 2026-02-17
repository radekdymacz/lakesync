import { describe, expect, it } from "vitest";
import type { OrgRole } from "../entities";
import {
	ALL_ACTIONS,
	checkPermission,
	getPermissions,
	orgRoleToJwtRole,
	type PermissionAction,
	requirePermission,
} from "../rbac/permissions";

describe("RBAC permissions", () => {
	// ── Owner: full access ────────────────────────────────────────

	describe("owner role", () => {
		it("has all permissions", () => {
			for (const action of ALL_ACTIONS) {
				expect(checkPermission("owner", action)).toBe(true);
			}
		});

		it("can delete org", () => {
			expect(checkPermission("owner", "org.delete")).toBe(true);
		});

		it("can manage billing", () => {
			expect(checkPermission("owner", "billing.manage")).toBe(true);
		});
	});

	// ── Admin: manage everything except org delete and billing ─────

	describe("admin role", () => {
		it("cannot delete org", () => {
			expect(checkPermission("admin", "org.delete")).toBe(false);
		});

		it("cannot manage billing", () => {
			expect(checkPermission("admin", "billing.manage")).toBe(false);
		});

		it("can read org", () => {
			expect(checkPermission("admin", "org.read")).toBe(true);
		});

		it("can update org", () => {
			expect(checkPermission("admin", "org.update")).toBe(true);
		});

		it("can manage gateways", () => {
			expect(checkPermission("admin", "gateway.create")).toBe(true);
			expect(checkPermission("admin", "gateway.read")).toBe(true);
			expect(checkPermission("admin", "gateway.update")).toBe(true);
			expect(checkPermission("admin", "gateway.delete")).toBe(true);
			expect(checkPermission("admin", "gateway.suspend")).toBe(true);
		});

		it("can manage API keys", () => {
			expect(checkPermission("admin", "api_key.create")).toBe(true);
			expect(checkPermission("admin", "api_key.read")).toBe(true);
			expect(checkPermission("admin", "api_key.revoke")).toBe(true);
			expect(checkPermission("admin", "api_key.rotate")).toBe(true);
		});

		it("can manage members", () => {
			expect(checkPermission("admin", "member.add")).toBe(true);
			expect(checkPermission("admin", "member.read")).toBe(true);
			expect(checkPermission("admin", "member.remove")).toBe(true);
			expect(checkPermission("admin", "member.role_change")).toBe(true);
		});

		it("can push and pull data", () => {
			expect(checkPermission("admin", "sync.push")).toBe(true);
			expect(checkPermission("admin", "sync.pull")).toBe(true);
		});

		it("can perform admin operations", () => {
			expect(checkPermission("admin", "admin.flush")).toBe(true);
			expect(checkPermission("admin", "admin.schema")).toBe(true);
			expect(checkPermission("admin", "admin.sync_rules")).toBe(true);
			expect(checkPermission("admin", "admin.connectors")).toBe(true);
			expect(checkPermission("admin", "admin.metrics")).toBe(true);
		});

		it("can read billing and audit log", () => {
			expect(checkPermission("admin", "billing.read")).toBe(true);
			expect(checkPermission("admin", "audit.read")).toBe(true);
		});
	});

	// ── Member: push/pull + read ──────────────────────────────────

	describe("member role", () => {
		it("can push and pull data", () => {
			expect(checkPermission("member", "sync.push")).toBe(true);
			expect(checkPermission("member", "sync.pull")).toBe(true);
		});

		it("can read resources", () => {
			expect(checkPermission("member", "org.read")).toBe(true);
			expect(checkPermission("member", "gateway.read")).toBe(true);
			expect(checkPermission("member", "api_key.read")).toBe(true);
			expect(checkPermission("member", "member.read")).toBe(true);
			expect(checkPermission("member", "billing.read")).toBe(true);
		});

		it("cannot create or modify gateways", () => {
			expect(checkPermission("member", "gateway.create")).toBe(false);
			expect(checkPermission("member", "gateway.update")).toBe(false);
			expect(checkPermission("member", "gateway.delete")).toBe(false);
			expect(checkPermission("member", "gateway.suspend")).toBe(false);
		});

		it("cannot manage API keys", () => {
			expect(checkPermission("member", "api_key.create")).toBe(false);
			expect(checkPermission("member", "api_key.revoke")).toBe(false);
			expect(checkPermission("member", "api_key.rotate")).toBe(false);
		});

		it("cannot manage members", () => {
			expect(checkPermission("member", "member.add")).toBe(false);
			expect(checkPermission("member", "member.remove")).toBe(false);
			expect(checkPermission("member", "member.role_change")).toBe(false);
		});

		it("cannot perform admin operations", () => {
			expect(checkPermission("member", "admin.flush")).toBe(false);
			expect(checkPermission("member", "admin.schema")).toBe(false);
			expect(checkPermission("member", "admin.sync_rules")).toBe(false);
			expect(checkPermission("member", "admin.connectors")).toBe(false);
			expect(checkPermission("member", "admin.metrics")).toBe(false);
		});

		it("cannot delete org or manage billing", () => {
			expect(checkPermission("member", "org.delete")).toBe(false);
			expect(checkPermission("member", "org.update")).toBe(false);
			expect(checkPermission("member", "billing.manage")).toBe(false);
		});

		it("cannot read audit log", () => {
			expect(checkPermission("member", "audit.read")).toBe(false);
		});
	});

	// ── Viewer: read-only ─────────────────────────────────────────

	describe("viewer role", () => {
		it("can pull data but not push", () => {
			expect(checkPermission("viewer", "sync.pull")).toBe(true);
			expect(checkPermission("viewer", "sync.push")).toBe(false);
		});

		it("can read resources", () => {
			expect(checkPermission("viewer", "org.read")).toBe(true);
			expect(checkPermission("viewer", "gateway.read")).toBe(true);
			expect(checkPermission("viewer", "api_key.read")).toBe(true);
			expect(checkPermission("viewer", "member.read")).toBe(true);
			expect(checkPermission("viewer", "billing.read")).toBe(true);
		});

		it("can read audit log", () => {
			expect(checkPermission("viewer", "audit.read")).toBe(true);
		});

		it("cannot modify anything", () => {
			const writeActions: PermissionAction[] = [
				"org.update",
				"org.delete",
				"gateway.create",
				"gateway.update",
				"gateway.delete",
				"gateway.suspend",
				"api_key.create",
				"api_key.revoke",
				"api_key.rotate",
				"member.add",
				"member.remove",
				"member.role_change",
				"admin.flush",
				"admin.schema",
				"admin.sync_rules",
				"admin.connectors",
				"admin.metrics",
				"billing.manage",
			];

			for (const action of writeActions) {
				expect(checkPermission("viewer", action)).toBe(false);
			}
		});
	});

	// ── requirePermission ─────────────────────────────────────────

	describe("requirePermission", () => {
		it("returns Ok for allowed action", () => {
			const result = requirePermission("owner", "org.delete");
			expect(result.ok).toBe(true);
		});

		it("returns Err for denied action", () => {
			const result = requirePermission("viewer", "sync.push");
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("INVALID_INPUT");
				expect(result.error.message).toContain("viewer");
				expect(result.error.message).toContain("sync.push");
			}
		});

		it("includes resource in error message when provided", () => {
			const result = requirePermission("member", "gateway.create", "gw_abc");
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.message).toContain("gw_abc");
			}
		});
	});

	// ── orgRoleToJwtRole ──────────────────────────────────────────

	describe("orgRoleToJwtRole", () => {
		it("maps owner to admin JWT role", () => {
			expect(orgRoleToJwtRole("owner")).toBe("admin");
		});

		it("maps admin to admin JWT role", () => {
			expect(orgRoleToJwtRole("admin")).toBe("admin");
		});

		it("maps member to client JWT role", () => {
			expect(orgRoleToJwtRole("member")).toBe("client");
		});

		it("maps viewer to client JWT role", () => {
			expect(orgRoleToJwtRole("viewer")).toBe("client");
		});
	});

	// ── getPermissions ────────────────────────────────────────────

	describe("getPermissions", () => {
		it("returns all actions for owner", () => {
			const perms = getPermissions("owner");
			expect(perms.size).toBe(ALL_ACTIONS.length);
		});

		it("returns fewer permissions for viewer than owner", () => {
			const ownerPerms = getPermissions("owner");
			const viewerPerms = getPermissions("viewer");
			expect(viewerPerms.size).toBeLessThan(ownerPerms.size);
		});

		it("admin has more permissions than member", () => {
			const adminPerms = getPermissions("admin");
			const memberPerms = getPermissions("member");
			expect(adminPerms.size).toBeGreaterThan(memberPerms.size);
		});
	});

	// ── Role hierarchy consistency ────────────────────────────────

	describe("role hierarchy", () => {
		it("owner is a strict superset of admin", () => {
			const ownerPerms = getPermissions("owner");
			const adminPerms = getPermissions("admin");
			for (const action of adminPerms) {
				expect(ownerPerms.has(action)).toBe(true);
			}
			expect(ownerPerms.size).toBeGreaterThan(adminPerms.size);
		});

		it("admin is a strict superset of member", () => {
			const adminPerms = getPermissions("admin");
			const memberPerms = getPermissions("member");
			for (const action of memberPerms) {
				expect(adminPerms.has(action)).toBe(true);
			}
			expect(adminPerms.size).toBeGreaterThan(memberPerms.size);
		});

		it("every role has at least org.read", () => {
			const roles: OrgRole[] = ["owner", "admin", "member", "viewer"];
			for (const role of roles) {
				expect(checkPermission(role, "org.read")).toBe(true);
			}
		});
	});
});
