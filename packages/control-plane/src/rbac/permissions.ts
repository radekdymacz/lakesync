import { Err, Ok, type Result } from "@lakesync/core";
import type { OrgRole } from "../entities";
import { ControlPlaneError } from "../errors";

/**
 * Actions that can be checked against the permission matrix.
 *
 * Grouped by resource:
 * - org.*: organisation-level operations
 * - gateway.*: gateway lifecycle
 * - api_key.*: API key management
 * - member.*: organisation membership
 * - sync.*: data plane operations (push/pull)
 * - admin.*: gateway admin operations (flush, schema, connectors)
 * - billing.*: billing and plan management
 * - audit.*: audit log access
 */
export type PermissionAction =
	| "org.read"
	| "org.update"
	| "org.delete"
	| "gateway.create"
	| "gateway.read"
	| "gateway.update"
	| "gateway.delete"
	| "gateway.suspend"
	| "api_key.create"
	| "api_key.read"
	| "api_key.revoke"
	| "api_key.rotate"
	| "member.add"
	| "member.read"
	| "member.remove"
	| "member.role_change"
	| "sync.push"
	| "sync.pull"
	| "admin.flush"
	| "admin.schema"
	| "admin.sync_rules"
	| "admin.connectors"
	| "admin.metrics"
	| "billing.read"
	| "billing.manage"
	| "audit.read";

/**
 * Permission matrix — defines which roles can perform which actions.
 *
 * Roles (highest to lowest):
 * - **owner**: full control including org deletion and billing
 * - **admin**: manage gateways, keys, members, admin operations (cannot delete org)
 * - **member**: push/pull data and read resources
 * - **viewer**: read-only access
 */
const PERMISSION_MATRIX: Readonly<Record<OrgRole, ReadonlySet<PermissionAction>>> = {
	owner: new Set<PermissionAction>([
		"org.read",
		"org.update",
		"org.delete",
		"gateway.create",
		"gateway.read",
		"gateway.update",
		"gateway.delete",
		"gateway.suspend",
		"api_key.create",
		"api_key.read",
		"api_key.revoke",
		"api_key.rotate",
		"member.add",
		"member.read",
		"member.remove",
		"member.role_change",
		"sync.push",
		"sync.pull",
		"admin.flush",
		"admin.schema",
		"admin.sync_rules",
		"admin.connectors",
		"admin.metrics",
		"billing.read",
		"billing.manage",
		"audit.read",
	]),

	admin: new Set<PermissionAction>([
		"org.read",
		"org.update",
		// admin cannot delete org
		"gateway.create",
		"gateway.read",
		"gateway.update",
		"gateway.delete",
		"gateway.suspend",
		"api_key.create",
		"api_key.read",
		"api_key.revoke",
		"api_key.rotate",
		"member.add",
		"member.read",
		"member.remove",
		"member.role_change",
		"sync.push",
		"sync.pull",
		"admin.flush",
		"admin.schema",
		"admin.sync_rules",
		"admin.connectors",
		"admin.metrics",
		"billing.read",
		// admin cannot manage billing
		"audit.read",
	]),

	member: new Set<PermissionAction>([
		"org.read",
		"gateway.read",
		"api_key.read",
		"member.read",
		"sync.push",
		"sync.pull",
		"billing.read",
	]),

	viewer: new Set<PermissionAction>([
		"org.read",
		"gateway.read",
		"api_key.read",
		"member.read",
		"sync.pull",
		"billing.read",
		"audit.read",
	]),
};

/**
 * Check whether a role has permission to perform an action.
 *
 * Pure function — no side effects, no database access.
 */
export function checkPermission(role: OrgRole, action: PermissionAction): boolean {
	const permissions = PERMISSION_MATRIX[role];
	return permissions.has(action);
}

/**
 * Require permission, returning a Result.
 *
 * Returns Ok(void) if permitted, Err(ControlPlaneError) with FORBIDDEN code if not.
 */
export function requirePermission(
	role: OrgRole,
	action: PermissionAction,
	resource?: string,
): Result<void, ControlPlaneError> {
	if (checkPermission(role, action)) {
		return Ok(undefined);
	}
	const msg = resource
		? `Role "${role}" does not have permission to ${action} on ${resource}`
		: `Role "${role}" does not have permission to ${action}`;
	return Err(new ControlPlaneError(msg, "INVALID_INPUT"));
}

/**
 * Map an OrgRole to the JWT role used in gateway tokens.
 *
 * - owner, admin → "admin" JWT role (full gateway access)
 * - member → "client" JWT role (push + pull)
 * - viewer → "client" JWT role (pull only — further restricted by gateway)
 */
export function orgRoleToJwtRole(role: OrgRole): "admin" | "client" {
	if (role === "owner" || role === "admin") return "admin";
	return "client";
}

/**
 * Get all permissions for a role.
 *
 * Useful for displaying in a UI or debugging.
 */
export function getPermissions(role: OrgRole): ReadonlySet<PermissionAction> {
	return PERMISSION_MATRIX[role];
}

/**
 * All defined permission actions.
 *
 * Useful for iterating in tests or building UI permission grids.
 */
export const ALL_ACTIONS: readonly PermissionAction[] = [
	"org.read",
	"org.update",
	"org.delete",
	"gateway.create",
	"gateway.read",
	"gateway.update",
	"gateway.delete",
	"gateway.suspend",
	"api_key.create",
	"api_key.read",
	"api_key.revoke",
	"api_key.rotate",
	"member.add",
	"member.read",
	"member.remove",
	"member.role_change",
	"sync.push",
	"sync.pull",
	"admin.flush",
	"admin.schema",
	"admin.sync_rules",
	"admin.connectors",
	"admin.metrics",
	"billing.read",
	"billing.manage",
	"audit.read",
];
