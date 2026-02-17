import { Err, Ok, type Result, signToken, type TokenPayload } from "@lakesync/core";
import { ControlPlaneError } from "../errors";
import type { MemberRepository } from "../repositories";

/** Input for exchanging a Clerk session for a LakeSync JWT */
export interface TokenExchangeInput {
	/** Clerk user ID (from verified session) */
	readonly userId: string;
	/** Target organisation ID */
	readonly orgId: string;
	/** Target gateway ID */
	readonly gatewayId: string;
	/** JWT signing secret */
	readonly jwtSecret: string;
	/** Optional token TTL in seconds (default: 3600) */
	readonly ttlSeconds?: number;
}

/** Result of a successful token exchange */
export interface TokenExchangeResult {
	readonly token: string;
	readonly expiresAt: number;
}

/**
 * Exchange a verified Clerk session for a LakeSync gateway JWT.
 *
 * Looks up the user's org membership and role, then signs a JWT
 * with the appropriate claims for gateway access.
 */
export async function exchangeToken(
	input: TokenExchangeInput,
	memberRepo: MemberRepository,
): Promise<Result<TokenExchangeResult, ControlPlaneError>> {
	// Verify the user is a member of the org
	const roleResult = await memberRepo.getRole(input.orgId, input.userId);
	if (!roleResult.ok) return roleResult;

	const role = roleResult.value;
	if (role === null) {
		return Err(
			new ControlPlaneError(
				`User "${input.userId}" is not a member of organisation "${input.orgId}"`,
				"NOT_FOUND",
			),
		);
	}

	// Map org role to gateway JWT role
	const jwtRole = mapOrgRoleToJwtRole(role);
	const ttl = input.ttlSeconds ?? 3600;
	const exp = Math.floor(Date.now() / 1000) + ttl;

	const payload: TokenPayload = {
		sub: input.userId,
		gw: input.gatewayId,
		role: jwtRole,
		exp,
		org: input.orgId,
	};

	const token = await signToken(payload, input.jwtSecret);

	return Ok({
		token,
		expiresAt: exp,
	});
}

/**
 * Map an organisation role to a gateway JWT role.
 *
 * - owner, admin → "admin" (full gateway access)
 * - member, viewer → "client" (sync-only access)
 */
function mapOrgRoleToJwtRole(orgRole: string): "admin" | "client" {
	if (orgRole === "owner" || orgRole === "admin") {
		return "admin";
	}
	return "client";
}
