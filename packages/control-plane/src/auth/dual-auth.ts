import { Err, Ok, type Result } from "@lakesync/core";
import type { ApiKeyRole, OrgRole } from "../entities";
import { ControlPlaneError } from "../errors";
import type { ApiKeyRepository, MemberRepository } from "../repositories";
import { authenticateApiKey } from "./api-key-auth";

/** Unified auth context from either Clerk session or API key */
export interface AuthContext {
	/** User ID (Clerk user ID or API key ID) */
	readonly actorId: string;
	/** Actor type */
	readonly actorType: "user" | "api_key";
	/** Organisation ID */
	readonly orgId: string;
	/** Gateway ID (if scoped to a specific gateway) */
	readonly gatewayId?: string;
	/** Effective role */
	readonly role: OrgRole | ApiKeyRole;
}

/** Dependencies for the dual auth middleware */
export interface DualAuthDeps {
	readonly memberRepo: MemberRepository;
	readonly apiKeyRepo: ApiKeyRepository;
	/** Function to verify a Clerk session token and return the user ID */
	readonly verifyClerkSession?: (token: string) => Promise<Result<string, ControlPlaneError>>;
}

/**
 * Authenticate a request using either a Clerk session token or an API key.
 *
 * - If the Authorization header contains `lk_...`, authenticate as API key
 * - Otherwise, treat as a Clerk Bearer token
 *
 * @param authHeader - The Authorization header value
 * @param orgId - The target organisation ID (from URL params)
 * @param deps - Auth dependencies
 */
export async function authenticateRequest(
	authHeader: string | undefined,
	orgId: string,
	deps: DualAuthDeps,
): Promise<Result<AuthContext, ControlPlaneError>> {
	if (!authHeader) {
		return Err(new ControlPlaneError("Missing Authorization header", "INVALID_INPUT"));
	}

	const token = authHeader.startsWith("Bearer ")
		? authHeader.slice(7)
		: authHeader;

	// API key authentication
	if (token.startsWith("lk_")) {
		const keyResult = await authenticateApiKey(token, deps.apiKeyRepo);
		if (!keyResult.ok) return keyResult;

		const ctx = keyResult.value;

		// Verify the API key belongs to the requested org
		if (ctx.orgId !== orgId) {
			return Err(new ControlPlaneError("API key does not belong to this organisation", "NOT_FOUND"));
		}

		return Ok({
			actorId: ctx.apiKeyId,
			actorType: "api_key" as const,
			orgId: ctx.orgId,
			gatewayId: ctx.gatewayId,
			role: ctx.role,
		});
	}

	// Clerk session authentication
	if (!deps.verifyClerkSession) {
		return Err(
			new ControlPlaneError("Clerk session verification not configured", "INTERNAL"),
		);
	}

	const userIdResult = await deps.verifyClerkSession(token);
	if (!userIdResult.ok) return userIdResult;

	const userId = userIdResult.value;

	// Look up org membership
	const roleResult = await deps.memberRepo.getRole(orgId, userId);
	if (!roleResult.ok) return roleResult;

	const role = roleResult.value;
	if (role === null) {
		return Err(
			new ControlPlaneError(
				`User is not a member of organisation "${orgId}"`,
				"NOT_FOUND",
			),
		);
	}

	return Ok({
		actorId: userId,
		actorType: "user" as const,
		orgId,
		role,
	});
}
