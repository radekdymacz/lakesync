// ---------------------------------------------------------------------------
// Auth Middleware — JWT validation for HTTP requests
// ---------------------------------------------------------------------------

import type { IncomingMessage } from "node:http";
import type { AuthClaims } from "./auth";
import { verifyToken } from "./auth";

/** Result of authentication: either authenticated claims or an error. */
export type AuthResult =
	| { authenticated: true; claims: AuthClaims }
	| { authenticated: false; status: number; message: string };

/** Set of actions that require admin role. */
const ADMIN_ACTIONS = new Set([
	"flush",
	"schema",
	"sync-rules",
	"register-connector",
	"unregister-connector",
	"list-connectors",
	"metrics",
]);

/**
 * Extract the Bearer token from an Authorization header.
 * Returns the raw token string, or null if missing/malformed.
 */
export function extractBearerToken(req: IncomingMessage): string | null {
	const header = req.headers.authorization;
	if (!header) return null;
	const match = header.match(/^Bearer\s+(\S+)$/);
	return match?.[1] ?? null;
}

/**
 * Authenticate an HTTP request.
 *
 * When `jwtSecret` is undefined, auth is disabled and all requests pass.
 * Otherwise validates the Bearer token, checks gateway ID, and enforces
 * admin role for admin actions.
 */
export async function authenticateRequest(
	req: IncomingMessage,
	routeGatewayId: string,
	routeAction: string,
	jwtSecret: string | [string, string] | undefined,
): Promise<AuthResult> {
	if (!jwtSecret) {
		return { authenticated: true, claims: undefined as unknown as AuthClaims };
	}

	const token = extractBearerToken(req);
	if (!token) {
		return { authenticated: false, status: 401, message: "Missing Bearer token" };
	}

	const authResult = await verifyToken(token, jwtSecret);
	if (!authResult.ok) {
		return { authenticated: false, status: 401, message: authResult.error.message };
	}

	const claims = authResult.value;

	// Verify JWT gateway ID matches the route
	if (claims.gatewayId !== routeGatewayId) {
		return {
			authenticated: false,
			status: 403,
			message: "Gateway ID mismatch: JWT authorises a different gateway",
		};
	}

	// Admin route protection
	if (ADMIN_ACTIONS.has(routeAction) && claims.role !== "admin") {
		return { authenticated: false, status: 403, message: "Admin role required" };
	}

	return { authenticated: true, claims };
}

/** Check whether auth is disabled (no jwtSecret configured). */
export function isAuthDisabled(jwtSecret: string | [string, string] | undefined): boolean {
	return jwtSecret === undefined;
}
