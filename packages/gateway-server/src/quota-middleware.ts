// ---------------------------------------------------------------------------
// Quota Enforcement Middleware — checks usage against plan limits
// ---------------------------------------------------------------------------

import { API_ERROR_CODES } from "@lakesync/core";
import type { Middleware } from "./middleware";
import { sendError } from "./pipeline";

/**
 * Quota checker interface accepted by the middleware.
 *
 * Matches the `QuotaChecker` interface from `@lakesync/control-plane` so
 * the gateway-server does not depend on the control-plane package directly.
 */
export interface QuotaEnforcer {
	checkPush(orgId: string, deltaCount: number): Promise<QuotaEnforcerResult>;
	checkConnection(orgId: string, gatewayId: string): Promise<QuotaEnforcerResult>;
}

/** Minimal quota check result. */
export type QuotaEnforcerResult =
	| { readonly allowed: true; readonly remaining: number }
	| { readonly allowed: false; readonly reason: string; readonly resetAt?: Date };

/** Resolve the orgId from the request context (JWT claims or a lookup). */
export type OrgIdResolver = (
	gatewayId: string,
	claims?: Record<string, unknown>,
) => Promise<string | null> | string | null;

/**
 * Quota enforcement middleware.
 *
 * Sits after auth and route-matching in the pipeline. For push and ws
 * actions, checks the resolved org's quota before allowing the request
 * through. Returns 429 when quota is exceeded.
 *
 * When `orgIdResolver` returns null (org not found), the request is
 * allowed through (fail-open) — the control plane may not be configured.
 */
export function quotaMiddleware(enforcer: QuotaEnforcer, resolveOrgId: OrgIdResolver): Middleware {
	return async (ctx, next) => {
		const route = ctx.route;
		if (!route) {
			await next();
			return;
		}

		const { action, gatewayId } = route;

		// Only enforce quota on push and WebSocket connection actions
		if (action !== "push" && action !== "ws") {
			await next();
			return;
		}

		let orgId: string | null;
		try {
			orgId = await resolveOrgId(gatewayId, ctx.auth as Record<string, unknown> | undefined);
		} catch {
			// Fail-open if org resolution fails
			console.warn(
				`[lakesync] Org ID resolution failed for gateway ${gatewayId}, allowing request (fail-open)`,
			);
			await next();
			return;
		}

		if (!orgId) {
			// No org mapping — allow through (control plane may not be configured)
			await next();
			return;
		}

		let result: QuotaEnforcerResult;
		try {
			if (action === "ws") {
				result = await enforcer.checkConnection(orgId, gatewayId);
			} else {
				// For push, we don't know the delta count yet at this stage,
				// so we do a preliminary check with count 0 (checks if already over limit)
				result = await enforcer.checkPush(orgId, 0);
			}
		} catch {
			// Fail-open if quota check fails
			console.warn(`[lakesync] Quota check failed for org ${orgId}, allowing request (fail-open)`);
			await next();
			return;
		}

		if (!result.allowed) {
			const headers: Record<string, string> = {
				...ctx.corsHeaders,
				"X-Quota-Remaining": "0",
			};
			if (result.resetAt) {
				const retryAfterSec = Math.max(
					1,
					Math.ceil((result.resetAt.getTime() - Date.now()) / 1000),
				);
				headers["Retry-After"] = String(retryAfterSec);
			}
			sendError(ctx.res, result.reason, 429, headers, {
				code: API_ERROR_CODES.RATE_LIMITED,
				requestId: ctx.requestId,
			});
			return;
		}

		// Set quota remaining header on the response
		ctx.res.setHeader("X-Quota-Remaining", String(result.remaining));
		await next();
	};
}
