import type { QuotaChecker, QuotaResult } from "./types";

/** Quota check context extracted from the request. */
export interface QuotaContext {
	/** Organisation ID (from JWT claims). */
	orgId: string;
	/** Gateway ID (from route params). */
	gatewayId: string;
	/** Request type: push, connection, or gateway_create. */
	type: "push" | "connection" | "gateway_create";
	/** Number of deltas in a push request (required when type is "push"). */
	deltaCount?: number;
}

/** Result of quota enforcement, suitable for building HTTP responses. */
export interface QuotaEnforcementResult {
	/** Whether the request should proceed. */
	allowed: boolean;
	/** HTTP status code (429 when blocked). */
	status: number;
	/** Response headers to set (X-Quota-Remaining, Retry-After). */
	headers: Record<string, string>;
	/** Error message when blocked. */
	message?: string;
}

/**
 * Enforce quota for a request.
 *
 * Checks the quota and returns a result with appropriate HTTP headers.
 * Returns `allowed: true` with `X-Quota-Remaining` header on success,
 * or `allowed: false` with 429 status, `Retry-After`, and error message
 * on quota exceeded.
 */
export async function enforceQuota(
	checker: QuotaChecker,
	ctx: QuotaContext,
): Promise<QuotaEnforcementResult> {
	let result: QuotaResult;

	switch (ctx.type) {
		case "push":
			result = await checker.checkPush(ctx.orgId, ctx.deltaCount ?? 0);
			break;
		case "connection":
			result = await checker.checkConnection(ctx.orgId, ctx.gatewayId);
			break;
		case "gateway_create":
			result = await checker.checkGatewayCreate(ctx.orgId);
			break;
	}

	if (result.allowed) {
		return {
			allowed: true,
			status: 200,
			headers: {
				"X-Quota-Remaining": String(result.remaining),
			},
		};
	}

	const headers: Record<string, string> = {};
	if (result.resetAt) {
		const retryAfter = Math.max(1, Math.ceil((result.resetAt.getTime() - Date.now()) / 1000));
		headers["Retry-After"] = String(retryAfter);
	}

	return {
		allowed: false,
		status: 429,
		headers,
		message: result.reason,
	};
}
