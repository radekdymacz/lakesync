// ---------------------------------------------------------------------------
// CORS Middleware
// ---------------------------------------------------------------------------

import type { ServerResponse } from "node:http";

/** Configuration for CORS header generation. */
export interface CorsConfig {
	/** Allowed origins. When empty/omitted, all origins are reflected. */
	allowedOrigins?: string[];
}

/**
 * Build CORS response headers for the given origin.
 *
 * When `allowedOrigins` is set, only listed origins receive CORS headers.
 * When omitted, the request origin is reflected (or `*` if no origin header).
 */
export function corsHeaders(
	origin: string | null | undefined,
	config: CorsConfig,
): Record<string, string> {
	const { allowedOrigins } = config;
	let allowOrigin = "*";

	if (allowedOrigins && allowedOrigins.length > 0) {
		if (origin && allowedOrigins.includes(origin)) {
			allowOrigin = origin;
		} else {
			return {};
		}
	} else if (origin) {
		allowOrigin = origin;
	}

	return {
		"Access-Control-Allow-Origin": allowOrigin,
		"Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
		"Access-Control-Allow-Headers": "Authorization, Content-Type",
		"Access-Control-Max-Age": "86400",
	};
}

/** Handle CORS preflight (OPTIONS) request. Returns true if handled. */
export function handlePreflight(
	method: string,
	res: ServerResponse,
	corsH: Record<string, string>,
): boolean {
	if (method !== "OPTIONS") return false;
	res.writeHead(204, corsH);
	res.end();
	return true;
}
