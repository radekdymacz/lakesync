import { API_ERROR_CODES } from "@lakesync/core";
import { verifyToken } from "./auth";
import type { Env } from "./env";
import { logger } from "./logger";
import {
	allShardGatewayIds,
	handleShardedAdmin,
	handleShardedCheckpoint,
	handleShardedPull,
	handleShardedPush,
	parseShardConfig,
	type ShardConfig,
} from "./shard-router";

export { SyncGatewayDO } from "./sync-gateway-do";

/**
 * Extract the Bearer token from an Authorization header.
 * Returns the raw token string, or null if the header is missing or malformed.
 */
function extractBearerToken(request: Request): string | null {
	const header = request.headers.get("Authorization");
	if (!header) return null;
	const match = header.match(/^Bearer\s+(\S+)$/);
	return match?.[1] ?? null;
}

/**
 * Return a 401 Unauthorized JSON response with the given error message.
 */
function unauthorised(message: string, requestId: string): Response {
	return new Response(
		JSON.stringify({ error: message, code: API_ERROR_CODES.AUTH_ERROR, requestId }),
		{
			status: 401,
			headers: { "Content-Type": "application/json", "X-Request-Id": requestId },
		},
	);
}

// ---------------------------------------------------------------------------
// Route table
// ---------------------------------------------------------------------------

interface RouteMatch {
	/** Gateway ID extracted from URL path */
	gatewayId: string;
	/** Path to forward to the Durable Object */
	doPath: string;
	/** HTTP method for the DO request (defaults to original method) */
	doMethod?: string;
	/** Whether the request body should be forwarded */
	forwardBody: boolean;
}

interface RouteEntry {
	pattern: RegExp;
	/** Extract a RouteMatch from the regex match groups */
	extract: (match: RegExpMatchArray, method: string) => RouteMatch | null;
}

const ROUTE_TABLE: RouteEntry[] = [
	{
		// POST /v1/sync/:gatewayId/push | GET /v1/sync/:gatewayId/pull | POST /v1/sync/:gatewayId/action | GET /v1/sync/:gatewayId/actions
		pattern: /^\/v1\/sync\/([^/]+)\/(push|pull|action|actions)$/,
		extract: (match) => {
			const gatewayId = match[1];
			const action = match[2];
			if (!gatewayId || !action) return null;
			return {
				gatewayId,
				doPath: `/${action}`,
				forwardBody: action === "push" || action === "action",
			};
		},
	},
	{
		// GET /v1/sync/:gatewayId/checkpoint
		pattern: /^\/v1\/sync\/([^/]+)\/checkpoint$/,
		extract: (match) => {
			const gatewayId = match[1];
			if (!gatewayId) return null;
			return { gatewayId, doPath: "/checkpoint", doMethod: "GET", forwardBody: false };
		},
	},
	{
		// WebSocket /v1/sync/:gatewayId/ws
		pattern: /^\/v1\/sync\/([^/]+)\/ws$/,
		extract: (match) => {
			const gatewayId = match[1];
			if (!gatewayId) return null;
			return { gatewayId, doPath: "/ws", doMethod: "GET", forwardBody: false };
		},
	},
	{
		// POST /v1/admin/flush/:gatewayId
		pattern: /^\/v1\/admin\/flush\/([^/]+)$/,
		extract: (match) => {
			const gatewayId = match[1];
			if (!gatewayId) return null;
			return { gatewayId, doPath: "/flush", doMethod: "POST", forwardBody: false };
		},
	},
	{
		// POST /v1/admin/schema/:gatewayId
		pattern: /^\/v1\/admin\/schema\/([^/]+)$/,
		extract: (match) => {
			const gatewayId = match[1];
			if (!gatewayId) return null;
			return { gatewayId, doPath: "/admin/schema", doMethod: "POST", forwardBody: true };
		},
	},
	{
		// POST /v1/admin/sync-rules/:gatewayId
		pattern: /^\/v1\/admin\/sync-rules\/([^/]+)$/,
		extract: (match) => {
			const gatewayId = match[1];
			if (!gatewayId) return null;
			return { gatewayId, doPath: "/admin/sync-rules", doMethod: "POST", forwardBody: true };
		},
	},
	{
		// POST|GET /v1/admin/connectors/:gatewayId
		pattern: /^\/v1\/admin\/connectors\/([^/]+)$/,
		extract: (match, method) => {
			const gatewayId = match[1];
			if (!gatewayId) return null;
			if (method === "POST") {
				return { gatewayId, doPath: "/admin/connectors", doMethod: "POST", forwardBody: true };
			}
			return { gatewayId, doPath: "/admin/connectors", doMethod: "GET", forwardBody: false };
		},
	},
	{
		// DELETE /v1/admin/connectors/:gatewayId/:name
		pattern: /^\/v1\/admin\/connectors\/([^/]+)\/([^/]+)$/,
		extract: (match) => {
			const gatewayId = match[1];
			const name = match[2];
			if (!gatewayId || !name) return null;
			return {
				gatewayId,
				doPath: `/admin/connectors/${name}`,
				doMethod: "DELETE",
				forwardBody: false,
			};
		},
	},
	{
		// GET /v1/admin/metrics/:gatewayId
		pattern: /^\/v1\/admin\/metrics\/([^/]+)$/,
		extract: (match) => {
			const gatewayId = match[1];
			if (!gatewayId) return null;
			return { gatewayId, doPath: "/admin/metrics", doMethod: "GET", forwardBody: false };
		},
	},
];

function matchRoute(path: string, method: string): RouteMatch | null {
	for (const route of ROUTE_TABLE) {
		const match = path.match(route.pattern);
		if (match) {
			return route.extract(match, method);
		}
	}
	return null;
}

async function handleRequest(request: Request, env: Env, requestId: string): Promise<Response> {
	const startMs = Date.now();
	const url = new URL(request.url);
	const path = url.pathname;
	const method = request.method;
	const origin = request.headers.get("Origin");

	logger.info("request", { method, path, origin: origin ?? undefined, requestId });

	// Health check — unauthenticated
	if (path === "/health" && method === "GET") {
		return new Response(JSON.stringify({ status: "ok" }), {
			status: 200,
			headers: { "Content-Type": "application/json", "X-Request-Id": requestId },
		});
	}

	// OpenAPI spec — unauthenticated (static metadata)
	if (path === "/v1/openapi.json" && method === "GET") {
		const { generateOpenApiJson } = await import("@lakesync/gateway");
		return new Response(generateOpenApiJson(), {
			status: 200,
			headers: {
				"Content-Type": "application/json",
				"API-Version": "v1",
				"X-Request-Id": requestId,
			},
		});
	}

	// Connector types — unauthenticated (static metadata)
	if (path === "/v1/connectors/types" && method === "GET") {
		const { handleListConnectorTypes } = await import("@lakesync/gateway");
		const result = handleListConnectorTypes();
		return new Response(JSON.stringify(result.body), {
			status: result.status,
			headers: {
				"Content-Type": "application/json",
				"API-Version": "v1",
				"X-Request-Id": requestId,
			},
		});
	}

	// ── Legacy path redirect ───────────────────────────────────────
	// Redirect unversioned paths to their /v1/ equivalents.
	if (
		(path.startsWith("/sync/") || path.startsWith("/admin/") || path === "/connectors/types") &&
		!path.startsWith("/v1/")
	) {
		const newUrl = new URL(request.url);
		newUrl.pathname = `/v1${path}`;
		return new Response(null, {
			status: 301,
			headers: {
				Location: newUrl.toString(),
				Sunset: "2026-06-01",
				"Content-Type": "application/json",
			},
		});
	}

	// ── Authentication ──────────────────────────────────────────────
	const token = extractBearerToken(request) ?? url.searchParams.get("token");
	if (!token) {
		logger.warn("auth_failed", { reason: "Missing Bearer token", requestId });
		return unauthorised("Missing Bearer token", requestId);
	}

	const jwtSecret: string | [string, string] = env.JWT_SECRET_PREVIOUS
		? [env.JWT_SECRET, env.JWT_SECRET_PREVIOUS]
		: env.JWT_SECRET;
	const authResult = await verifyToken(token, jwtSecret);
	if (!authResult.ok) {
		logger.warn("auth_failed", { reason: authResult.error.message, requestId });
		return unauthorised(authResult.error.message, requestId);
	}

	const { clientId, gatewayId: jwtGatewayId, role } = authResult.value;

	// ── Admin route protection ───────────────────────────────────────
	if (path.startsWith("/v1/admin/") && role !== "admin") {
		logger.warn("admin_denied", { clientId, path, requestId });
		return new Response(
			JSON.stringify({ error: "Admin role required", code: API_ERROR_CODES.FORBIDDEN, requestId }),
			{
				status: 403,
				headers: { "Content-Type": "application/json", "X-Request-Id": requestId },
			},
		);
	}

	// ── Routing ─────────────────────────────────────────────────────

	const route = matchRoute(path, method);
	if (!route) {
		logger.warn("route_not_found", { path, requestId });
		return new Response(
			JSON.stringify({ error: "Not found", code: API_ERROR_CODES.NOT_FOUND, requestId }),
			{
				status: 404,
				headers: { "Content-Type": "application/json", "X-Request-Id": requestId },
			},
		);
	}

	// ── Gateway ID enforcement ──────────────────────────────────────
	if (route.gatewayId !== jwtGatewayId) {
		return new Response(
			JSON.stringify({
				error: "Gateway ID mismatch: JWT authorises a different gateway",
				code: API_ERROR_CODES.FORBIDDEN,
				requestId,
			}),
			{ status: 403, headers: { "Content-Type": "application/json", "X-Request-Id": requestId } },
		);
	}

	// Attach identity headers to the request for DO consumption
	const shardHeaders = new Headers(request.headers);
	shardHeaders.set("X-Client-Id", clientId);
	const { customClaims } = authResult.value;
	shardHeaders.set("X-Auth-Claims", JSON.stringify(customClaims));

	const enrichedRequest = new Request(request.url, {
		method: request.method,
		headers: shardHeaders,
		body: request.body,
	});

	// ── Shard routing ──────────────────────────────────────────────
	const shardConfig = env.SHARD_CONFIG ? parseShardConfig(env.SHARD_CONFIG) : null;

	if (shardConfig) {
		const response = await handleShardedRoute(shardConfig, enrichedRequest, route, env);
		const durationMs = Date.now() - startMs;
		logger.info("response", { method, path, status: response.status, durationMs, sharded: true });
		return withApiVersion(response);
	}

	// ── Direct DO routing (no sharding) ─────────────────────────────

	const id = env.SYNC_GATEWAY.idFromName(route.gatewayId);
	const stub = env.SYNC_GATEWAY.get(id);

	const doUrl = new URL(request.url);
	doUrl.pathname = route.doPath;

	const response = await stub.fetch(
		new Request(doUrl.toString(), {
			method: route.doMethod ?? method,
			headers: shardHeaders,
			body: route.forwardBody ? enrichedRequest.body : undefined,
		}),
	);

	const durationMs = Date.now() - startMs;
	logger.info("response", { method, path, status: response.status, durationMs });

	return withApiVersion(response);
}

/**
 * Route a request through the shard router, dispatching to the appropriate
 * Durable Object(s) based on the table sharding configuration.
 */
async function handleShardedRoute(
	config: ShardConfig,
	request: Request,
	route: RouteMatch,
	env: Env,
): Promise<Response> {
	const url = new URL(request.url);

	// Push — partition deltas by shard and fan out
	if (route.doPath === "/push") {
		const headerClientId = request.headers.get("X-Client-Id");
		return handleShardedPush(config, request, env.SYNC_GATEWAY, headerClientId);
	}

	// Pull — fan out to all shards and merge
	if (route.doPath === "/pull") {
		return handleShardedPull(config, request, url, env.SYNC_GATEWAY);
	}

	// Checkpoint — fan out to all shards and merge
	if (route.doPath === "/checkpoint") {
		return handleShardedCheckpoint(config, request, env.SYNC_GATEWAY);
	}

	// Actions discovery / action execution — route to default shard (connector-scoped, not table-scoped)
	if (route.doPath === "/action" || route.doPath === "/actions") {
		const id = env.SYNC_GATEWAY.idFromName(config.default);
		const stub = env.SYNC_GATEWAY.get(id);
		const doUrl = new URL(request.url);
		doUrl.pathname = route.doPath;
		return stub.fetch(
			new Request(doUrl.toString(), {
				method: route.doMethod ?? request.method,
				headers: request.headers,
				body: request.body,
			}),
		);
	}

	// WebSocket — route to default shard (cross-shard broadcasting is future work)
	if (route.doPath === "/ws") {
		const id = env.SYNC_GATEWAY.idFromName(config.default);
		const stub = env.SYNC_GATEWAY.get(id);
		const doUrl = new URL(request.url);
		doUrl.pathname = route.doPath;
		return stub.fetch(
			new Request(doUrl.toString(), {
				method: route.doMethod ?? request.method,
				headers: request.headers,
			}),
		);
	}

	// Metrics — fan out to all shards and sum stats
	if (route.doPath === "/admin/metrics") {
		const gatewayIds = allShardGatewayIds(config);
		const responses = await Promise.all(
			gatewayIds.map(async (gid) => {
				const stub = env.SYNC_GATEWAY.get(env.SYNC_GATEWAY.idFromName(gid));
				const doUrl = new URL(request.url);
				doUrl.pathname = "/admin/metrics";
				return stub.fetch(
					new Request(doUrl.toString(), { method: "GET", headers: request.headers }),
				);
			}),
		);

		const totals = { logSize: 0, indexSize: 0, byteSize: 0 };
		for (const resp of responses) {
			if (!resp.ok) continue;
			const stats = (await resp.json()) as typeof totals;
			totals.logSize += stats.logSize;
			totals.indexSize += stats.indexSize;
			totals.byteSize += stats.byteSize;
		}

		return new Response(JSON.stringify(totals), {
			status: 200,
			headers: { "Content-Type": "application/json" },
		});
	}

	// Admin operations — fan out to all shards
	let body: string | null = null;
	if (route.forwardBody) {
		body = await request.text();
	}

	return handleShardedAdmin(
		config,
		request,
		route.doPath,
		route.doMethod ?? request.method,
		env.SYNC_GATEWAY,
		body,
	);
}

// ---------------------------------------------------------------------------
// API version header
// ---------------------------------------------------------------------------

/** Add `API-Version: v1` header to a response from a versioned route. */
function withApiVersion(response: Response): Response {
	const headers = new Headers(response.headers);
	headers.set("API-Version", "v1");
	return new Response(response.body, {
		status: response.status,
		statusText: response.statusText,
		headers,
	});
}

// ---------------------------------------------------------------------------
// CORS
// ---------------------------------------------------------------------------

function corsHeaders(env: Env, origin?: string | null): Record<string, string> {
	let allowOrigin = "*";

	if (env.ALLOWED_ORIGINS) {
		const allowlist = env.ALLOWED_ORIGINS.split(",").map((o) => o.trim());
		if (origin && allowlist.includes(origin)) {
			allowOrigin = origin;
		} else {
			// Origin not in allowlist — return empty CORS to block the request
			return {};
		}
	} else if (origin) {
		// No allowlist configured (dev) — reflect origin
		allowOrigin = origin;
	}

	return {
		"Access-Control-Allow-Origin": allowOrigin,
		"Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
		"Access-Control-Allow-Headers": "Authorization, Content-Type, X-Client-Id, X-Auth-Claims",
		"Access-Control-Expose-Headers": "X-Checkpoint-Hlc, X-Sync-Rules-Version, X-Request-Id",
		"Access-Control-Max-Age": "86400",
	};
}

/** Standard security headers applied to every response. */
const SECURITY_HEADERS: Record<string, string> = {
	"X-Content-Type-Options": "nosniff",
	"X-Frame-Options": "DENY",
	"Strict-Transport-Security": "max-age=31536000; includeSubDomains",
};

function withCorsAndSecurity(
	response: Response,
	env: Env,
	origin?: string | null,
	path?: string,
	requestId?: string,
): Response {
	const headers = new Headers(response.headers);
	for (const [key, value] of Object.entries(corsHeaders(env, origin))) {
		headers.set(key, value);
	}
	// Security headers on all responses
	for (const [key, value] of Object.entries(SECURITY_HEADERS)) {
		headers.set(key, value);
	}
	// Request ID on all responses
	if (requestId) {
		headers.set("X-Request-Id", requestId);
	}
	// Cache-Control: no-store on /sync/* and /admin/* responses only (including /v1/ prefixed)
	if (
		path &&
		(path.startsWith("/sync/") ||
			path.startsWith("/admin/") ||
			path.startsWith("/v1/sync/") ||
			path.startsWith("/v1/admin/"))
	) {
		headers.set("Cache-Control", "no-store");
	}
	return new Response(response.body, {
		status: response.status,
		statusText: response.statusText,
		headers,
	});
}

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
		const origin = request.headers.get("Origin");
		const path = new URL(request.url).pathname;

		// Accept incoming X-Request-Id (pass-through from load balancer) or generate a new one
		const requestId = request.headers.get("X-Request-Id") ?? crypto.randomUUID();

		if (request.method === "OPTIONS") {
			return new Response(null, {
				status: 204,
				headers: { ...corsHeaders(env, origin), "X-Request-Id": requestId },
			});
		}

		const response = await handleRequest(request, env, requestId);
		return withCorsAndSecurity(response, env, origin, path, requestId);
	},
} satisfies ExportedHandler<Env>;
