import { verifyToken } from "./auth";
import type { Env } from "./env";
import { logger } from "./logger";
import {
	handleShardedAdmin,
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
function unauthorised(message: string): Response {
	return new Response(JSON.stringify({ error: message }), {
		status: 401,
		headers: { "Content-Type": "application/json" },
	});
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
		// POST /sync/:gatewayId/push | GET /sync/:gatewayId/pull
		pattern: /^\/sync\/([^/]+)\/(push|pull)$/,
		extract: (match) => {
			const gatewayId = match[1];
			const action = match[2];
			if (!gatewayId || !action) return null;
			return { gatewayId, doPath: `/${action}`, forwardBody: action === "push" };
		},
	},
	{
		// GET /sync/:gatewayId/checkpoint
		pattern: /^\/sync\/([^/]+)\/checkpoint$/,
		extract: (match) => {
			const gatewayId = match[1];
			if (!gatewayId) return null;
			return { gatewayId, doPath: "/checkpoint", doMethod: "GET", forwardBody: false };
		},
	},
	{
		// WebSocket /sync/:gatewayId/ws
		pattern: /^\/sync\/([^/]+)\/ws$/,
		extract: (match) => {
			const gatewayId = match[1];
			if (!gatewayId) return null;
			return { gatewayId, doPath: "/ws", doMethod: "GET", forwardBody: false };
		},
	},
	{
		// POST /admin/flush/:gatewayId
		pattern: /^\/admin\/flush\/([^/]+)$/,
		extract: (match) => {
			const gatewayId = match[1];
			if (!gatewayId) return null;
			return { gatewayId, doPath: "/flush", doMethod: "POST", forwardBody: false };
		},
	},
	{
		// POST /admin/schema/:gatewayId
		pattern: /^\/admin\/schema\/([^/]+)$/,
		extract: (match) => {
			const gatewayId = match[1];
			if (!gatewayId) return null;
			return { gatewayId, doPath: "/admin/schema", doMethod: "POST", forwardBody: true };
		},
	},
	{
		// POST /admin/sync-rules/:gatewayId
		pattern: /^\/admin\/sync-rules\/([^/]+)$/,
		extract: (match) => {
			const gatewayId = match[1];
			if (!gatewayId) return null;
			return { gatewayId, doPath: "/admin/sync-rules", doMethod: "POST", forwardBody: true };
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

async function handleRequest(request: Request, env: Env): Promise<Response> {
	const startMs = Date.now();
	const url = new URL(request.url);
	const path = url.pathname;
	const method = request.method;
	const origin = request.headers.get("Origin");

	logger.info("request", { method, path, origin: origin ?? undefined });

	// Health check — unauthenticated
	if (path === "/health" && method === "GET") {
		return new Response(JSON.stringify({ status: "ok" }), {
			status: 200,
			headers: { "Content-Type": "application/json" },
		});
	}

	// ── Authentication ──────────────────────────────────────────────
	const token = extractBearerToken(request) ?? url.searchParams.get("token");
	if (!token) {
		logger.warn("auth_failed", { reason: "Missing Bearer token" });
		return unauthorised("Missing Bearer token");
	}

	const authResult = await verifyToken(token, env.JWT_SECRET);
	if (!authResult.ok) {
		logger.warn("auth_failed", { reason: authResult.error.message });
		return unauthorised(authResult.error.message);
	}

	const { clientId, gatewayId: jwtGatewayId, role } = authResult.value;

	// ── Admin route protection ───────────────────────────────────────
	if (path.startsWith("/admin/") && role !== "admin") {
		logger.warn("admin_denied", { clientId, path });
		return new Response(JSON.stringify({ error: "Admin role required" }), {
			status: 403,
			headers: { "Content-Type": "application/json" },
		});
	}

	// ── Routing ─────────────────────────────────────────────────────

	const route = matchRoute(path, method);
	if (!route) {
		logger.warn("route_not_found", { path });
		return new Response("Not found", { status: 404 });
	}

	// ── Gateway ID enforcement ──────────────────────────────────────
	if (route.gatewayId !== jwtGatewayId) {
		return new Response(
			JSON.stringify({ error: "Gateway ID mismatch: JWT authorises a different gateway" }),
			{ status: 403, headers: { "Content-Type": "application/json" } },
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
		return response;
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

	return response;
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
		return handleShardedPush(config, request, env.SYNC_GATEWAY);
	}

	// Pull — fan out to all shards and merge
	if (route.doPath === "/pull") {
		return handleShardedPull(config, request, url, env.SYNC_GATEWAY);
	}

	// Checkpoint — fan out to all shards (not yet implemented as sharded)
	// For now, route to the default shard
	if (route.doPath === "/checkpoint") {
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
		"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
		"Access-Control-Allow-Headers": "Authorization, Content-Type, X-Client-Id, X-Auth-Claims",
		"Access-Control-Expose-Headers": "X-Checkpoint-Hlc, X-Sync-Rules-Version",
		"Access-Control-Max-Age": "86400",
	};
}

function withCors(response: Response, env: Env, origin?: string | null): Response {
	const headers = new Headers(response.headers);
	for (const [key, value] of Object.entries(corsHeaders(env, origin))) {
		headers.set(key, value);
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

		if (request.method === "OPTIONS") {
			return new Response(null, { status: 204, headers: corsHeaders(env, origin) });
		}

		const response = await handleRequest(request, env);
		return withCors(response, env, origin);
	},
} satisfies ExportedHandler<Env>;
