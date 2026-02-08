import { verifyToken } from "./auth";
import type { Env } from "./env";

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
	const url = new URL(request.url);
	const path = url.pathname;

	// Health check — unauthenticated
	if (path === "/health" && request.method === "GET") {
		return new Response(JSON.stringify({ status: "ok" }), {
			status: 200,
			headers: { "Content-Type": "application/json" },
		});
	}

	// ── Authentication ──────────────────────────────────────────────
	const token = extractBearerToken(request);
	if (!token) {
		return unauthorised("Missing Bearer token");
	}

	const authResult = await verifyToken(token, env.JWT_SECRET);
	if (!authResult.ok) {
		return unauthorised(authResult.error.message);
	}

	const { clientId, gatewayId: jwtGatewayId } = authResult.value;

	// ── Routing ─────────────────────────────────────────────────────

	const route = matchRoute(path, request.method);
	if (!route) {
		return new Response("Not found", { status: 404 });
	}

	// ── Gateway ID enforcement ──────────────────────────────────────
	if (route.gatewayId !== jwtGatewayId) {
		return new Response(
			JSON.stringify({ error: "Gateway ID mismatch: JWT authorises a different gateway" }),
			{ status: 403, headers: { "Content-Type": "application/json" } },
		);
	}

	const id = env.SYNC_GATEWAY.idFromName(route.gatewayId);
	const stub = env.SYNC_GATEWAY.get(id);

	const doUrl = new URL(request.url);
	doUrl.pathname = route.doPath;
	const doHeaders = new Headers(request.headers);
	doHeaders.set("X-Client-Id", clientId);

	// Forward custom JWT claims for sync rules evaluation
	const { customClaims } = authResult.value;
	doHeaders.set("X-Auth-Claims", JSON.stringify(customClaims));

	return stub.fetch(
		new Request(doUrl.toString(), {
			method: route.doMethod ?? request.method,
			headers: doHeaders,
			body: route.forwardBody ? request.body : undefined,
		}),
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
