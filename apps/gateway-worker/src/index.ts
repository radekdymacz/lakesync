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

	const { clientId } = authResult.value;

	// ── Routing ─────────────────────────────────────────────────────

	// Route: POST /sync/:gatewayId/push
	// Route: GET  /sync/:gatewayId/pull
	const syncMatch = path.match(/^\/sync\/([^/]+)\/(push|pull)$/);
	if (syncMatch) {
		const [, gatewayId, action] = syncMatch;
		if (!gatewayId || !action) return new Response("Bad request", { status: 400 });

		const id = env.SYNC_GATEWAY.idFromName(gatewayId);
		const stub = env.SYNC_GATEWAY.get(id);

		const doUrl = new URL(request.url);
		doUrl.pathname = `/${action}`;
		const doHeaders = new Headers(request.headers);
		doHeaders.set("X-Client-Id", clientId);
		return stub.fetch(
			new Request(doUrl.toString(), {
				method: request.method,
				headers: doHeaders,
				body: request.body,
			}),
		);
	}

	// Route: POST /admin/flush/:gatewayId
	const flushMatch = path.match(/^\/admin\/flush\/([^/]+)$/);
	if (flushMatch) {
		const [, gatewayId] = flushMatch;
		if (!gatewayId) return new Response("Bad request", { status: 400 });

		const id = env.SYNC_GATEWAY.idFromName(gatewayId);
		const stub = env.SYNC_GATEWAY.get(id);

		const doUrl = new URL(request.url);
		doUrl.pathname = "/flush";
		const doHeaders = new Headers(request.headers);
		doHeaders.set("X-Client-Id", clientId);
		return stub.fetch(new Request(doUrl.toString(), { method: "POST", headers: doHeaders }));
	}

	// Route: POST /admin/schema/:gatewayId
	const schemaMatch = path.match(/^\/admin\/schema\/([^/]+)$/);
	if (schemaMatch) {
		const [, gatewayId] = schemaMatch;
		if (!gatewayId) return new Response("Bad request", { status: 400 });

		const id = env.SYNC_GATEWAY.idFromName(gatewayId);
		const stub = env.SYNC_GATEWAY.get(id);

		const doUrl = new URL(request.url);
		doUrl.pathname = "/admin/schema";
		const doHeaders = new Headers(request.headers);
		doHeaders.set("X-Client-Id", clientId);
		return stub.fetch(
			new Request(doUrl.toString(), {
				method: "POST",
				headers: doHeaders,
				body: request.body,
			}),
		);
	}

	return new Response("Not found", { status: 404 });
}

function corsHeaders(origin?: string | null): Record<string, string> {
	return {
		"Access-Control-Allow-Origin": origin ?? "*",
		"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
		"Access-Control-Allow-Headers": "Authorization, Content-Type, X-Client-Id",
		"Access-Control-Max-Age": "86400",
	};
}

function withCors(response: Response, origin?: string | null): Response {
	const headers = new Headers(response.headers);
	for (const [key, value] of Object.entries(corsHeaders(origin))) {
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
			return new Response(null, { status: 204, headers: corsHeaders(origin) });
		}

		const response = await handleRequest(request, env);
		return withCors(response, origin);
	},
} satisfies ExportedHandler<Env>;
