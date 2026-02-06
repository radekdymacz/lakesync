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

export default {
	async fetch(request: Request, env: Env): Promise<Response> {
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

			// Forward the request to the Durable Object, passing clientId via header
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

		return new Response("Not found", { status: 404 });
	},
} satisfies ExportedHandler<Env>;
