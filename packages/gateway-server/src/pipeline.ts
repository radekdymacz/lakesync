// ---------------------------------------------------------------------------
// Server Pipeline — standalone middleware composition
// ---------------------------------------------------------------------------

import type { ServerResponse } from "node:http";
import type { HLCTimestamp, LakeAdapter } from "@lakesync/core";
import { API_ERROR_CODES, isDatabaseAdapter } from "@lakesync/core";
import { generateOpenApiJson, handleListConnectorTypes } from "@lakesync/gateway";
import { authenticateRequest } from "./auth-middleware";
import { handlePreflight } from "./cors-middleware";
import type { MetricsRegistry } from "./metrics";
import type { Middleware, RouteHandler } from "./middleware";
import type { RateLimiter } from "./rate-limiter";
import { matchLegacyRoute, matchRoute } from "./router";
import type { GatewayServerConfig } from "./server";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Immutable configuration for building the server pipeline. */
export interface PipelineConfig {
	readonly allowedOrigins?: string[];
	readonly jwtSecret?: string | [string, string];
	readonly requestTimeoutMs?: number;
	readonly gatewayId: string;
	readonly rateLimiter: RateLimiter | null;
	readonly adapter?: GatewayServerConfig["adapter"];
}

/** Mutable server-level state shared across middleware. */
export interface PipelineState {
	draining: boolean;
	activeRequests: number;
}

// ---------------------------------------------------------------------------
// JSON helpers (shared with route-handlers)
// ---------------------------------------------------------------------------

import { bigintReplacer } from "@lakesync/core";

/** Send a JSON response. */
export function sendJson(
	res: ServerResponse,
	body: unknown,
	status = 200,
	extraHeaders?: Record<string, string>,
): void {
	const json = JSON.stringify(body, bigintReplacer);
	res.writeHead(status, {
		"Content-Type": "application/json",
		...extraHeaders,
	});
	res.end(json);
}

/** Send a JSON error response with optional structured error code and request ID. */
export function sendError(
	res: ServerResponse,
	message: string,
	status: number,
	extraHeaders?: Record<string, string>,
	opts?: { code?: string; requestId?: string },
): void {
	const body: Record<string, string> = { error: message };
	if (opts?.code) body.code = opts.code;
	if (opts?.requestId) body.requestId = opts.requestId;
	sendJson(res, body, status, extraHeaders);
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
const DEFAULT_ADAPTER_HEALTH_TIMEOUT_MS = 5_000;

// ---------------------------------------------------------------------------
// Standalone middleware factories
// ---------------------------------------------------------------------------

/** Standard security headers applied to every response. */
const SECURITY_HEADERS: Record<string, string> = {
	"X-Content-Type-Options": "nosniff",
	"X-Frame-Options": "DENY",
	"Strict-Transport-Security": "max-age=31536000; includeSubDomains",
};

/**
 * Security headers — sets standard security headers on every response.
 *
 * Also sets `Cache-Control: no-store` on /sync/* and /admin/* paths
 * (including /v1/ prefixed routes) to prevent caching of sensitive data.
 */
function securityHeaders(): Middleware {
	return async (ctx, next) => {
		for (const [key, value] of Object.entries(SECURITY_HEADERS)) {
			ctx.res.setHeader(key, value);
		}
		const { pathname } = ctx;
		if (
			pathname.startsWith("/sync/") ||
			pathname.startsWith("/admin/") ||
			pathname.startsWith("/v1/sync/") ||
			pathname.startsWith("/v1/admin/")
		) {
			ctx.res.setHeader("Cache-Control", "no-store");
		}
		await next();
	};
}

/** CORS preflight — short-circuits OPTIONS requests. */
function corsPreflight(): Middleware {
	return async (ctx, next) => {
		if (handlePreflight(ctx.method, ctx.res, ctx.corsHeaders)) return;
		await next();
	};
}

/** Static routes — health, ready, metrics, connector types. No auth required. */
function staticRoutes(
	config: PipelineConfig,
	state: PipelineState,
	metrics: MetricsRegistry,
	updateBufferGauges: () => void,
): Middleware {
	return async (ctx, next) => {
		const { pathname, method, res, corsHeaders: corsH } = ctx;

		if (pathname === "/health" && method === "GET") {
			sendJson(res, { status: "ok" }, 200, corsH);
			return;
		}

		if (pathname === "/ready" && method === "GET") {
			await handleReady(res, corsH, state.draining, config.adapter);
			return;
		}

		if (pathname === "/metrics" && method === "GET") {
			updateBufferGauges();
			const body = metrics.expose();
			res.writeHead(200, {
				"Content-Type": "text/plain; version=0.0.4; charset=utf-8",
				...corsH,
			});
			res.end(body);
			return;
		}

		if (pathname === "/v1/openapi.json" && method === "GET") {
			res.writeHead(200, {
				"Content-Type": "application/json",
				"API-Version": "v1",
				...corsH,
			});
			res.end(generateOpenApiJson());
			return;
		}

		if (pathname === "/v1/connectors/types" && method === "GET") {
			const result = handleListConnectorTypes();
			sendJson(res, result.body, result.status, { ...corsH, "API-Version": "v1" });
			return;
		}

		// Legacy path redirect — unversioned /sync/, /admin/, /connectors/types → 301 to /v1/
		const legacyRedirect = matchLegacyRoute(pathname);
		if (legacyRedirect) {
			res.writeHead(301, {
				Location: legacyRedirect,
				Sunset: "2026-06-01",
				"Content-Type": "application/json",
				...corsH,
			});
			res.end();
			return;
		}

		await next();
	};
}

/** Drain guard — rejects requests when the server is shutting down. */
function drainGuard(state: PipelineState): Middleware {
	return async (ctx, next) => {
		if (state.draining) {
			sendError(ctx.res, "Service is shutting down", 503, ctx.corsHeaders, {
				code: API_ERROR_CODES.INTERNAL_ERROR,
				requestId: ctx.requestId,
			});
			return;
		}
		await next();
	};
}

/** Request timeout — aborts with 504 after the configured timeout. */
function requestTimeout(config: PipelineConfig): Middleware {
	return async (ctx, next) => {
		const timeoutMs = config.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;
		ctx.res.setTimeout(timeoutMs, () => {
			if (!ctx.res.writableEnded) {
				sendError(ctx.res, "Request timeout", 504, ctx.corsHeaders, {
					code: API_ERROR_CODES.INTERNAL_ERROR,
					requestId: ctx.requestId,
				});
			}
		});
		await next();
	};
}

/** Active request tracking — increments/decrements for graceful shutdown. */
function activeRequestTracking(state: PipelineState): Middleware {
	return async (_ctx, next) => {
		state.activeRequests++;
		try {
			await next();
		} finally {
			state.activeRequests--;
		}
	};
}

/** Route matching — matches URL to route table, sets ctx.route. */
function routeMatching(config: PipelineConfig): Middleware {
	return async (ctx, next) => {
		const route = matchRoute(ctx.pathname, ctx.method);
		if (!route) {
			sendError(ctx.res, "Not found", 404, ctx.corsHeaders, {
				code: API_ERROR_CODES.NOT_FOUND,
				requestId: ctx.requestId,
			});
			return;
		}

		if (route.gatewayId !== config.gatewayId) {
			sendError(ctx.res, "Gateway ID mismatch", 404, ctx.corsHeaders, {
				code: API_ERROR_CODES.NOT_FOUND,
				requestId: ctx.requestId,
			});
			return;
		}

		ctx.route = route;
		await next();
	};
}

/** Authentication — validates JWT and sets ctx.auth. */
function authMiddleware(config: PipelineConfig): Middleware {
	return async (ctx, next) => {
		const route = ctx.route!;
		const authResult = await authenticateRequest(
			ctx.req,
			route.gatewayId,
			route.action,
			config.jwtSecret,
		);
		if (!authResult.authenticated) {
			sendError(ctx.res, authResult.message, authResult.status, ctx.corsHeaders, {
				code: API_ERROR_CODES.AUTH_ERROR,
				requestId: ctx.requestId,
			});
			return;
		}
		ctx.auth = config.jwtSecret ? authResult.claims : undefined;
		await next();
	};
}

/** Rate limiting — rejects requests exceeding the per-client rate limit. */
function rateLimitMiddleware(config: PipelineConfig): Middleware {
	return async (ctx, next) => {
		if (config.rateLimiter) {
			const clientKey = ctx.auth?.clientId ?? ctx.req.socket.remoteAddress ?? "unknown";
			if (!config.rateLimiter.tryConsume(clientKey)) {
				const retryAfter = config.rateLimiter.retryAfterSeconds(clientKey);
				sendError(
					ctx.res,
					"Too many requests",
					429,
					{
						...ctx.corsHeaders,
						"Retry-After": String(retryAfter),
					},
					{ code: API_ERROR_CODES.RATE_LIMITED, requestId: ctx.requestId },
				);
				return;
			}
		}
		await next();
	};
}

/** Route dispatch — looks up handler from the route handler map. Sets API-Version header. */
function routeDispatch(routeHandlers: Record<string, RouteHandler>): Middleware {
	return async (ctx) => {
		const handler = routeHandlers[ctx.route!.action];
		if (!handler) {
			sendError(ctx.res, "Not found", 404, ctx.corsHeaders, {
				code: API_ERROR_CODES.NOT_FOUND,
				requestId: ctx.requestId,
			});
			return;
		}
		// Set API-Version header on all versioned route responses
		ctx.res.setHeader("API-Version", "v1");
		await handler(ctx);
	};
}

// ---------------------------------------------------------------------------
// Readiness probe helpers
// ---------------------------------------------------------------------------

/** Handle GET /ready — checks draining status and adapter health. */
async function handleReady(
	res: ServerResponse,
	corsH: Record<string, string>,
	draining: boolean,
	adapter: GatewayServerConfig["adapter"],
): Promise<void> {
	if (draining) {
		sendJson(res, { status: "not_ready", reason: "draining" }, 503, corsH);
		return;
	}

	const adapterHealthy = await checkAdapterHealth(adapter);
	if (!adapterHealthy) {
		sendJson(res, { status: "not_ready", reason: "adapter unreachable" }, 503, corsH);
		return;
	}

	sendJson(res, { status: "ready" }, 200, corsH);
}

/**
 * Check whether the configured adapter is reachable.
 *
 * For a DatabaseAdapter, attempts a lightweight query with a timeout.
 * For a LakeAdapter, attempts a headObject call (404 still means reachable).
 * Returns true when no adapter is configured (stateless mode).
 */
async function checkAdapterHealth(adapter: GatewayServerConfig["adapter"]): Promise<boolean> {
	if (!adapter) return true;

	const timeoutMs = DEFAULT_ADAPTER_HEALTH_TIMEOUT_MS;
	const timeoutPromise = new Promise<false>((resolve) => {
		setTimeout(() => resolve(false), timeoutMs);
	});

	try {
		if (isDatabaseAdapter(adapter)) {
			const healthCheck = adapter
				.queryDeltasSince(0n as HLCTimestamp, [])
				.then((result) => result.ok);
			return await Promise.race([healthCheck, timeoutPromise]);
		}
		// LakeAdapter — try headObject on a known key
		const healthCheck = (adapter as LakeAdapter)
			.headObject("__health__")
			.then(() => true)
			.catch(() => true); // S3 404 is still "reachable"
		return await Promise.race([healthCheck, timeoutPromise]);
	} catch {
		return false;
	}
}

// ---------------------------------------------------------------------------
// Public API — build the complete pipeline
// ---------------------------------------------------------------------------

/**
 * Build the middleware pipeline executed for every request.
 *
 * Each middleware is a standalone function closed over its dependencies —
 * no class binding required. The pipeline follows the order:
 * security headers -> cors -> static routes -> drain -> timeout ->
 * tracking -> route match -> auth -> rate limit -> dispatch.
 */
export function buildServerPipeline(
	config: PipelineConfig,
	state: PipelineState,
	routeHandlers: Record<string, RouteHandler>,
	metrics: MetricsRegistry,
	updateBufferGauges: () => void,
): Middleware[] {
	return [
		securityHeaders(),
		corsPreflight(),
		staticRoutes(config, state, metrics, updateBufferGauges),
		drainGuard(state),
		requestTimeout(config),
		activeRequestTracking(state),
		routeMatching(config),
		authMiddleware(config),
		rateLimitMiddleware(config),
		routeDispatch(routeHandlers),
	];
}
