// ---------------------------------------------------------------------------
// Route Handlers — standalone data-driven dispatch map
// ---------------------------------------------------------------------------

import type { IncomingMessage } from "node:http";
import type { HLCTimestamp, RowDelta, SyncResponse } from "@lakesync/core";
import type { ConfigStore, HandlerResult } from "@lakesync/gateway";
import {
	handleActionRequest,
	handleFlushRequest,
	handleMetrics,
	handlePullRequest,
	handlePushRequest,
	handleSaveSchema,
	handleSaveSyncRules,
	MAX_PUSH_PAYLOAD_BYTES,
	type SyncGateway,
} from "@lakesync/gateway";
import type { ConnectorManager } from "./connector-manager";
import type { MetricsRegistry } from "./metrics";
import type { RequestContext, RouteHandler } from "./middleware";
import type { DeltaPersistence } from "./persistence";
import { sendError, sendJson } from "./pipeline";
import type { SharedBuffer } from "./shared-buffer";
import type { WebSocketManager } from "./ws-manager";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Dependencies for building the route handler map. */
export interface RouteHandlerDeps {
	readonly gateway: SyncGateway;
	readonly configStore: ConfigStore;
	readonly persistence: DeltaPersistence;
	readonly connectors: ConnectorManager;
	readonly metrics: MetricsRegistry;
	readonly sharedBuffer: SharedBuffer | null;
	readonly gatewayId: string;
	readonly getWsManager: () => WebSocketManager | null;
	readonly updateBufferGauges: () => void;
}

// ---------------------------------------------------------------------------
// Node HTTP helpers
// ---------------------------------------------------------------------------

/** Read the full request body as a string. */
function readBody(req: IncomingMessage): Promise<string> {
	return new Promise((resolve, reject) => {
		const chunks: Buffer[] = [];
		req.on("data", (chunk: Buffer) => chunks.push(chunk));
		req.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
		req.on("error", reject);
	});
}

/** Send a HandlerResult as HTTP response. */
function sendResult(
	res: import("node:http").ServerResponse,
	result: HandlerResult,
	corsH: Record<string, string>,
): void {
	sendJson(res, result.body, result.status, corsH);
}

// ---------------------------------------------------------------------------
// Individual route handlers
// ---------------------------------------------------------------------------

function handlePush(deps: RouteHandlerDeps): RouteHandler {
	return async (ctx: RequestContext) => {
		const { req, res, corsHeaders: corsH, auth, logger: reqLogger } = ctx;
		const start = performance.now();
		const contentLength = Number(req.headers["content-length"] ?? "0");
		if (contentLength > MAX_PUSH_PAYLOAD_BYTES) {
			deps.metrics.pushTotal.inc({ status: "error" });
			sendError(res, "Payload too large (max 1 MiB)", 413, corsH);
			return;
		}

		const raw = await readBody(req);
		const result = handlePushRequest(deps.gateway, raw, auth?.clientId, {
			persistBatch: (deltas) => deps.persistence.appendBatch(deltas),
			clearPersistence: () => deps.persistence.clear(),
			broadcastFn: (deltas, serverHlc, excludeClientId) =>
				deps.getWsManager()?.broadcastDeltas(deltas, serverHlc, excludeClientId),
		});

		// Shared buffer write-through for cross-instance visibility
		if (result.status === 200 && deps.sharedBuffer) {
			const pushResult = result.body as { deltas: RowDelta[]; serverHlc: HLCTimestamp };
			if (pushResult.deltas.length > 0) {
				const writeResult = await deps.sharedBuffer.writeThroughPush(pushResult.deltas);
				if (!writeResult.ok) {
					deps.metrics.pushTotal.inc({ status: "error" });
					sendError(res, writeResult.error.message, 502, corsH);
					return;
				}
			}
		}

		const status = result.status === 200 ? "ok" : "error";
		const durationMs = Math.round(performance.now() - start);
		deps.metrics.pushTotal.inc({ status });
		deps.metrics.pushLatency.observe({}, performance.now() - start);
		deps.updateBufferGauges();
		reqLogger.info("push completed", { status: result.status, durationMs });

		sendResult(res, result, corsH);
	};
}

function handlePull(deps: RouteHandlerDeps): RouteHandler {
	return async (ctx: RequestContext) => {
		const { url, res, corsHeaders: corsH, auth, logger: reqLogger } = ctx;
		const syncRules = await deps.configStore.getSyncRules(deps.gatewayId);
		const result = await handlePullRequest(
			deps.gateway,
			{
				since: url.searchParams.get("since"),
				clientId: url.searchParams.get("clientId"),
				limit: url.searchParams.get("limit"),
				source: url.searchParams.get("source"),
			},
			auth?.customClaims,
			syncRules,
		);

		// Merge with shared buffer for cross-instance visibility
		let body = result.body;
		if (result.status === 200 && deps.sharedBuffer) {
			const sinceParam = url.searchParams.get("since");
			if (sinceParam) {
				try {
					const sinceHlc = BigInt(sinceParam) as HLCTimestamp;
					body = await deps.sharedBuffer.mergePull(body as SyncResponse, sinceHlc);
				} catch {
					// If since parsing fails, pull handler already returned an error
				}
			}
		}

		const pullStatus = result.status === 200 ? "ok" : "error";
		deps.metrics.pullTotal.inc({ status: pullStatus });
		reqLogger.info("pull completed", { status: result.status });

		sendJson(res, body, result.status, corsH);
	};
}

function handleAction(deps: RouteHandlerDeps): RouteHandler {
	return async (ctx: RequestContext) => {
		const { req, res, corsHeaders: corsH, auth } = ctx;
		const raw = await readBody(req);
		const result = await handleActionRequest(deps.gateway, raw, auth?.clientId, auth?.customClaims);
		sendResult(res, result, corsH);
	};
}

function handleDescribeActions(deps: RouteHandlerDeps): RouteHandler {
	return (ctx: RequestContext) => {
		sendJson(ctx.res, deps.gateway.describeActions(), 200, ctx.corsHeaders);
	};
}

function handleFlush(deps: RouteHandlerDeps): RouteHandler {
	return async (ctx: RequestContext) => {
		const { res, corsHeaders: corsH, logger: reqLogger } = ctx;
		const start = performance.now();
		const result = await handleFlushRequest(deps.gateway, {
			clearPersistence: () => deps.persistence.clear(),
		});
		const durationMs = Math.round(performance.now() - start);
		const status = result.status === 200 ? "ok" : "error";
		deps.metrics.flushTotal.inc({ status });
		deps.metrics.flushDuration.observe({}, performance.now() - start);
		deps.updateBufferGauges();
		reqLogger.info("flush completed", { status: result.status, durationMs });
		sendResult(res, result, corsH);
	};
}

function handleSaveSchemaRoute(deps: RouteHandlerDeps): RouteHandler {
	return async (ctx: RequestContext) => {
		const raw = await readBody(ctx.req);
		const result = await handleSaveSchema(raw, deps.configStore, deps.gatewayId);
		sendResult(ctx.res, result, ctx.corsHeaders);
	};
}

function handleSaveSyncRulesRoute(deps: RouteHandlerDeps): RouteHandler {
	return async (ctx: RequestContext) => {
		const raw = await readBody(ctx.req);
		const result = await handleSaveSyncRules(raw, deps.configStore, deps.gatewayId);
		sendResult(ctx.res, result, ctx.corsHeaders);
	};
}

function handleRegisterConnectorRoute(deps: RouteHandlerDeps): RouteHandler {
	return async (ctx: RequestContext) => {
		const raw = await readBody(ctx.req);
		const result = await deps.connectors.register(raw);
		sendResult(ctx.res, result, ctx.corsHeaders);
	};
}

function handleUnregisterConnectorRoute(deps: RouteHandlerDeps): RouteHandler {
	return async (ctx: RequestContext) => {
		const result = await deps.connectors.unregister(ctx.route!.connectorName!);
		sendResult(ctx.res, result, ctx.corsHeaders);
	};
}

function handleListConnectorsRoute(deps: RouteHandlerDeps): RouteHandler {
	return async (ctx: RequestContext) => {
		const result = await deps.connectors.list();
		sendJson(ctx.res, result.body, result.status, ctx.corsHeaders);
	};
}

function handleMetricsRoute(deps: RouteHandlerDeps): RouteHandler {
	return (ctx: RequestContext) => {
		const result = handleMetrics(deps.gateway, { process: process.memoryUsage() });
		sendResult(ctx.res, result, ctx.corsHeaders);
	};
}

// ---------------------------------------------------------------------------
// Public API — build the route handler map
// ---------------------------------------------------------------------------

/**
 * Build the action -> handler map for route dispatch.
 *
 * Each handler is a standalone function closed over shared dependencies.
 * The map is consumed by the pipeline's route dispatch middleware.
 */
export function buildServerRouteHandlers(deps: RouteHandlerDeps): Record<string, RouteHandler> {
	return {
		push: handlePush(deps),
		pull: handlePull(deps),
		action: handleAction(deps),
		"describe-actions": handleDescribeActions(deps),
		flush: handleFlush(deps),
		schema: handleSaveSchemaRoute(deps),
		"sync-rules": handleSaveSyncRulesRoute(deps),
		"register-connector": handleRegisterConnectorRoute(deps),
		"unregister-connector": handleUnregisterConnectorRoute(deps),
		"list-connectors": handleListConnectorsRoute(deps),
		metrics: handleMetricsRoute(deps),
	};
}
