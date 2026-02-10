import type { HLCTimestamp, RowDelta, SyncResponse } from "@lakesync/core";
import { bigintReplacer, bigintReviver } from "@lakesync/core";
import { decodeSyncResponse, encodeSyncResponse } from "@lakesync/proto";
import { logger } from "./logger";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** A shard maps a set of table names to a specific gateway (Durable Object) ID. */
export interface Shard {
	/** Table names handled by this shard. */
	tables: string[];
	/** Gateway ID for the Durable Object that handles these tables. */
	gatewayId: string;
}

/**
 * Configuration for table-based sharding across multiple Durable Objects.
 *
 * Each shard declares the table names it owns. Tables not matched by any
 * shard are routed to the `default` gateway ID.
 */
export interface ShardConfig {
	/** Shard definitions — each maps table names to a gateway ID. */
	shards: Shard[];
	/** Fallback gateway ID for tables not matched by any shard. */
	default: string;
}

/**
 * Error type for shard routing failures.
 */
export class ShardRouterError extends Error {
	readonly code: string;

	constructor(message: string, code: string) {
		super(message);
		this.name = "ShardRouterError";
		this.code = code;
	}
}

// ---------------------------------------------------------------------------
// Core routing
// ---------------------------------------------------------------------------

/**
 * Resolve which gateway IDs to route to for a given set of table names.
 *
 * Returns a deduplicated array of gateway IDs. Tables not matched by any
 * shard entry are routed to the config's default gateway.
 *
 * @param config - The shard configuration.
 * @param tables - Table names to resolve.
 * @returns Deduplicated gateway IDs to route to.
 */
export function resolveShardGatewayIds(config: ShardConfig, tables: string[]): string[] {
	const gatewayIds = new Set<string>();

	for (const table of tables) {
		let matched = false;
		for (const shard of config.shards) {
			if (shard.tables.includes(table)) {
				gatewayIds.add(shard.gatewayId);
				matched = true;
				break;
			}
		}
		if (!matched) {
			gatewayIds.add(config.default);
		}
	}

	// If no tables provided, route to all shards + default
	if (tables.length === 0) {
		for (const shard of config.shards) {
			gatewayIds.add(shard.gatewayId);
		}
		gatewayIds.add(config.default);
	}

	return [...gatewayIds];
}

/**
 * Extract unique table names from an array of deltas.
 *
 * @param deltas - Row deltas to extract table names from.
 * @returns Deduplicated table names.
 */
export function extractTableNames(deltas: RowDelta[]): string[] {
	const tables = new Set<string>();
	for (const delta of deltas) {
		tables.add(delta.table);
	}
	return [...tables];
}

/**
 * Return all unique gateway IDs from the shard config (all shards + default).
 *
 * Used for admin operations that must fan out to every shard.
 *
 * @param config - The shard configuration.
 * @returns All unique gateway IDs.
 */
export function allShardGatewayIds(config: ShardConfig): string[] {
	const ids = new Set<string>();
	for (const shard of config.shards) {
		ids.add(shard.gatewayId);
	}
	ids.add(config.default);
	return [...ids];
}

/**
 * Partition deltas by their target shard gateway ID.
 *
 * @param config - The shard configuration.
 * @param deltas - Deltas to partition.
 * @returns A map from gateway ID to the deltas routed to that shard.
 */
export function partitionDeltasByShard(
	config: ShardConfig,
	deltas: RowDelta[],
): Map<string, RowDelta[]> {
	const partitions = new Map<string, RowDelta[]>();

	for (const delta of deltas) {
		let targetGatewayId = config.default;
		for (const shard of config.shards) {
			if (shard.tables.includes(delta.table)) {
				targetGatewayId = shard.gatewayId;
				break;
			}
		}

		const existing = partitions.get(targetGatewayId);
		if (existing) {
			existing.push(delta);
		} else {
			partitions.set(targetGatewayId, [delta]);
		}
	}

	return partitions;
}

/**
 * Merge multiple pull responses into a single response, sorting deltas by HLC.
 *
 * The merged `serverHlc` is the maximum across all responses. `hasMore` is
 * true if any individual response indicates more data is available.
 *
 * @param responses - Individual shard pull responses to merge.
 * @returns A merged response with deltas sorted by HLC ascending.
 */
export function mergePullResponses(responses: SyncResponse[]): SyncResponse {
	if (responses.length === 0) {
		return { deltas: [], serverHlc: 0n as HLCTimestamp, hasMore: false };
	}

	if (responses.length === 1) {
		return responses[0]!;
	}

	const allDeltas: RowDelta[] = [];
	let maxServerHlc: HLCTimestamp = 0n as HLCTimestamp;
	let hasMore = false;

	for (const response of responses) {
		allDeltas.push(...response.deltas);
		if (response.serverHlc > maxServerHlc) {
			maxServerHlc = response.serverHlc;
		}
		if (response.hasMore) {
			hasMore = true;
		}
	}

	allDeltas.sort((a, b) => (a.hlc < b.hlc ? -1 : a.hlc > b.hlc ? 1 : 0));

	return { deltas: allDeltas, serverHlc: maxServerHlc, hasMore };
}

// ---------------------------------------------------------------------------
// Config parsing
// ---------------------------------------------------------------------------

/**
 * Parse a JSON string into a validated ShardConfig.
 *
 * @param raw - Raw JSON string from the SHARD_CONFIG environment variable.
 * @returns The parsed ShardConfig, or null if invalid.
 */
export function parseShardConfig(raw: string): ShardConfig | null {
	let parsed: unknown;
	try {
		parsed = JSON.parse(raw);
	} catch {
		logger.error("shard_config_parse_error", { reason: "Invalid JSON" });
		return null;
	}

	if (typeof parsed !== "object" || parsed === null) {
		logger.error("shard_config_parse_error", { reason: "Not an object" });
		return null;
	}

	const obj = parsed as Record<string, unknown>;

	if (typeof obj.default !== "string" || obj.default.length === 0) {
		logger.error("shard_config_parse_error", { reason: "Missing or empty 'default' field" });
		return null;
	}

	if (!Array.isArray(obj.shards)) {
		logger.error("shard_config_parse_error", { reason: "Missing 'shards' array" });
		return null;
	}

	for (const shard of obj.shards) {
		if (typeof shard !== "object" || shard === null) {
			logger.error("shard_config_parse_error", { reason: "Shard entry is not an object" });
			return null;
		}
		const s = shard as Record<string, unknown>;
		if (typeof s.gatewayId !== "string" || s.gatewayId.length === 0) {
			logger.error("shard_config_parse_error", {
				reason: "Shard entry missing 'gatewayId'",
			});
			return null;
		}
		if (
			!Array.isArray(s.tables) ||
			s.tables.length === 0 ||
			!s.tables.every((t: unknown) => typeof t === "string")
		) {
			logger.error("shard_config_parse_error", {
				reason: "Shard entry has invalid 'tables' array",
			});
			return null;
		}
	}

	return {
		shards: (obj.shards as Array<{ tables: string[]; gatewayId: string }>).map((s) => ({
			tables: s.tables,
			gatewayId: s.gatewayId,
		})),
		default: obj.default,
	};
}

// ---------------------------------------------------------------------------
// Request handling helpers
// ---------------------------------------------------------------------------

/**
 * Create a DO stub request, forwarding relevant headers.
 *
 * @param baseUrl - Original request URL.
 * @param doPath - Path to set on the DO request.
 * @param method - HTTP method.
 * @param headers - Original request headers.
 * @param body - Optional request body.
 * @returns A new Request targeting the DO.
 */
function createDoRequest(
	baseUrl: string,
	doPath: string,
	method: string,
	headers: Headers,
	body?: ReadableStream<Uint8Array> | string | null,
): Request {
	const url = new URL(baseUrl);
	url.pathname = doPath;
	const doHeaders = new Headers(headers);
	return new Request(url.toString(), {
		method,
		headers: doHeaders,
		body: body ?? undefined,
	});
}

/**
 * Handle a sharded push request.
 *
 * Reads the push body, partitions deltas by shard, and fans out to
 * the appropriate Durable Objects. Returns a merged response.
 *
 * @param config - Shard configuration.
 * @param request - Original push request.
 * @param doNamespace - Durable Object namespace binding.
 * @returns Aggregated push response.
 */
export async function handleShardedPush(
	config: ShardConfig,
	request: Request,
	doNamespace: DurableObjectNamespace,
): Promise<Response> {
	// Read and parse the push body
	const rawBody = await request.text();
	let pushBody: { clientId: string; deltas: RowDelta[]; lastSeenHlc: unknown };
	try {
		pushBody = JSON.parse(rawBody, bigintReviver) as typeof pushBody;
	} catch {
		return new Response(JSON.stringify({ error: "Invalid JSON body" }), {
			status: 400,
			headers: { "Content-Type": "application/json" },
		});
	}

	if (!pushBody.clientId || !Array.isArray(pushBody.deltas)) {
		return new Response(JSON.stringify({ error: "Missing required fields: clientId, deltas" }), {
			status: 400,
			headers: { "Content-Type": "application/json" },
		});
	}

	// Partition deltas by shard
	const partitions = partitionDeltasByShard(config, pushBody.deltas);

	logger.info("sharded_push", {
		clientId: pushBody.clientId,
		totalDeltas: pushBody.deltas.length,
		shardCount: partitions.size,
	});

	// Fan out push to each shard DO
	const responses = await Promise.all(
		[...partitions.entries()].map(async ([gatewayId, deltas]) => {
			const shardPush = {
				clientId: pushBody.clientId,
				deltas,
				lastSeenHlc: pushBody.lastSeenHlc,
			};

			const id = doNamespace.idFromName(gatewayId);
			const stub = doNamespace.get(id);

			const doReq = createDoRequest(
				request.url,
				"/push",
				"POST",
				request.headers,
				JSON.stringify(shardPush, bigintReplacer),
			);

			return stub.fetch(doReq);
		}),
	);

	// Check for errors — return first error encountered
	for (const resp of responses) {
		if (!resp.ok) {
			const body = await resp.text();
			return new Response(body, {
				status: resp.status,
				headers: { "Content-Type": "application/json" },
			});
		}
	}

	// Merge push responses — take the latest serverHlc
	const pushResults = await Promise.all(
		responses.map(
			async (r) => JSON.parse(await r.text(), bigintReviver) as { serverHlc: HLCTimestamp },
		),
	);

	let maxServerHlc: HLCTimestamp = 0n as HLCTimestamp;
	for (const result of pushResults) {
		if (result.serverHlc > maxServerHlc) {
			maxServerHlc = result.serverHlc;
		}
	}
	const totalAccepted = pushBody.deltas.length;

	// Cross-shard broadcast: notify shards that did NOT receive deltas
	// so their WebSocket clients see the updates (fire-and-forget)
	const allIds = allShardGatewayIds(config);
	const broadcastPromises: Promise<Response>[] = [];

	for (const [sourceShardId, shardDeltas] of partitions) {
		if (shardDeltas.length === 0) continue;

		const payload = JSON.stringify(
			{ deltas: shardDeltas, serverHlc: maxServerHlc, excludeClientId: pushBody.clientId },
			bigintReplacer,
		);

		const broadcastUrl = new URL(request.url);
		broadcastUrl.pathname = "/internal/broadcast";
		const urlStr = broadcastUrl.toString();

		for (const targetId of allIds) {
			if (targetId === sourceShardId) continue;
			const stub = doNamespace.get(doNamespace.idFromName(targetId));
			broadcastPromises.push(
				stub
					.fetch(
						new Request(urlStr, {
							method: "POST",
							headers: { "Content-Type": "application/json" },
							body: payload,
						}),
					)
					.catch(() => new Response(null, { status: 500 })),
			);
		}
	}

	void Promise.allSettled(broadcastPromises);

	return new Response(
		JSON.stringify({ serverHlc: maxServerHlc, accepted: totalAccepted }, bigintReplacer),
		{ status: 200, headers: { "Content-Type": "application/json" } },
	);
}

/**
 * Handle a sharded pull request.
 *
 * Fans out the pull to all shards (since the client may need data from
 * any table) and merges the results, sorted by HLC.
 *
 * @param config - Shard configuration.
 * @param request - Original pull request.
 * @param url - Parsed URL of the request.
 * @param doNamespace - Durable Object namespace binding.
 * @returns Merged pull response with deltas sorted by HLC.
 */
export async function handleShardedPull(
	config: ShardConfig,
	request: Request,
	url: URL,
	doNamespace: DurableObjectNamespace,
): Promise<Response> {
	const allGatewayIds = allShardGatewayIds(config);

	logger.info("sharded_pull", { shardCount: allGatewayIds.length });

	// Fan out pull to all shards
	const responses = await Promise.all(
		allGatewayIds.map(async (gatewayId) => {
			const id = doNamespace.idFromName(gatewayId);
			const stub = doNamespace.get(id);

			const doReq = createDoRequest(request.url, `/pull${url.search}`, "GET", request.headers);
			return stub.fetch(doReq);
		}),
	);

	// Collect successful responses and parse them
	const syncResponses: SyncResponse[] = [];
	for (const resp of responses) {
		if (!resp.ok) {
			// Log but continue — partial results are better than total failure
			logger.warn("sharded_pull_shard_error", { status: resp.status });
			continue;
		}
		const body = await resp.text();
		const parsed = JSON.parse(body, bigintReviver) as SyncResponse;
		syncResponses.push(parsed);
	}

	const merged = mergePullResponses(syncResponses);

	return new Response(JSON.stringify(merged, bigintReplacer), {
		status: 200,
		headers: { "Content-Type": "application/json" },
	});
}

/**
 * Handle a sharded admin operation (flush, schema, sync-rules).
 *
 * Fans out the request to all shards and returns success only if all
 * shards respond successfully.
 *
 * @param config - Shard configuration.
 * @param request - Original admin request.
 * @param doPath - Path to forward to each DO.
 * @param method - HTTP method for the DO request.
 * @param doNamespace - Durable Object namespace binding.
 * @param body - Optional request body to forward.
 * @returns Aggregated admin response.
 */
export async function handleShardedAdmin(
	config: ShardConfig,
	request: Request,
	doPath: string,
	method: string,
	doNamespace: DurableObjectNamespace,
	body?: string | null,
): Promise<Response> {
	const allGatewayIds = allShardGatewayIds(config);

	logger.info("sharded_admin", { doPath, shardCount: allGatewayIds.length });

	const responses = await Promise.all(
		allGatewayIds.map(async (gatewayId) => {
			const id = doNamespace.idFromName(gatewayId);
			const stub = doNamespace.get(id);

			const doReq = createDoRequest(request.url, doPath, method, request.headers, body);
			return stub.fetch(doReq);
		}),
	);

	// Return first error if any shard fails
	for (const resp of responses) {
		if (!resp.ok) {
			const errorBody = await resp.text();
			return new Response(errorBody, {
				status: resp.status,
				headers: { "Content-Type": "application/json" },
			});
		}
	}

	return new Response(JSON.stringify({ applied: true, shards: allGatewayIds.length }), {
		status: 200,
		headers: { "Content-Type": "application/json" },
	});
}

/**
 * Handle a sharded checkpoint request.
 *
 * Fans out `GET /checkpoint` to all shards, decodes proto responses,
 * merges deltas sorted by HLC, and re-encodes as a single proto response.
 *
 * @param config - Shard configuration.
 * @param request - Original checkpoint request.
 * @param doNamespace - Durable Object namespace binding.
 * @returns Merged checkpoint response.
 */
export async function handleShardedCheckpoint(
	config: ShardConfig,
	request: Request,
	doNamespace: DurableObjectNamespace,
): Promise<Response> {
	const allGatewayIds = allShardGatewayIds(config);

	logger.info("sharded_checkpoint", { shardCount: allGatewayIds.length });

	// Fan out checkpoint to all shards
	const responses = await Promise.all(
		allGatewayIds.map(async (gatewayId) => {
			const stub = doNamespace.get(doNamespace.idFromName(gatewayId));
			try {
				return await stub.fetch(
					createDoRequest(request.url, "/checkpoint", "GET", request.headers),
				);
			} catch {
				return null;
			}
		}),
	);

	// Collect and merge deltas from all shards
	const allDeltas: RowDelta[] = [];
	let maxCheckpointHlc = 0n;

	for (const resp of responses) {
		if (!resp || !resp.ok) continue;

		const hlcHeader = resp.headers.get("X-Checkpoint-Hlc");
		if (hlcHeader) {
			const hlc = BigInt(hlcHeader);
			if (hlc > maxCheckpointHlc) maxCheckpointHlc = hlc;
		}

		const body = new Uint8Array(await resp.arrayBuffer());
		const decoded = decodeSyncResponse(body);
		if (!decoded.ok) continue;

		allDeltas.push(...decoded.value.deltas);
	}

	allDeltas.sort((a, b) => (a.hlc < b.hlc ? -1 : a.hlc > b.hlc ? 1 : 0));

	const snapshotHlc = maxCheckpointHlc as HLCTimestamp;
	const encoded = encodeSyncResponse({
		deltas: allDeltas,
		serverHlc: snapshotHlc,
		hasMore: false,
	});

	if (!encoded.ok) {
		return new Response(JSON.stringify({ error: "Failed to encode merged checkpoint" }), {
			status: 500,
			headers: { "Content-Type": "application/json" },
		});
	}

	return new Response(encoded.value, {
		status: 200,
		headers: {
			"Content-Type": "application/octet-stream",
			"X-Checkpoint-Hlc": maxCheckpointHlc.toString(),
		},
	});
}
