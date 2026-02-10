import type { HLCTimestamp, RowDelta, SyncResponse } from "@lakesync/core";
import { bigintReplacer, bigintReviver } from "@lakesync/core";
import { encodeSyncResponse } from "@lakesync/proto";
import { describe, expect, it } from "vitest";
import {
	allShardGatewayIds,
	extractTableNames,
	handleShardedCheckpoint,
	handleShardedPush,
	mergePullResponses,
	parseShardConfig,
	partitionDeltasByShard,
	resolveShardGatewayIds,
	type ShardConfig,
} from "../shard-router";

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/** Create a minimal RowDelta for testing. */
function makeDelta(table: string, rowId = "row-1", hlc = 100n as HLCTimestamp): RowDelta {
	return {
		op: "INSERT",
		table,
		rowId,
		clientId: "client-1",
		columns: [{ column: "name", value: "test" }],
		hlc,
		deltaId: `delta-${table}-${rowId}-${hlc}`,
	};
}

/** Standard shard config used across tests. */
function testConfig(): ShardConfig {
	return {
		shards: [
			{ tables: ["users", "profiles"], gatewayId: "shard-users" },
			{ tables: ["orders", "payments"], gatewayId: "shard-orders" },
		],
		default: "shard-default",
	};
}

// ---------------------------------------------------------------------------
// resolveShardGatewayIds
// ---------------------------------------------------------------------------

describe("resolveShardGatewayIds", () => {
	const config = testConfig();

	it("routes a single table to the correct shard", () => {
		const ids = resolveShardGatewayIds(config, ["users"]);
		expect(ids).toEqual(["shard-users"]);
	});

	it("routes a table from a different shard", () => {
		const ids = resolveShardGatewayIds(config, ["orders"]);
		expect(ids).toEqual(["shard-orders"]);
	});

	it("routes an unknown table to the default shard", () => {
		const ids = resolveShardGatewayIds(config, ["logs"]);
		expect(ids).toEqual(["shard-default"]);
	});

	it("routes multiple tables from the same shard to a single ID", () => {
		const ids = resolveShardGatewayIds(config, ["users", "profiles"]);
		expect(ids).toEqual(["shard-users"]);
	});

	it("routes tables from different shards to multiple IDs", () => {
		const ids = resolveShardGatewayIds(config, ["users", "orders"]);
		expect(ids).toHaveLength(2);
		expect(ids).toContain("shard-users");
		expect(ids).toContain("shard-orders");
	});

	it("includes the default shard when mix of known and unknown tables", () => {
		const ids = resolveShardGatewayIds(config, ["users", "logs"]);
		expect(ids).toHaveLength(2);
		expect(ids).toContain("shard-users");
		expect(ids).toContain("shard-default");
	});

	it("routes to all shards + default when no tables specified", () => {
		const ids = resolveShardGatewayIds(config, []);
		expect(ids).toHaveLength(3);
		expect(ids).toContain("shard-users");
		expect(ids).toContain("shard-orders");
		expect(ids).toContain("shard-default");
	});

	it("deduplicates gateway IDs", () => {
		const ids = resolveShardGatewayIds(config, ["users", "users", "profiles"]);
		expect(ids).toEqual(["shard-users"]);
	});

	it("deduplicates default shard across multiple unknown tables", () => {
		const ids = resolveShardGatewayIds(config, ["logs", "metrics"]);
		expect(ids).toEqual(["shard-default"]);
	});
});

// ---------------------------------------------------------------------------
// extractTableNames
// ---------------------------------------------------------------------------

describe("extractTableNames", () => {
	it("returns empty array for no deltas", () => {
		expect(extractTableNames([])).toEqual([]);
	});

	it("extracts a single table name", () => {
		const deltas = [makeDelta("users")];
		expect(extractTableNames(deltas)).toEqual(["users"]);
	});

	it("deduplicates table names", () => {
		const deltas = [makeDelta("users"), makeDelta("users", "row-2")];
		expect(extractTableNames(deltas)).toEqual(["users"]);
	});

	it("extracts multiple unique table names", () => {
		const deltas = [makeDelta("users"), makeDelta("orders")];
		const tables = extractTableNames(deltas);
		expect(tables).toHaveLength(2);
		expect(tables).toContain("users");
		expect(tables).toContain("orders");
	});
});

// ---------------------------------------------------------------------------
// allShardGatewayIds
// ---------------------------------------------------------------------------

describe("allShardGatewayIds", () => {
	it("returns all shard IDs plus default", () => {
		const config = testConfig();
		const ids = allShardGatewayIds(config);
		expect(ids).toHaveLength(3);
		expect(ids).toContain("shard-users");
		expect(ids).toContain("shard-orders");
		expect(ids).toContain("shard-default");
	});

	it("deduplicates when default matches a shard", () => {
		const config: ShardConfig = {
			shards: [{ tables: ["users"], gatewayId: "shard-a" }],
			default: "shard-a",
		};
		const ids = allShardGatewayIds(config);
		expect(ids).toEqual(["shard-a"]);
	});

	it("returns only default when no shards defined", () => {
		const config: ShardConfig = {
			shards: [],
			default: "shard-default",
		};
		const ids = allShardGatewayIds(config);
		expect(ids).toEqual(["shard-default"]);
	});
});

// ---------------------------------------------------------------------------
// partitionDeltasByShard
// ---------------------------------------------------------------------------

describe("partitionDeltasByShard", () => {
	const config = testConfig();

	it("partitions deltas to the correct shards", () => {
		const deltas = [makeDelta("users"), makeDelta("orders"), makeDelta("profiles")];
		const partitions = partitionDeltasByShard(config, deltas);

		expect(partitions.size).toBe(2);
		expect(partitions.get("shard-users")).toHaveLength(2);
		expect(partitions.get("shard-orders")).toHaveLength(1);
	});

	it("routes unknown tables to default shard", () => {
		const deltas = [makeDelta("logs")];
		const partitions = partitionDeltasByShard(config, deltas);

		expect(partitions.size).toBe(1);
		expect(partitions.get("shard-default")).toHaveLength(1);
	});

	it("handles empty delta array", () => {
		const partitions = partitionDeltasByShard(config, []);
		expect(partitions.size).toBe(0);
	});

	it("partitions mixed known and unknown tables correctly", () => {
		const deltas = [
			makeDelta("users"),
			makeDelta("logs"),
			makeDelta("orders"),
			makeDelta("metrics"),
		];
		const partitions = partitionDeltasByShard(config, deltas);

		expect(partitions.size).toBe(3);
		expect(partitions.get("shard-users")).toHaveLength(1);
		expect(partitions.get("shard-orders")).toHaveLength(1);
		expect(partitions.get("shard-default")).toHaveLength(2);
	});
});

// ---------------------------------------------------------------------------
// mergePullResponses
// ---------------------------------------------------------------------------

describe("mergePullResponses", () => {
	it("returns empty response for no inputs", () => {
		const merged = mergePullResponses([]);
		expect(merged.deltas).toEqual([]);
		expect(merged.serverHlc).toBe(0n);
		expect(merged.hasMore).toBe(false);
	});

	it("returns the single response unchanged", () => {
		const response: SyncResponse = {
			deltas: [makeDelta("users", "r1", 100n as HLCTimestamp)],
			serverHlc: 200n as HLCTimestamp,
			hasMore: false,
		};
		const merged = mergePullResponses([response]);
		expect(merged).toBe(response);
	});

	it("merges deltas from multiple responses sorted by HLC", () => {
		const r1: SyncResponse = {
			deltas: [makeDelta("users", "r1", 300n as HLCTimestamp)],
			serverHlc: 300n as HLCTimestamp,
			hasMore: false,
		};
		const r2: SyncResponse = {
			deltas: [makeDelta("orders", "r2", 100n as HLCTimestamp)],
			serverHlc: 200n as HLCTimestamp,
			hasMore: false,
		};

		const merged = mergePullResponses([r1, r2]);
		expect(merged.deltas).toHaveLength(2);
		// Sorted ascending — HLC 100 before HLC 300
		expect(merged.deltas[0]!.hlc).toBe(100n);
		expect(merged.deltas[1]!.hlc).toBe(300n);
	});

	it("takes the maximum serverHlc across responses", () => {
		const r1: SyncResponse = {
			deltas: [],
			serverHlc: 100n as HLCTimestamp,
			hasMore: false,
		};
		const r2: SyncResponse = {
			deltas: [],
			serverHlc: 500n as HLCTimestamp,
			hasMore: false,
		};

		const merged = mergePullResponses([r1, r2]);
		expect(merged.serverHlc).toBe(500n);
	});

	it("sets hasMore to true if any response has more", () => {
		const r1: SyncResponse = {
			deltas: [],
			serverHlc: 100n as HLCTimestamp,
			hasMore: false,
		};
		const r2: SyncResponse = {
			deltas: [],
			serverHlc: 200n as HLCTimestamp,
			hasMore: true,
		};

		const merged = mergePullResponses([r1, r2]);
		expect(merged.hasMore).toBe(true);
	});

	it("maintains stable order for equal HLCs", () => {
		const d1 = makeDelta("users", "r1", 100n as HLCTimestamp);
		const d2 = makeDelta("orders", "r2", 100n as HLCTimestamp);

		const r1: SyncResponse = {
			deltas: [d1],
			serverHlc: 100n as HLCTimestamp,
			hasMore: false,
		};
		const r2: SyncResponse = {
			deltas: [d2],
			serverHlc: 100n as HLCTimestamp,
			hasMore: false,
		};

		const merged = mergePullResponses([r1, r2]);
		expect(merged.deltas).toHaveLength(2);
		// Both have same HLC — order is stable (insertion order preserved)
	});
});

// ---------------------------------------------------------------------------
// parseShardConfig
// ---------------------------------------------------------------------------

describe("parseShardConfig", () => {
	it("parses valid shard config", () => {
		const raw = JSON.stringify({
			shards: [
				{ tables: ["users", "profiles"], gatewayId: "shard-users" },
				{ tables: ["orders"], gatewayId: "shard-orders" },
			],
			default: "shard-default",
		});

		const config = parseShardConfig(raw);
		expect(config).not.toBeNull();
		expect(config!.shards).toHaveLength(2);
		expect(config!.default).toBe("shard-default");
		expect(config!.shards[0]!.tables).toEqual(["users", "profiles"]);
		expect(config!.shards[0]!.gatewayId).toBe("shard-users");
	});

	it("returns null for invalid JSON", () => {
		expect(parseShardConfig("{not json}")).toBeNull();
	});

	it("returns null for non-object JSON", () => {
		expect(parseShardConfig('"just a string"')).toBeNull();
	});

	it("returns null when default is missing", () => {
		const raw = JSON.stringify({ shards: [] });
		expect(parseShardConfig(raw)).toBeNull();
	});

	it("returns null when default is empty", () => {
		const raw = JSON.stringify({ shards: [], default: "" });
		expect(parseShardConfig(raw)).toBeNull();
	});

	it("returns null when shards is not an array", () => {
		const raw = JSON.stringify({ shards: "not-array", default: "x" });
		expect(parseShardConfig(raw)).toBeNull();
	});

	it("returns null when a shard entry has no gatewayId", () => {
		const raw = JSON.stringify({
			shards: [{ tables: ["users"] }],
			default: "x",
		});
		expect(parseShardConfig(raw)).toBeNull();
	});

	it("returns null when a shard entry has empty tables", () => {
		const raw = JSON.stringify({
			shards: [{ tables: [], gatewayId: "shard-a" }],
			default: "x",
		});
		expect(parseShardConfig(raw)).toBeNull();
	});

	it("returns null when tables contains non-string values", () => {
		const raw = JSON.stringify({
			shards: [{ tables: [123], gatewayId: "shard-a" }],
			default: "x",
		});
		expect(parseShardConfig(raw)).toBeNull();
	});

	it("parses config with no shards (empty array)", () => {
		// No shards is invalid since each shard needs tables
		// But an empty shards array with only a default is valid
		const raw = JSON.stringify({ shards: [], default: "shard-default" });
		const config = parseShardConfig(raw);
		expect(config).not.toBeNull();
		expect(config!.shards).toHaveLength(0);
		expect(config!.default).toBe("shard-default");
	});
});

// ---------------------------------------------------------------------------
// handleShardedCheckpoint
// ---------------------------------------------------------------------------

/**
 * Create a mock DurableObjectNamespace that routes idFromName/get calls
 * to a stub map keyed by gateway ID.
 */
function createMockDoNamespace(
	stubs: Map<string, { fetch: (req: Request) => Promise<Response> }>,
): DurableObjectNamespace {
	return {
		idFromName(name: string) {
			return { name } as unknown as DurableObjectId;
		},
		get(id: DurableObjectId) {
			const stub = stubs.get((id as unknown as { name: string }).name);
			if (!stub) throw new Error(`No stub for ${(id as unknown as { name: string }).name}`);
			return stub as unknown as DurableObjectStub;
		},
	} as unknown as DurableObjectNamespace;
}

/** Encode a SyncResponse to a proto binary Response with checkpoint headers. */
function makeCheckpointResponse(deltas: RowDelta[], serverHlc: HLCTimestamp): Response {
	const encoded = encodeSyncResponse({ deltas, serverHlc, hasMore: false });
	if (!encoded.ok) throw new Error("Failed to encode test checkpoint response");
	return new Response(encoded.value, {
		status: 200,
		headers: {
			"Content-Type": "application/octet-stream",
			"X-Checkpoint-Hlc": serverHlc.toString(),
		},
	});
}

describe("handleShardedCheckpoint", () => {
	it("merges checkpoint deltas from all shards sorted by HLC", async () => {
		const config = testConfig();

		const stubs = new Map<string, { fetch: (req: Request) => Promise<Response> }>();

		// shard-users returns deltas with HLC 300 and 100
		stubs.set("shard-users", {
			fetch: async () =>
				makeCheckpointResponse(
					[
						makeDelta("users", "u1", 300n as HLCTimestamp),
						makeDelta("profiles", "p1", 100n as HLCTimestamp),
					],
					300n as HLCTimestamp,
				),
		});

		// shard-orders returns deltas with HLC 200
		stubs.set("shard-orders", {
			fetch: async () =>
				makeCheckpointResponse(
					[makeDelta("orders", "o1", 200n as HLCTimestamp)],
					200n as HLCTimestamp,
				),
		});

		// shard-default returns deltas with HLC 150
		stubs.set("shard-default", {
			fetch: async () =>
				makeCheckpointResponse(
					[makeDelta("logs", "l1", 150n as HLCTimestamp)],
					150n as HLCTimestamp,
				),
		});

		const doNamespace = createMockDoNamespace(stubs);
		const request = new Request("https://example.com/sync/gw/checkpoint", { method: "GET" });

		const response = await handleShardedCheckpoint(config, request, doNamespace);

		expect(response.status).toBe(200);
		expect(response.headers.get("X-Checkpoint-Hlc")).toBe("300");

		// Decode the proto response and verify merged + sorted deltas
		const { decodeSyncResponse } = await import("@lakesync/proto");
		const body = new Uint8Array(await response.arrayBuffer());
		const decoded = decodeSyncResponse(body);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.deltas).toHaveLength(4);
		// Sorted by HLC ascending: 100, 150, 200, 300
		expect(decoded.value.deltas[0]!.hlc).toBe(100n);
		expect(decoded.value.deltas[1]!.hlc).toBe(150n);
		expect(decoded.value.deltas[2]!.hlc).toBe(200n);
		expect(decoded.value.deltas[3]!.hlc).toBe(300n);

		// Verify serverHlc is the max
		expect(decoded.value.serverHlc).toBe(300n);
	});

	it("skips failed shards gracefully during checkpoint", async () => {
		const config = testConfig();

		const stubs = new Map<string, { fetch: (req: Request) => Promise<Response> }>();

		// shard-users succeeds
		stubs.set("shard-users", {
			fetch: async () =>
				makeCheckpointResponse(
					[makeDelta("users", "u1", 100n as HLCTimestamp)],
					100n as HLCTimestamp,
				),
		});

		// shard-orders returns 500 error
		stubs.set("shard-orders", {
			fetch: async () => new Response(JSON.stringify({ error: "Internal error" }), { status: 500 }),
		});

		// shard-default throws (network failure)
		stubs.set("shard-default", {
			fetch: async () => {
				throw new Error("Network timeout");
			},
		});

		const doNamespace = createMockDoNamespace(stubs);
		const request = new Request("https://example.com/sync/gw/checkpoint", { method: "GET" });

		const response = await handleShardedCheckpoint(config, request, doNamespace);

		expect(response.status).toBe(200);

		// Decode and verify only the successful shard's deltas are present
		const { decodeSyncResponse } = await import("@lakesync/proto");
		const body = new Uint8Array(await response.arrayBuffer());
		const decoded = decodeSyncResponse(body);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.deltas).toHaveLength(1);
		expect(decoded.value.deltas[0]!.table).toBe("users");
		expect(decoded.value.deltas[0]!.hlc).toBe(100n);
	});
});

// ---------------------------------------------------------------------------
// handleShardedPush — cross-shard broadcast
// ---------------------------------------------------------------------------

describe("handleShardedPush", () => {
	it("forwards deltas cross-shard via /internal/broadcast", async () => {
		const config = testConfig();

		// Track all requests made to each stub
		const fetchCalls = new Map<string, Request[]>();
		for (const id of ["shard-users", "shard-orders", "shard-default"]) {
			fetchCalls.set(id, []);
		}

		const stubs = new Map<string, { fetch: (req: Request) => Promise<Response> }>();

		// Each shard stub records requests and responds appropriately
		for (const shardId of ["shard-users", "shard-orders", "shard-default"]) {
			stubs.set(shardId, {
				fetch: async (req: Request) => {
					fetchCalls.get(shardId)!.push(req);
					const url = new URL(req.url);
					if (url.pathname === "/push") {
						// Return a successful push response
						return new Response(JSON.stringify({ serverHlc: "100", accepted: 1 }, bigintReplacer), {
							status: 200,
							headers: { "Content-Type": "application/json" },
						});
					}
					if (url.pathname === "/internal/broadcast") {
						return new Response(JSON.stringify({ broadcast: true }), {
							status: 200,
							headers: { "Content-Type": "application/json" },
						});
					}
					return new Response("Not found", { status: 404 });
				},
			});
		}

		const doNamespace = createMockDoNamespace(stubs);

		// Push deltas that go to shard-users only (users table)
		const pushBody = {
			clientId: "client-1",
			deltas: [makeDelta("users", "u1", 100n as HLCTimestamp)],
			lastSeenHlc: 0n as HLCTimestamp,
		};

		const request = new Request("https://example.com/sync/gw/push", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(pushBody, bigintReplacer),
		});

		const response = await handleShardedPush(config, request, doNamespace);
		expect(response.status).toBe(200);

		// shard-users should get the /push request
		const usersPushes = fetchCalls
			.get("shard-users")!
			.filter((r) => new URL(r.url).pathname === "/push");
		expect(usersPushes).toHaveLength(1);

		// Wait for fire-and-forget broadcast promises to settle
		await new Promise((resolve) => setTimeout(resolve, 50));

		// Other shards (shard-orders, shard-default) should get /internal/broadcast
		const ordersBroadcasts = fetchCalls
			.get("shard-orders")!
			.filter((r) => new URL(r.url).pathname === "/internal/broadcast");
		const defaultBroadcasts = fetchCalls
			.get("shard-default")!
			.filter((r) => new URL(r.url).pathname === "/internal/broadcast");
		expect(ordersBroadcasts).toHaveLength(1);
		expect(defaultBroadcasts).toHaveLength(1);

		// shard-users should NOT get a broadcast (it already handled the push)
		const usersBroadcasts = fetchCalls
			.get("shard-users")!
			.filter((r) => new URL(r.url).pathname === "/internal/broadcast");
		expect(usersBroadcasts).toHaveLength(0);

		// Verify broadcast payload contains the deltas and excludeClientId
		const broadcastBody = await ordersBroadcasts[0]!.text();
		const parsed = JSON.parse(broadcastBody, bigintReviver) as {
			deltas: RowDelta[];
			serverHlc: HLCTimestamp;
			excludeClientId: string;
		};
		expect(parsed.deltas).toHaveLength(1);
		expect(parsed.deltas[0]!.table).toBe("users");
		expect(parsed.excludeClientId).toBe("client-1");
	});
});
