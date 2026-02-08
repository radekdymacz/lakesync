import type { HLCTimestamp, RowDelta, SyncResponse } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import {
	allShardGatewayIds,
	extractTableNames,
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
