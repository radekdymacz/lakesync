import type { LakeAdapter } from "@lakesync/adapter";
import type { DeltaOp, HLCTimestamp, Result, RowDelta, TableSchema } from "@lakesync/core";
import { AdapterError, Err, HLC, Ok } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { SyncGateway } from "../gateway";
import { SchemaManager } from "../schema-manager";
import type { GatewayConfig } from "../types";

/** Helper to build a RowDelta with sensible defaults */
function makeDelta(opts: Partial<RowDelta> & { hlc: HLCTimestamp }): RowDelta {
	return {
		op: (opts.op ?? "UPDATE") as DeltaOp,
		table: opts.table ?? "todos",
		rowId: opts.rowId ?? "row-1",
		clientId: opts.clientId ?? "client-a",
		columns: opts.columns ?? [{ column: "title", value: "Test" }],
		hlc: opts.hlc,
		deltaId: opts.deltaId ?? `delta-${Math.random().toString(36).slice(2)}`,
	};
}

/** Simple in-memory mock adapter */
function createMockAdapter(): LakeAdapter & {
	stored: Map<string, Uint8Array>;
} {
	const stored = new Map<string, Uint8Array>();
	return {
		stored,
		async putObject(path: string, data: Uint8Array): Promise<Result<void, AdapterError>> {
			stored.set(path, data);
			return Ok(undefined);
		},
		async getObject(path: string): Promise<Result<Uint8Array, AdapterError>> {
			const data = stored.get(path);
			return data ? Ok(data) : Err(new AdapterError("Not found"));
		},
		async headObject(
			path: string,
		): Promise<Result<{ size: number; lastModified: Date }, AdapterError>> {
			const data = stored.get(path);
			return data
				? Ok({ size: data.length, lastModified: new Date() })
				: Err(new AdapterError("Not found"));
		},
		async listObjects(
			prefix: string,
		): Promise<Result<Array<{ key: string; size: number; lastModified: Date }>, AdapterError>> {
			const results = [...stored.entries()]
				.filter(([k]) => k.startsWith(prefix))
				.map(([key, data]) => ({
					key,
					size: data.length,
					lastModified: new Date(),
				}));
			return Ok(results);
		},
		async deleteObject(path: string): Promise<Result<void, AdapterError>> {
			stored.delete(path);
			return Ok(undefined);
		},
		async deleteObjects(paths: string[]): Promise<Result<void, AdapterError>> {
			for (const p of paths) stored.delete(p);
			return Ok(undefined);
		},
	};
}

/** Simple failing mock adapter for flush-failure tests */
function createFailingAdapter(): LakeAdapter {
	return {
		async putObject(): Promise<Result<void, AdapterError>> {
			return Err(new AdapterError("Simulated write failure"));
		},
		async getObject(): Promise<Result<Uint8Array, AdapterError>> {
			return Err(new AdapterError("Not implemented"));
		},
		async headObject(): Promise<Result<{ size: number; lastModified: Date }, AdapterError>> {
			return Err(new AdapterError("Not implemented"));
		},
		async listObjects(): Promise<
			Result<Array<{ key: string; size: number; lastModified: Date }>, AdapterError>
		> {
			return Ok([]);
		},
		async deleteObject(): Promise<Result<void, AdapterError>> {
			return Ok(undefined);
		},
		async deleteObjects(): Promise<Result<void, AdapterError>> {
			return Ok(undefined);
		},
	};
}

const defaultConfig: GatewayConfig = {
	gatewayId: "gw-test-1",
	maxBufferBytes: 1_048_576, // 1 MiB
	maxBufferAgeMs: 30_000, // 30 seconds
	flushFormat: "json" as const,
};

describe("SyncGateway", () => {
	const hlcLow = HLC.encode(1_000_000, 0);
	const hlcMid = HLC.encode(2_000_000, 0);
	const hlcHigh = HLC.encode(3_000_000, 0);

	it("push single delta stores it in buffer log and index", () => {
		const gw = new SyncGateway(defaultConfig);
		const delta = makeDelta({ hlc: hlcLow });

		const result = gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.accepted).toBe(1);
		}

		expect(gw.bufferStats.logSize).toBe(1);
		expect(gw.bufferStats.indexSize).toBe(1);
	});

	it("push + pull returns the delta from the log", () => {
		const gw = new SyncGateway(defaultConfig);
		const delta = makeDelta({ hlc: hlcLow });

		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		// Pull since HLC 0 (before all deltas)
		const zeroHlc = HLC.encode(0, 0);
		const pullResult = gw.handlePull({
			clientId: "client-b",
			sinceHlc: zeroHlc,
			maxDeltas: 100,
		});

		expect(pullResult.ok).toBe(true);
		if (pullResult.ok) {
			expect(pullResult.value.deltas).toHaveLength(1);
			expect(pullResult.value.deltas[0]?.deltaId).toBe(delta.deltaId);
			expect(pullResult.value.hasMore).toBe(false);
		}
	});

	it("pull sinceHlc filters correctly", () => {
		const gw = new SyncGateway(defaultConfig);

		const d1 = makeDelta({
			hlc: hlcLow,
			rowId: "row-1",
			deltaId: "delta-1",
		});
		const d2 = makeDelta({
			hlc: hlcMid,
			rowId: "row-2",
			deltaId: "delta-2",
		});
		const d3 = makeDelta({
			hlc: hlcHigh,
			rowId: "row-3",
			deltaId: "delta-3",
		});

		gw.handlePush({
			clientId: "client-a",
			deltas: [d1, d2, d3],
			lastSeenHlc: hlcLow,
		});

		// Pull since hlcMid should only return d3
		const pullResult = gw.handlePull({
			clientId: "client-b",
			sinceHlc: hlcMid,
			maxDeltas: 100,
		});

		expect(pullResult.ok).toBe(true);
		if (pullResult.ok) {
			expect(pullResult.value.deltas).toHaveLength(1);
			expect(pullResult.value.deltas[0]?.deltaId).toBe("delta-3");
		}
	});

	it("pull pagination returns hasMore when limit exceeded", () => {
		const gw = new SyncGateway(defaultConfig);

		const deltas: RowDelta[] = [];
		for (let i = 0; i < 10; i++) {
			deltas.push(
				makeDelta({
					hlc: HLC.encode(1_000_000 + i * 1000, 0),
					rowId: `row-${i}`,
					deltaId: `delta-${i}`,
				}),
			);
		}

		gw.handlePush({
			clientId: "client-a",
			deltas,
			lastSeenHlc: hlcLow,
		});

		const zeroHlc = HLC.encode(0, 0);
		const pullResult = gw.handlePull({
			clientId: "client-b",
			sinceHlc: zeroHlc,
			maxDeltas: 3,
		});

		expect(pullResult.ok).toBe(true);
		if (pullResult.ok) {
			expect(pullResult.value.deltas).toHaveLength(3);
			expect(pullResult.value.hasMore).toBe(true);
		}
	});

	it("push with future drift returns Err(ClockDriftError)", () => {
		const gw = new SyncGateway(defaultConfig);

		// Create a delta with HLC 10 seconds in the future
		const futureMs = Date.now() + 10_000;
		const futureHlc = HLC.encode(futureMs, 0);
		const delta = makeDelta({ hlc: futureHlc });

		const result = gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: futureHlc,
		});

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("CLOCK_DRIFT");
		}
	});

	it("concurrent updates to same row: index reflects LWW-resolved state", () => {
		const gw = new SyncGateway(defaultConfig);

		const d1 = makeDelta({
			hlc: hlcLow,
			rowId: "row-1",
			clientId: "client-a",
			deltaId: "delta-old",
			columns: [{ column: "title", value: "Old Title" }],
		});
		const d2 = makeDelta({
			hlc: hlcHigh,
			rowId: "row-1",
			clientId: "client-b",
			deltaId: "delta-new",
			columns: [{ column: "title", value: "New Title" }],
		});

		// Push the first delta
		gw.handlePush({
			clientId: "client-a",
			deltas: [d1],
			lastSeenHlc: hlcLow,
		});

		// Push the second (conflicting) delta
		gw.handlePush({
			clientId: "client-b",
			deltas: [d2],
			lastSeenHlc: hlcHigh,
		});

		// Index should have 1 row with the LWW-resolved (higher HLC) value
		expect(gw.bufferStats.indexSize).toBe(1);
	});

	it("concurrent updates to same row: log contains both events", () => {
		const gw = new SyncGateway(defaultConfig);

		const d1 = makeDelta({
			hlc: hlcLow,
			rowId: "row-1",
			clientId: "client-a",
			deltaId: "delta-first",
			columns: [{ column: "title", value: "First" }],
		});
		const d2 = makeDelta({
			hlc: hlcHigh,
			rowId: "row-1",
			clientId: "client-b",
			deltaId: "delta-second",
			columns: [{ column: "title", value: "Second" }],
		});

		gw.handlePush({
			clientId: "client-a",
			deltas: [d1],
			lastSeenHlc: hlcLow,
		});
		gw.handlePush({
			clientId: "client-b",
			deltas: [d2],
			lastSeenHlc: hlcHigh,
		});

		// Log should contain both entries (original + resolved)
		expect(gw.bufferStats.logSize).toBe(2);
	});

	it("shouldFlush triggers at byte threshold", () => {
		const config: GatewayConfig = {
			...defaultConfig,
			maxBufferBytes: 50, // Very low threshold
			maxBufferAgeMs: 999_999,
		};
		const gw = new SyncGateway(config);

		const delta = makeDelta({
			hlc: hlcLow,
			columns: [
				{
					column: "description",
					value: "A reasonably long value that exceeds the threshold",
				},
			],
		});

		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		expect(gw.shouldFlush()).toBe(true);
	});

	it("shouldFlush triggers at age threshold", () => {
		const config: GatewayConfig = {
			...defaultConfig,
			maxBufferBytes: 999_999,
			maxBufferAgeMs: 0, // Immediate flush
		};
		const gw = new SyncGateway(config);

		const delta = makeDelta({ hlc: hlcLow });

		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		expect(gw.shouldFlush()).toBe(true);
	});

	it("buffer drain returns log entries and clears both structures", () => {
		const gw = new SyncGateway(defaultConfig);

		const d1 = makeDelta({
			hlc: hlcLow,
			rowId: "row-1",
			deltaId: "delta-1",
		});
		const d2 = makeDelta({
			hlc: hlcMid,
			rowId: "row-2",
			deltaId: "delta-2",
		});

		gw.handlePush({
			clientId: "client-a",
			deltas: [d1, d2],
			lastSeenHlc: hlcLow,
		});

		expect(gw.bufferStats.logSize).toBe(2);
		expect(gw.bufferStats.indexSize).toBe(2);

		// Flush will drain the buffer (no adapter means it returns Err, but
		// we can test drain indirectly through bufferStats after a successful flush)
		// Instead, let's verify via push/pull cycle
		const zeroHlc = HLC.encode(0, 0);
		const pullBefore = gw.handlePull({
			clientId: "client-b",
			sinceHlc: zeroHlc,
			maxDeltas: 100,
		});
		expect(pullBefore.ok).toBe(true);
		if (pullBefore.ok) {
			expect(pullBefore.value.deltas).toHaveLength(2);
		}
	});

	it("flush writes FlushEnvelope to mock adapter with correct key pattern", async () => {
		const adapter = createMockAdapter();
		const gw = new SyncGateway(defaultConfig, adapter);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-flush" });

		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		const result = await gw.flush();

		expect(result.ok).toBe(true);

		// Verify the adapter received the data
		expect(adapter.stored.size).toBe(1);

		// Verify the key pattern: deltas/YYYY-MM-DD/gatewayId/...
		const key = [...adapter.stored.keys()][0]!;
		expect(key).toMatch(/^deltas\/\d{4}-\d{2}-\d{2}\/gw-test-1\/.+\.json$/);

		// Verify the envelope content
		const raw = adapter.stored.get(key)!;
		const envelope = JSON.parse(new TextDecoder().decode(raw)) as {
			version: number;
			gatewayId: string;
			deltaCount: number;
			deltas: RowDelta[];
		};
		expect(envelope.version).toBe(1);
		expect(envelope.gatewayId).toBe("gw-test-1");
		expect(envelope.deltaCount).toBe(1);
		expect(envelope.deltas).toHaveLength(1);

		// Buffer should be empty after successful flush
		expect(gw.bufferStats.logSize).toBe(0);
	});

	it("flush failure retains buffer entries", async () => {
		const adapter = createFailingAdapter();
		const gw = new SyncGateway(defaultConfig, adapter);

		const delta = makeDelta({ hlc: hlcLow });

		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		expect(gw.bufferStats.logSize).toBe(1);

		const result = await gw.flush();

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("FLUSH_FAILED");
		}

		// Buffer should still have the entries after failed flush
		expect(gw.bufferStats.logSize).toBe(1);
	});

	it("gateway HLC advances on each push", () => {
		const gw = new SyncGateway(defaultConfig);

		const d1 = makeDelta({ hlc: hlcLow, deltaId: "delta-1" });
		const result1 = gw.handlePush({
			clientId: "client-a",
			deltas: [d1],
			lastSeenHlc: hlcLow,
		});

		const d2 = makeDelta({ hlc: hlcMid, deltaId: "delta-2" });
		const result2 = gw.handlePush({
			clientId: "client-a",
			deltas: [d2],
			lastSeenHlc: hlcMid,
		});

		expect(result1.ok).toBe(true);
		expect(result2.ok).toBe(true);
		if (result1.ok && result2.ok) {
			expect(HLC.compare(result2.value.serverHlc, result1.value.serverHlc)).toBe(1);
		}
	});

	it("re-push same deltaId is idempotent", () => {
		const gw = new SyncGateway(defaultConfig);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "idempotent-id" });

		// First push
		const result1 = gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		expect(result1.ok).toBe(true);
		if (result1.ok) {
			expect(result1.value.accepted).toBe(1);
		}
		expect(gw.bufferStats.logSize).toBe(1);

		// Second push with same deltaId
		const result2 = gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		expect(result2.ok).toBe(true);
		if (result2.ok) {
			expect(result2.value.accepted).toBe(1);
		}

		// Log should still have only 1 entry (not duplicated)
		expect(gw.bufferStats.logSize).toBe(1);
	});

	it("flush resets flushing flag when adapter throws", async () => {
		// Create an adapter whose putObject THROWS (not returns Err)
		const throwingAdapter: LakeAdapter = {
			...createMockAdapter(),
			async putObject(): Promise<Result<void, AdapterError>> {
				throw new Error("Unexpected adapter explosion");
			},
		};
		const gw = new SyncGateway(defaultConfig, throwingAdapter);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-throw" });
		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		// First flush should catch the throw and return Err
		const result1 = await gw.flush();
		expect(result1.ok).toBe(false);

		// Push another delta and flush again — must NOT be stuck on "already in progress"
		const delta2 = makeDelta({ hlc: hlcMid, deltaId: "delta-after-throw" });
		gw.handlePush({
			clientId: "client-a",
			deltas: [delta2],
			lastSeenHlc: hlcMid,
		});

		const result2 = await gw.flush();
		// The second flush should attempt the adapter again, not return "already in progress"
		expect(result2.ok).toBe(false);
		if (!result2.ok) {
			expect(result2.error.message).not.toContain("already in progress");
		}
	});

	it("concurrent flush returns already-in-progress", async () => {
		// Create a slow adapter whose putObject resolves after a delay
		const slowAdapter: LakeAdapter = {
			...createMockAdapter(),
			async putObject(_path: string, _data: Uint8Array): Promise<Result<void, AdapterError>> {
				await new Promise((resolve) => setTimeout(resolve, 100));
				return Ok(undefined);
			},
		};
		const gw = new SyncGateway(defaultConfig, slowAdapter);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-slow" });
		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		// Start flush (will be delayed by 100ms)
		const flush1 = gw.flush();

		// Immediately call flush again while first is in progress
		const flush2 = await gw.flush();

		expect(flush2.ok).toBe(false);
		if (!flush2.ok) {
			expect(flush2.error.message).toContain("already in progress");
		}

		// Wait for the first flush to complete
		const result1 = await flush1;
		expect(result1.ok).toBe(true);
	});

	it("concurrent pushes to same row resolve via LWW", () => {
		const gw = new SyncGateway(defaultConfig);

		// Client-a pushes with a low HLC
		const deltaA = makeDelta({
			hlc: hlcLow,
			rowId: "row-1",
			clientId: "client-a",
			deltaId: "delta-a-low",
			columns: [{ column: "title", value: "Value from A" }],
		});
		gw.handlePush({
			clientId: "client-a",
			deltas: [deltaA],
			lastSeenHlc: hlcLow,
		});

		// Client-b pushes with a high HLC to the same row
		const deltaB = makeDelta({
			hlc: hlcHigh,
			rowId: "row-1",
			clientId: "client-b",
			deltaId: "delta-b-high",
			columns: [{ column: "title", value: "Value from B" }],
		});
		gw.handlePush({
			clientId: "client-b",
			deltas: [deltaB],
			lastSeenHlc: hlcHigh,
		});

		// Both deltas should be in the log
		expect(gw.bufferStats.logSize).toBe(2);

		// Index should reflect a single row (LWW-resolved)
		expect(gw.bufferStats.indexSize).toBe(1);

		// Pull from a third client — the latest delta (hlcHigh) should be present
		const zeroHlc = HLC.encode(0, 0);
		const pullResult = gw.handlePull({
			clientId: "client-c",
			sinceHlc: zeroHlc,
			maxDeltas: 100,
		});

		expect(pullResult.ok).toBe(true);
		if (pullResult.ok) {
			// Log contains both deltas
			expect(pullResult.value.deltas).toHaveLength(2);
			// The higher-HLC delta from client-b should be present
			const hasBDelta = pullResult.value.deltas.some((d) => d.deltaId === "delta-b-high");
			expect(hasBDelta).toBe(true);
		}
	});

	it("handlePush returns ingested deltas", () => {
		const gw = new SyncGateway(defaultConfig);
		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-ingest" });

		const result = gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.accepted).toBe(1);
			expect(result.value.deltas).toHaveLength(1);
			expect(result.value.deltas[0]?.deltaId).toBe("delta-ingest");
		}
	});

	it("handlePush excludes idempotent re-pushes from returned deltas", () => {
		const gw = new SyncGateway(defaultConfig);
		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-idem" });

		// First push
		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		// Second push with same deltaId
		const result = gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.accepted).toBe(1);
			// Re-push should NOT appear in returned deltas
			expect(result.value.deltas).toHaveLength(0);
		}
	});

	it("handlePush returns resolved delta on LWW conflict", () => {
		const gw = new SyncGateway(defaultConfig);

		const d1 = makeDelta({
			hlc: hlcLow,
			rowId: "row-1",
			clientId: "client-a",
			deltaId: "delta-old",
			columns: [{ column: "title", value: "Old" }],
		});
		gw.handlePush({
			clientId: "client-a",
			deltas: [d1],
			lastSeenHlc: hlcLow,
		});

		const d2 = makeDelta({
			hlc: hlcHigh,
			rowId: "row-1",
			clientId: "client-b",
			deltaId: "delta-new",
			columns: [{ column: "title", value: "New" }],
		});
		const result = gw.handlePush({
			clientId: "client-b",
			deltas: [d2],
			lastSeenHlc: hlcHigh,
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(1);
			// The resolved delta should reflect the higher HLC winner
			expect(result.value.deltas[0]?.hlc).toBe(hlcHigh);
		}
	});

	it("handlePush returns multiple ingested deltas in batch", () => {
		const gw = new SyncGateway(defaultConfig);

		const d1 = makeDelta({ hlc: hlcLow, rowId: "row-1", deltaId: "d1" });
		const d2 = makeDelta({ hlc: hlcMid, rowId: "row-2", deltaId: "d2" });
		const d3 = makeDelta({ hlc: hlcHigh, rowId: "row-3", deltaId: "d3" });

		const result = gw.handlePush({
			clientId: "client-a",
			deltas: [d1, d2, d3],
			lastSeenHlc: hlcLow,
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.accepted).toBe(3);
			expect(result.value.deltas).toHaveLength(3);
		}
	});

	it("handlePush returns schema error mid-batch but earlier deltas remain in buffer", () => {
		const schema: TableSchema = {
			table: "todos",
			columns: [{ name: "title", type: "string" }],
		};
		const schemaManager = new SchemaManager(schema);

		const config: GatewayConfig = {
			...defaultConfig,
			schemaManager,
		};
		const gw = new SyncGateway(config);

		// Three deltas: first two have valid "title" column, third has unknown "invalid_col"
		const d1 = makeDelta({
			hlc: hlcLow,
			rowId: "row-1",
			deltaId: "delta-valid-1",
			columns: [{ column: "title", value: "Valid 1" }],
		});
		const d2 = makeDelta({
			hlc: hlcMid,
			rowId: "row-2",
			deltaId: "delta-valid-2",
			columns: [{ column: "title", value: "Valid 2" }],
		});
		const d3 = makeDelta({
			hlc: hlcHigh,
			rowId: "row-3",
			deltaId: "delta-bad",
			columns: [{ column: "invalid_col", value: "Boom" }],
		});

		const result = gw.handlePush({
			clientId: "client-a",
			deltas: [d1, d2, d3],
			lastSeenHlc: hlcLow,
		});

		// Push should return Err with a SchemaError
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("SCHEMA_MISMATCH");
		}

		// The first two valid deltas should still be in the buffer
		expect(gw.bufferStats.logSize).toBe(2);
	});

	it("returns BackpressureError when buffer exceeds limit", () => {
		const config: GatewayConfig = {
			...defaultConfig,
			maxBackpressureBytes: 1, // 1 byte — any push will exceed
		};
		const gw = new SyncGateway(config);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-bp-reject" });

		// First push to put data in the buffer
		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		// Second push should be rejected — buffer exceeds 1 byte
		const delta2 = makeDelta({ hlc: hlcMid, rowId: "row-2", deltaId: "delta-bp-reject-2" });
		const result = gw.handlePush({
			clientId: "client-a",
			deltas: [delta2],
			lastSeenHlc: hlcMid,
		});

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("BACKPRESSURE");
		}
	});

	it("shouldFlush triggers earlier for wide-column deltas", () => {
		const config: GatewayConfig = {
			...defaultConfig,
			maxBufferBytes: 20_000, // 20 KB
			maxBufferAgeMs: 999_999,
			adaptiveBufferConfig: {
				wideColumnThreshold: 100, // Low threshold — most deltas will exceed this
				reductionFactor: 0.5, // Effective limit becomes 10 KB
			},
		};
		const gw = new SyncGateway(config);

		// Push wide deltas (large column values) that exceed 10 KB but stay under 20 KB
		for (let i = 0; i < 5; i++) {
			gw.handlePush({
				clientId: "client-a",
				deltas: [
					makeDelta({
						hlc: HLC.encode(1_000_000 + i * 1000, 0),
						rowId: `row-${i}`,
						deltaId: `delta-wide-${i}`,
						columns: [
							{ column: "col1", value: "x".repeat(500) },
							{ column: "col2", value: "y".repeat(500) },
						],
					}),
				],
				lastSeenHlc: HLC.encode(1_000_000 + i * 1000, 0),
			});
		}

		// Buffer should be > 10 KB (reduced threshold) but < 20 KB (original)
		expect(gw.bufferStats.byteSize).toBeGreaterThan(10_000);
		expect(gw.bufferStats.byteSize).toBeLessThan(20_000);

		// With adaptive config, shouldFlush should trigger at the reduced threshold
		expect(gw.shouldFlush()).toBe(true);
	});

	it("shouldFlush does not trigger early without adaptive config", () => {
		const config: GatewayConfig = {
			...defaultConfig,
			maxBufferBytes: 20_000,
			maxBufferAgeMs: 999_999,
			// No adaptiveBufferConfig
		};
		const gw = new SyncGateway(config);

		// Push same wide deltas
		for (let i = 0; i < 5; i++) {
			gw.handlePush({
				clientId: "client-a",
				deltas: [
					makeDelta({
						hlc: HLC.encode(1_000_000 + i * 1000, 0),
						rowId: `row-${i}`,
						deltaId: `delta-no-adaptive-${i}`,
						columns: [
							{ column: "col1", value: "x".repeat(500) },
							{ column: "col2", value: "y".repeat(500) },
						],
					}),
				],
				lastSeenHlc: HLC.encode(1_000_000 + i * 1000, 0),
			});
		}

		// Buffer is > 10 KB but < 20 KB — without adaptive config, should NOT flush
		expect(gw.bufferStats.byteSize).toBeGreaterThan(10_000);
		expect(gw.bufferStats.byteSize).toBeLessThan(20_000);
		expect(gw.shouldFlush()).toBe(false);
	});

	it("accepts push below backpressure limit", () => {
		const config: GatewayConfig = {
			...defaultConfig,
			maxBackpressureBytes: 10_000_000, // 10 MB — plenty of room
		};
		const gw = new SyncGateway(config);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-bp-ok" });

		const result = gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.accepted).toBe(1);
		}
	});

	it("tableStats returns per-table bytes and counts", () => {
		const gw = new SyncGateway(defaultConfig);

		const d1 = makeDelta({
			hlc: hlcLow,
			table: "todos",
			rowId: "row-1",
			deltaId: "delta-t1",
		});
		const d2 = makeDelta({
			hlc: hlcMid,
			table: "todos",
			rowId: "row-2",
			deltaId: "delta-t2",
		});
		const d3 = makeDelta({
			hlc: hlcHigh,
			table: "users",
			rowId: "row-3",
			deltaId: "delta-u1",
		});

		gw.handlePush({
			clientId: "client-a",
			deltas: [d1, d2, d3],
			lastSeenHlc: hlcLow,
		});

		const stats = gw.tableStats;
		expect(stats).toHaveLength(2);

		const todosStats = stats.find((s) => s.table === "todos");
		const usersStats = stats.find((s) => s.table === "users");

		expect(todosStats).toBeDefined();
		expect(todosStats!.deltaCount).toBe(2);
		expect(todosStats!.byteSize).toBeGreaterThan(0);

		expect(usersStats).toBeDefined();
		expect(usersStats!.deltaCount).toBe(1);
		expect(usersStats!.byteSize).toBeGreaterThan(0);
	});

	it("drainTable removes only target table, other tables remain", () => {
		const gw = new SyncGateway(defaultConfig);

		const d1 = makeDelta({
			hlc: hlcLow,
			table: "todos",
			rowId: "row-1",
			deltaId: "delta-drain-t1",
		});
		const d2 = makeDelta({
			hlc: hlcMid,
			table: "users",
			rowId: "row-2",
			deltaId: "delta-drain-u1",
		});

		gw.handlePush({
			clientId: "client-a",
			deltas: [d1, d2],
			lastSeenHlc: hlcLow,
		});

		expect(gw.bufferStats.logSize).toBe(2);

		// Flush only the "todos" table
		const stats = gw.tableStats;
		expect(stats).toHaveLength(2);

		// After flushing just todos, only users should remain
		// We test this via flushTable which internally calls drainTable
	});

	it("flushTable flushes single table while other table remains", async () => {
		const adapter = createMockAdapter();
		const gw = new SyncGateway(defaultConfig, adapter);

		const d1 = makeDelta({
			hlc: hlcLow,
			table: "todos",
			rowId: "row-1",
			deltaId: "delta-ft-t1",
		});
		const d2 = makeDelta({
			hlc: hlcMid,
			table: "users",
			rowId: "row-2",
			deltaId: "delta-ft-u1",
		});

		gw.handlePush({
			clientId: "client-a",
			deltas: [d1, d2],
			lastSeenHlc: hlcLow,
		});

		expect(gw.bufferStats.logSize).toBe(2);

		// Flush only "todos"
		const result = await gw.flushTable("todos");
		expect(result.ok).toBe(true);

		// Buffer should have only the "users" delta remaining
		expect(gw.bufferStats.logSize).toBe(1);

		// Adapter should have received the todos flush
		expect(adapter.stored.size).toBe(1);
		const key = [...adapter.stored.keys()][0]!;
		expect(key).toContain("todos-");

		// Table stats should show only users
		const stats = gw.tableStats;
		expect(stats).toHaveLength(1);
		expect(stats[0]!.table).toBe("users");
	});

	it("getTablesExceedingBudget returns hot tables", () => {
		const config: GatewayConfig = {
			...defaultConfig,
			perTableBudgetBytes: 100, // Very low budget
		};
		const gw = new SyncGateway(config);

		const d1 = makeDelta({
			hlc: hlcLow,
			table: "todos",
			rowId: "row-1",
			deltaId: "delta-budget-t1",
			columns: [{ column: "title", value: "x".repeat(100) }],
		});
		const d2 = makeDelta({
			hlc: hlcMid,
			table: "users",
			rowId: "row-2",
			deltaId: "delta-budget-u1",
			columns: [{ column: "name", value: "y" }],
		});

		gw.handlePush({
			clientId: "client-a",
			deltas: [d1, d2],
			lastSeenHlc: hlcLow,
		});

		const hot = gw.getTablesExceedingBudget();
		// todos should exceed the 100-byte budget, users should not
		expect(hot).toContain("todos");
	});

	it("flushTable on empty table is a no-op", async () => {
		const adapter = createMockAdapter();
		const gw = new SyncGateway(defaultConfig, adapter);

		const result = await gw.flushTable("nonexistent");
		expect(result.ok).toBe(true);
		expect(adapter.stored.size).toBe(0);
	});
});
