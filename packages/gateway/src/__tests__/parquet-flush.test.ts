import type { LakeAdapter } from "@lakesync/adapter";
import { AdapterError, Err, HLC, Ok } from "@lakesync/core";
import type { DeltaOp, HLCTimestamp, Result, RowDelta, TableSchema } from "@lakesync/core";
import { readParquetToDeltas } from "@lakesync/parquet";
import { describe, expect, it } from "vitest";
import { SyncGateway } from "../gateway";
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

const todoSchema: TableSchema = {
	table: "todos",
	columns: [{ name: "title", type: "string" }],
};

const parquetConfig: GatewayConfig = {
	gatewayId: "gw-parquet-1",
	maxBufferBytes: 1_048_576,
	maxBufferAgeMs: 30_000,
	flushFormat: "parquet",
	tableSchema: todoSchema,
};

describe("SyncGateway Parquet flush", () => {
	const hlcLow = HLC.encode(1_000_000, 0);
	const hlcMid = HLC.encode(2_000_000, 0);
	const hlcHigh = HLC.encode(3_000_000, 0);

	it("produces a .parquet object key", async () => {
		const adapter = createMockAdapter();
		const gw = new SyncGateway(parquetConfig, adapter);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-pq-1" });

		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		const result = await gw.flush();
		expect(result.ok).toBe(true);

		// Verify the adapter received exactly one object
		expect(adapter.stored.size).toBe(1);

		// Verify the key ends with .parquet
		const key = [...adapter.stored.keys()][0]!;
		expect(key).toMatch(/^deltas\/\d{4}-\d{2}-\d{2}\/gw-parquet-1\/.+\.parquet$/);

		// Buffer should be empty after successful flush
		expect(gw.bufferStats.logSize).toBe(0);
	});

	it("data can be read back with readParquetToDeltas()", async () => {
		const adapter = createMockAdapter();
		const gw = new SyncGateway(parquetConfig, adapter);

		const d1 = makeDelta({
			hlc: hlcLow,
			rowId: "row-1",
			deltaId: "delta-rt-1",
			columns: [{ column: "title", value: "First" }],
		});
		const d2 = makeDelta({
			hlc: hlcMid,
			rowId: "row-2",
			deltaId: "delta-rt-2",
			columns: [{ column: "title", value: "Second" }],
		});

		gw.handlePush({
			clientId: "client-a",
			deltas: [d1, d2],
			lastSeenHlc: hlcLow,
		});

		const flushResult = await gw.flush();
		expect(flushResult.ok).toBe(true);

		// Read the Parquet data back
		const key = [...adapter.stored.keys()][0]!;
		const parquetBytes = adapter.stored.get(key)!;

		const readResult = await readParquetToDeltas(parquetBytes);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		const restored = readResult.value;
		expect(restored).toHaveLength(2);

		// Verify deltaId equality
		const restoredIds = restored.map((d) => d.deltaId).sort();
		const originalIds = [d1.deltaId, d2.deltaId].sort();
		expect(restoredIds).toEqual(originalIds);
	});

	it("adapter failure restores buffer entries", async () => {
		const adapter = createFailingAdapter();
		const gw = new SyncGateway(parquetConfig, adapter);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-fail-1" });

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

	it("missing tableSchema returns FlushError", async () => {
		const adapter = createMockAdapter();
		const configWithoutSchema: GatewayConfig = {
			gatewayId: "gw-no-schema",
			maxBufferBytes: 1_048_576,
			maxBufferAgeMs: 30_000,
			// No flushFormat specified — defaults to parquet
			// No tableSchema specified
		};
		const gw = new SyncGateway(configWithoutSchema, adapter);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-no-schema" });

		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		const result = await gw.flush();
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("FLUSH_FAILED");
			expect(result.error.message).toContain("tableSchema required");
		}

		// Buffer should be restored
		expect(gw.bufferStats.logSize).toBe(1);
	});
});
