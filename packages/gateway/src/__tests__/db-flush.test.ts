import type { DatabaseAdapter } from "@lakesync/adapter";
import type { AdapterError, DeltaOp, HLCTimestamp, Result, RowDelta } from "@lakesync/core";
import { Err, HLC, Ok } from "@lakesync/core";
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

/** In-memory mock DatabaseAdapter that records calls */
function createMockDbAdapter(): DatabaseAdapter & { calls: RowDelta[][] } {
	const calls: RowDelta[][] = [];
	return {
		calls,
		async insertDeltas(deltas) {
			calls.push([...deltas]);
			return Ok(undefined);
		},
		async queryDeltasSince() {
			return Ok([]);
		},
		async getLatestState() {
			return Ok(null);
		},
		async ensureSchema() {
			return Ok(undefined);
		},
		async close() {},
	};
}

/** Mock DatabaseAdapter that always fails on insertDeltas */
function createFailingDbAdapter(): DatabaseAdapter {
	return {
		async insertDeltas(): Promise<Result<void, AdapterError>> {
			return Err({ code: "ADAPTER_ERROR", message: "Simulated DB write failure" } as AdapterError);
		},
		async queryDeltasSince() {
			return Ok([]);
		},
		async getLatestState() {
			return Ok(null);
		},
		async ensureSchema() {
			return Ok(undefined);
		},
		async close() {},
	};
}

const defaultConfig: GatewayConfig = {
	gatewayId: "gw-db-test",
	maxBufferBytes: 1_048_576,
	maxBufferAgeMs: 30_000,
};

describe("SyncGateway with DatabaseAdapter", () => {
	const hlcLow = HLC.encode(1_000_000, 0);
	const hlcMid = HLC.encode(2_000_000, 0);

	it("flush with DatabaseAdapter calls insertDeltas with correct deltas", async () => {
		const dbAdapter = createMockDbAdapter();
		const gw = new SyncGateway(defaultConfig, dbAdapter);

		const d1 = makeDelta({ hlc: hlcLow, rowId: "row-1", deltaId: "delta-db-1" });
		const d2 = makeDelta({ hlc: hlcMid, rowId: "row-2", deltaId: "delta-db-2" });

		gw.handlePush({
			clientId: "client-a",
			deltas: [d1, d2],
			lastSeenHlc: hlcLow,
		});

		const result = await gw.flush();

		expect(result.ok).toBe(true);
		expect(dbAdapter.calls).toHaveLength(1);
		expect(dbAdapter.calls[0]).toHaveLength(2);
		expect(dbAdapter.calls[0]![0]!.deltaId).toBe("delta-db-1");
		expect(dbAdapter.calls[0]![1]!.deltaId).toBe("delta-db-2");
	});

	it("buffer is drained after successful DB flush", async () => {
		const dbAdapter = createMockDbAdapter();
		const gw = new SyncGateway(defaultConfig, dbAdapter);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-drain" });
		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		expect(gw.bufferStats.logSize).toBe(1);

		const result = await gw.flush();
		expect(result.ok).toBe(true);
		expect(gw.bufferStats.logSize).toBe(0);
	});

	it("failed DB flush restores buffer", async () => {
		const dbAdapter = createFailingDbAdapter();
		const gw = new SyncGateway(defaultConfig, dbAdapter);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-fail" });
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
			expect(result.error.message).toContain("Database flush failed");
		}

		// Buffer should be restored after failure
		expect(gw.bufferStats.logSize).toBe(1);
	});

	it("shouldFlush works with DB adapter", () => {
		const config: GatewayConfig = {
			...defaultConfig,
			maxBufferBytes: 50, // Very low threshold
			maxBufferAgeMs: 999_999,
		};
		const dbAdapter = createMockDbAdapter();
		const gw = new SyncGateway(config, dbAdapter);

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

	it("empty buffer DB flush is no-op", async () => {
		const dbAdapter = createMockDbAdapter();
		const gw = new SyncGateway(defaultConfig, dbAdapter);

		const result = await gw.flush();
		expect(result.ok).toBe(true);
		expect(dbAdapter.calls).toHaveLength(0);
	});
});
