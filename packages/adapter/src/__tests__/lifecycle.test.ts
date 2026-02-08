import { Err, type HLCTimestamp, Ok, type RowDelta, type TableSchema } from "@lakesync/core";
import { describe, expect, it } from "vitest";

import type { DatabaseAdapter } from "../db-types";
import { LifecycleAdapter, migrateToTier } from "../lifecycle";

/** Encode a wall-clock ms + counter into an HLC timestamp (48-bit wall | 16-bit counter). */
function hlcFromWall(wallMs: number, counter = 0): HLCTimestamp {
	return ((BigInt(wallMs) << 16n) | BigInt(counter & 0xffff)) as HLCTimestamp;
}

function makeDelta(overrides: Partial<RowDelta> & { table: string; hlc: HLCTimestamp }): RowDelta {
	return {
		op: "INSERT",
		rowId: "r1",
		clientId: "c1",
		columns: [],
		deltaId: `delta-${overrides.table}-${overrides.hlc}`,
		...overrides,
	};
}

interface MockAdapter extends DatabaseAdapter {
	insertCalls: RowDelta[][];
	queryCalls: Array<{ hlc: HLCTimestamp; tables?: string[] }>;
	closeCalled: boolean;
	ensureSchemaCalls: TableSchema[];
	getLatestStateCalls: Array<{ table: string; rowId: string }>;
	queryResult: RowDelta[];
	getLatestStateResult: Record<string, unknown> | null;
	insertError: boolean;
}

function createMockAdapter(queryResult: RowDelta[] = []): MockAdapter {
	const mock: MockAdapter = {
		insertCalls: [],
		queryCalls: [],
		closeCalled: false,
		ensureSchemaCalls: [],
		getLatestStateCalls: [],
		queryResult,
		getLatestStateResult: null,
		insertError: false,

		async insertDeltas(deltas: RowDelta[]) {
			mock.insertCalls.push(deltas);
			if (mock.insertError) {
				return Err(new (await import("@lakesync/core")).AdapterError("insert failed"));
			}
			return Ok(undefined);
		},

		async queryDeltasSince(hlcVal: HLCTimestamp, tables?: string[]) {
			mock.queryCalls.push({ hlc: hlcVal, tables });
			return Ok(mock.queryResult);
		},

		async getLatestState(table: string, rowId: string) {
			mock.getLatestStateCalls.push({ table, rowId });
			return Ok(mock.getLatestStateResult);
		},

		async ensureSchema(schema: TableSchema) {
			mock.ensureSchemaCalls.push(schema);
			return Ok(undefined);
		},

		async close() {
			mock.closeCalled = true;
		},
	};
	return mock;
}

describe("LifecycleAdapter", () => {
	const ONE_HOUR_MS = 60 * 60 * 1000;

	it("insertDeltas always writes to hot", async () => {
		const hot = createMockAdapter();
		const cold = createMockAdapter();

		const adapter = new LifecycleAdapter({
			hot: { adapter: hot, maxAgeMs: ONE_HOUR_MS },
			cold: { adapter: cold },
		});

		const deltas = [makeDelta({ table: "users", hlc: hlcFromWall(Date.now()) })];
		const result = await adapter.insertDeltas(deltas);

		expect(result.ok).toBe(true);
		expect(hot.insertCalls).toHaveLength(1);
		expect(hot.insertCalls[0]).toEqual(deltas);
		expect(cold.insertCalls).toHaveLength(0);
	});

	it("queryDeltasSince with recent HLC only queries hot", async () => {
		const recentHlc = hlcFromWall(Date.now() - 1000); // 1 second ago
		const hot = createMockAdapter([makeDelta({ table: "users", hlc: recentHlc })]);
		const cold = createMockAdapter();

		const adapter = new LifecycleAdapter({
			hot: { adapter: hot, maxAgeMs: ONE_HOUR_MS },
			cold: { adapter: cold },
		});

		const result = await adapter.queryDeltasSince(recentHlc);
		expect(result.ok).toBe(true);
		expect(hot.queryCalls).toHaveLength(1);
		expect(cold.queryCalls).toHaveLength(0);
	});

	it("queryDeltasSince with old HLC queries both hot and cold", async () => {
		const oldHlc = hlcFromWall(Date.now() - 2 * ONE_HOUR_MS); // 2 hours ago
		const hotDelta = makeDelta({ table: "users", hlc: hlcFromWall(Date.now() - 1000) });
		const coldDelta = makeDelta({ table: "users", hlc: oldHlc });

		const hot = createMockAdapter([hotDelta]);
		const cold = createMockAdapter([coldDelta]);

		const adapter = new LifecycleAdapter({
			hot: { adapter: hot, maxAgeMs: ONE_HOUR_MS },
			cold: { adapter: cold },
		});

		const result = await adapter.queryDeltasSince(oldHlc);
		expect(result.ok).toBe(true);
		if (!result.ok) return;

		expect(hot.queryCalls).toHaveLength(1);
		expect(cold.queryCalls).toHaveLength(1);
		// Results should be sorted by HLC
		expect(result.value).toHaveLength(2);
		expect(result.value[0]!.hlc).toBe(coldDelta.hlc);
		expect(result.value[1]!.hlc).toBe(hotDelta.hlc);
	});

	it("queryDeltasSince passes tables filter to both adapters", async () => {
		const oldHlc = hlcFromWall(Date.now() - 2 * ONE_HOUR_MS);
		const hot = createMockAdapter();
		const cold = createMockAdapter();

		const adapter = new LifecycleAdapter({
			hot: { adapter: hot, maxAgeMs: ONE_HOUR_MS },
			cold: { adapter: cold },
		});

		await adapter.queryDeltasSince(oldHlc, ["users", "events"]);
		expect(hot.queryCalls[0]!.tables).toEqual(["users", "events"]);
		expect(cold.queryCalls[0]!.tables).toEqual(["users", "events"]);
	});

	it("getLatestState tries hot first, returns hot result if found", async () => {
		const hot = createMockAdapter();
		hot.getLatestStateResult = { name: "Alice" };
		const cold = createMockAdapter();
		cold.getLatestStateResult = { name: "Old Alice" };

		const adapter = new LifecycleAdapter({
			hot: { adapter: hot, maxAgeMs: ONE_HOUR_MS },
			cold: { adapter: cold },
		});

		const result = await adapter.getLatestState("users", "r1");
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.value).toEqual({ name: "Alice" });
		expect(hot.getLatestStateCalls).toHaveLength(1);
		expect(cold.getLatestStateCalls).toHaveLength(0);
	});

	it("getLatestState falls back to cold when hot returns null", async () => {
		const hot = createMockAdapter();
		hot.getLatestStateResult = null;
		const cold = createMockAdapter();
		cold.getLatestStateResult = { name: "Archived Alice" };

		const adapter = new LifecycleAdapter({
			hot: { adapter: hot, maxAgeMs: ONE_HOUR_MS },
			cold: { adapter: cold },
		});

		const result = await adapter.getLatestState("users", "r1");
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.value).toEqual({ name: "Archived Alice" });
		expect(hot.getLatestStateCalls).toHaveLength(1);
		expect(cold.getLatestStateCalls).toHaveLength(1);
	});

	it("ensureSchema applies to both hot and cold", async () => {
		const hot = createMockAdapter();
		const cold = createMockAdapter();

		const adapter = new LifecycleAdapter({
			hot: { adapter: hot, maxAgeMs: ONE_HOUR_MS },
			cold: { adapter: cold },
		});

		const schema: TableSchema = { table: "users", columns: [{ name: "name", type: "string" }] };
		const result = await adapter.ensureSchema(schema);

		expect(result.ok).toBe(true);
		expect(hot.ensureSchemaCalls).toHaveLength(1);
		expect(hot.ensureSchemaCalls[0]).toEqual(schema);
		expect(cold.ensureSchemaCalls).toHaveLength(1);
		expect(cold.ensureSchemaCalls[0]).toEqual(schema);
	});

	it("close closes both adapters", async () => {
		const hot = createMockAdapter();
		const cold = createMockAdapter();

		const adapter = new LifecycleAdapter({
			hot: { adapter: hot, maxAgeMs: ONE_HOUR_MS },
			cold: { adapter: cold },
		});

		await adapter.close();
		expect(hot.closeCalled).toBe(true);
		expect(cold.closeCalled).toBe(true);
	});

	it("queryDeltasSince propagates hot adapter error", async () => {
		const oldHlc = hlcFromWall(Date.now() - 2 * ONE_HOUR_MS);
		const hot = createMockAdapter();
		const cold = createMockAdapter();

		// Override hot to return an error
		hot.queryDeltasSince = async () => {
			const { AdapterError } = await import("@lakesync/core");
			return Err(new AdapterError("hot query failed"));
		};

		const adapter = new LifecycleAdapter({
			hot: { adapter: hot, maxAgeMs: ONE_HOUR_MS },
			cold: { adapter: cold },
		});

		const result = await adapter.queryDeltasSince(oldHlc);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toBe("hot query failed");
		}
	});

	it("queryDeltasSince propagates cold adapter error", async () => {
		const oldHlc = hlcFromWall(Date.now() - 2 * ONE_HOUR_MS);
		const hot = createMockAdapter();
		const cold = createMockAdapter();

		// Override cold to return an error
		cold.queryDeltasSince = async () => {
			const { AdapterError } = await import("@lakesync/core");
			return Err(new AdapterError("cold query failed"));
		};

		const adapter = new LifecycleAdapter({
			hot: { adapter: hot, maxAgeMs: ONE_HOUR_MS },
			cold: { adapter: cold },
		});

		const result = await adapter.queryDeltasSince(oldHlc);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toBe("cold query failed");
		}
	});
});

describe("migrateToTier", () => {
	const ONE_HOUR_MS = 60 * 60 * 1000;

	it("migrates old deltas from hot to cold", async () => {
		const oldHlc = hlcFromWall(Date.now() - 2 * ONE_HOUR_MS);
		const recentHlc = hlcFromWall(Date.now() - 1000);

		const oldDelta = makeDelta({ table: "users", hlc: oldHlc });
		const recentDelta = makeDelta({ table: "users", hlc: recentHlc });

		const hot = createMockAdapter([oldDelta, recentDelta]);
		const cold = createMockAdapter();

		const result = await migrateToTier(hot, cold, ONE_HOUR_MS);
		expect(result.ok).toBe(true);
		if (!result.ok) return;

		expect(result.value.migrated).toBe(1);
		expect(cold.insertCalls).toHaveLength(1);
		expect(cold.insertCalls[0]).toHaveLength(1);
		expect(cold.insertCalls[0]![0]!.hlc).toBe(oldHlc);
	});

	it("returns zero when no deltas are old enough", async () => {
		const recentHlc = hlcFromWall(Date.now() - 1000);
		const recentDelta = makeDelta({ table: "users", hlc: recentHlc });

		const hot = createMockAdapter([recentDelta]);
		const cold = createMockAdapter();

		const result = await migrateToTier(hot, cold, ONE_HOUR_MS);
		expect(result.ok).toBe(true);
		if (!result.ok) return;

		expect(result.value.migrated).toBe(0);
		expect(cold.insertCalls).toHaveLength(0);
	});

	it("propagates hot query error", async () => {
		const hot = createMockAdapter();
		hot.queryDeltasSince = async () => {
			const { AdapterError } = await import("@lakesync/core");
			return Err(new AdapterError("hot read failed"));
		};
		const cold = createMockAdapter();

		const result = await migrateToTier(hot, cold, ONE_HOUR_MS);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toBe("hot read failed");
		}
	});

	it("propagates cold insert error", async () => {
		const oldHlc = hlcFromWall(Date.now() - 2 * ONE_HOUR_MS);
		const hot = createMockAdapter([makeDelta({ table: "users", hlc: oldHlc })]);
		const cold = createMockAdapter();
		cold.insertError = true;

		const result = await migrateToTier(hot, cold, ONE_HOUR_MS);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toBe("insert failed");
		}
	});
});
