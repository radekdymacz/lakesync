import { Err, type HLCTimestamp, Ok, type RowDelta, type TableSchema } from "@lakesync/core";
import { describe, expect, it } from "vitest";

import { CompositeAdapter } from "../composite";
import type { DatabaseAdapter } from "../db-types";
import { hlc } from "./test-helpers";

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

describe("CompositeAdapter", () => {
	it("routes deltas to correct adapter by table", async () => {
		const adapterA = createMockAdapter();
		const adapterB = createMockAdapter();
		const defaultAdapter = createMockAdapter();

		const composite = new CompositeAdapter({
			routes: [
				{ tables: ["users"], adapter: adapterA },
				{ tables: ["events"], adapter: adapterB },
			],
			defaultAdapter,
		});

		const deltas = [
			makeDelta({ table: "users", hlc: hlc(1) }),
			makeDelta({ table: "events", hlc: hlc(2) }),
			makeDelta({ table: "users", hlc: hlc(3) }),
		];

		const result = await composite.insertDeltas(deltas);
		expect(result.ok).toBe(true);
		expect(adapterA.insertCalls).toHaveLength(1);
		expect(adapterA.insertCalls[0]).toHaveLength(2);
		expect(adapterA.insertCalls[0]!.every((d) => d.table === "users")).toBe(true);
		expect(adapterB.insertCalls).toHaveLength(1);
		expect(adapterB.insertCalls[0]).toHaveLength(1);
		expect(adapterB.insertCalls[0]![0]!.table).toBe("events");
		expect(defaultAdapter.insertCalls).toHaveLength(0);
	});

	it("default adapter catches unrouted tables", async () => {
		const adapterA = createMockAdapter();
		const defaultAdapter = createMockAdapter();

		const composite = new CompositeAdapter({
			routes: [{ tables: ["users"], adapter: adapterA }],
			defaultAdapter,
		});

		const deltas = [makeDelta({ table: "unknown_table", hlc: hlc(1) })];

		const result = await composite.insertDeltas(deltas);
		expect(result.ok).toBe(true);
		expect(adapterA.insertCalls).toHaveLength(0);
		expect(defaultAdapter.insertCalls).toHaveLength(1);
		expect(defaultAdapter.insertCalls[0]![0]!.table).toBe("unknown_table");
	});

	it("fan-out query merges and sorts by HLC", async () => {
		const adapterA = createMockAdapter([
			makeDelta({ table: "users", hlc: hlc(3) }),
			makeDelta({ table: "users", hlc: hlc(1) }),
		]);
		const adapterB = createMockAdapter([makeDelta({ table: "events", hlc: hlc(2) })]);
		const defaultAdapter = createMockAdapter();

		const composite = new CompositeAdapter({
			routes: [
				{ tables: ["users"], adapter: adapterA },
				{ tables: ["events"], adapter: adapterB },
			],
			defaultAdapter,
		});

		const result = await composite.queryDeltasSince(hlc(0));
		expect(result.ok).toBe(true);
		if (!result.ok) return;

		expect(result.value).toHaveLength(3);
		expect(result.value[0]!.hlc).toBe(hlc(1));
		expect(result.value[1]!.hlc).toBe(hlc(2));
		expect(result.value[2]!.hlc).toBe(hlc(3));
	});

	it("rejects duplicate table routes", () => {
		const adapterA = createMockAdapter();
		const adapterB = createMockAdapter();
		const defaultAdapter = createMockAdapter();

		expect(
			() =>
				new CompositeAdapter({
					routes: [
						{ tables: ["users"], adapter: adapterA },
						{ tables: ["users", "events"], adapter: adapterB },
					],
					defaultAdapter,
				}),
		).toThrow('Duplicate table route: "users" appears in multiple routes');
	});

	it("getLatestState routes to correct adapter", async () => {
		const adapterA = createMockAdapter();
		adapterA.getLatestStateResult = { name: "Alice" };
		const defaultAdapter = createMockAdapter();
		defaultAdapter.getLatestStateResult = { value: 42 };

		const composite = new CompositeAdapter({
			routes: [{ tables: ["users"], adapter: adapterA }],
			defaultAdapter,
		});

		const resultA = await composite.getLatestState("users", "r1");
		expect(resultA.ok).toBe(true);
		if (resultA.ok) expect(resultA.value).toEqual({ name: "Alice" });
		expect(adapterA.getLatestStateCalls).toHaveLength(1);
		expect(defaultAdapter.getLatestStateCalls).toHaveLength(0);

		const resultB = await composite.getLatestState("other", "r2");
		expect(resultB.ok).toBe(true);
		if (resultB.ok) expect(resultB.value).toEqual({ value: 42 });
		expect(defaultAdapter.getLatestStateCalls).toHaveLength(1);
	});

	it("ensureSchema routes to correct adapter", async () => {
		const adapterA = createMockAdapter();
		const defaultAdapter = createMockAdapter();

		const composite = new CompositeAdapter({
			routes: [{ tables: ["users"], adapter: adapterA }],
			defaultAdapter,
		});

		const schema: TableSchema = { table: "users", columns: [{ name: "name", type: "string" }] };
		const result = await composite.ensureSchema(schema);
		expect(result.ok).toBe(true);
		expect(adapterA.ensureSchemaCalls).toHaveLength(1);
		expect(adapterA.ensureSchemaCalls[0]).toEqual(schema);
		expect(defaultAdapter.ensureSchemaCalls).toHaveLength(0);

		const schema2: TableSchema = { table: "logs", columns: [{ name: "msg", type: "string" }] };
		await composite.ensureSchema(schema2);
		expect(defaultAdapter.ensureSchemaCalls).toHaveLength(1);
		expect(defaultAdapter.ensureSchemaCalls[0]).toEqual(schema2);
	});

	it("close() closes all adapters", async () => {
		const adapterA = createMockAdapter();
		const adapterB = createMockAdapter();
		const defaultAdapter = createMockAdapter();

		const composite = new CompositeAdapter({
			routes: [
				{ tables: ["users"], adapter: adapterA },
				{ tables: ["events"], adapter: adapterB },
			],
			defaultAdapter,
		});

		await composite.close();
		expect(adapterA.closeCalled).toBe(true);
		expect(adapterB.closeCalled).toBe(true);
		expect(defaultAdapter.closeCalled).toBe(true);
	});

	it("close() deduplicates shared adapter instances", async () => {
		const shared = createMockAdapter();
		let closeCount = 0;
		const originalClose = shared.close.bind(shared);
		shared.close = async () => {
			closeCount++;
			await originalClose();
		};

		const composite = new CompositeAdapter({
			routes: [{ tables: ["users"], adapter: shared }],
			defaultAdapter: shared,
		});

		await composite.close();
		expect(closeCount).toBe(1);
	});

	it("insertDeltas fails fast on adapter error", async () => {
		const adapterA = createMockAdapter();
		adapterA.insertError = true;
		const defaultAdapter = createMockAdapter();

		const composite = new CompositeAdapter({
			routes: [{ tables: ["users"], adapter: adapterA }],
			defaultAdapter,
		});

		const deltas = [
			makeDelta({ table: "users", hlc: hlc(1) }),
			makeDelta({ table: "other", hlc: hlc(2) }),
		];

		const result = await composite.insertDeltas(deltas);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toBe("insert failed");
		}
	});

	it("queryDeltasSince with specific tables routes correctly", async () => {
		const adapterA = createMockAdapter([makeDelta({ table: "users", hlc: hlc(1) })]);
		const adapterB = createMockAdapter([makeDelta({ table: "events", hlc: hlc(2) })]);
		const defaultAdapter = createMockAdapter([makeDelta({ table: "logs", hlc: hlc(3) })]);

		const composite = new CompositeAdapter({
			routes: [
				{ tables: ["users"], adapter: adapterA },
				{ tables: ["events"], adapter: adapterB },
			],
			defaultAdapter,
		});

		const result = await composite.queryDeltasSince(hlc(0), ["users", "events"]);
		expect(result.ok).toBe(true);
		if (!result.ok) return;

		expect(result.value).toHaveLength(2);
		expect(adapterA.queryCalls).toHaveLength(1);
		expect(adapterA.queryCalls[0]!.tables).toEqual(["users"]);
		expect(adapterB.queryCalls).toHaveLength(1);
		expect(adapterB.queryCalls[0]!.tables).toEqual(["events"]);
		// Default adapter should NOT be queried
		expect(defaultAdapter.queryCalls).toHaveLength(0);
	});
});
