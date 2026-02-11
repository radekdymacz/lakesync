import { Err, type HLCTimestamp, Ok, type RowDelta, type TableSchema } from "@lakesync/core";
import { describe, expect, it } from "vitest";

import type { DatabaseAdapter } from "../db-types";
import { FanOutAdapter } from "../fan-out";
import type { Materialisable } from "../materialise";
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
	ensureSchemaError: boolean;
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
		ensureSchemaError: false,

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
			if (mock.ensureSchemaError) {
				return Err(new (await import("@lakesync/core")).AdapterError("schema failed"));
			}
			return Ok(undefined);
		},

		async close() {
			mock.closeCalled = true;
		},
	};
	return mock;
}

describe("FanOutAdapter", () => {
	it("insertDeltas writes to primary and secondaries", async () => {
		const primary = createMockAdapter();
		const secondaryA = createMockAdapter();
		const secondaryB = createMockAdapter();

		const fanOut = new FanOutAdapter({
			primary,
			secondaries: [secondaryA, secondaryB],
		});

		const deltas = [makeDelta({ table: "users", hlc: hlc(1) })];

		const result = await fanOut.insertDeltas(deltas);
		expect(result.ok).toBe(true);
		expect(primary.insertCalls).toHaveLength(1);
		expect(primary.insertCalls[0]).toEqual(deltas);

		// Allow fire-and-forget to settle
		await new Promise((resolve) => setTimeout(resolve, 10));

		expect(secondaryA.insertCalls).toHaveLength(1);
		expect(secondaryA.insertCalls[0]).toEqual(deltas);
		expect(secondaryB.insertCalls).toHaveLength(1);
		expect(secondaryB.insertCalls[0]).toEqual(deltas);
	});

	it("insertDeltas returns error on primary failure without replicating", async () => {
		const primary = createMockAdapter();
		primary.insertError = true;
		const secondary = createMockAdapter();

		const fanOut = new FanOutAdapter({
			primary,
			secondaries: [secondary],
		});

		const deltas = [makeDelta({ table: "users", hlc: hlc(1) })];
		const result = await fanOut.insertDeltas(deltas);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toBe("insert failed");
		}

		// Allow time to confirm secondaries were not called
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(secondary.insertCalls).toHaveLength(0);
	});

	it("secondary insertDeltas failure does not affect return value", async () => {
		const primary = createMockAdapter();
		const secondary = createMockAdapter();
		secondary.insertError = true;

		const fanOut = new FanOutAdapter({
			primary,
			secondaries: [secondary],
		});

		const deltas = [makeDelta({ table: "users", hlc: hlc(1) })];
		const result = await fanOut.insertDeltas(deltas);

		expect(result.ok).toBe(true);

		// Allow fire-and-forget to settle (error is silently caught)
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(secondary.insertCalls).toHaveLength(1);
	});

	it("queryDeltasSince only hits primary", async () => {
		const primary = createMockAdapter([makeDelta({ table: "users", hlc: hlc(1) })]);
		const secondary = createMockAdapter();

		const fanOut = new FanOutAdapter({
			primary,
			secondaries: [secondary],
		});

		const result = await fanOut.queryDeltasSince(hlc(0));
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toHaveLength(1);
			expect(result.value[0]!.table).toBe("users");
		}
		expect(primary.queryCalls).toHaveLength(1);
		expect(secondary.queryCalls).toHaveLength(0);
	});

	it("getLatestState only hits primary", async () => {
		const primary = createMockAdapter();
		primary.getLatestStateResult = { name: "Alice" };
		const secondary = createMockAdapter();

		const fanOut = new FanOutAdapter({
			primary,
			secondaries: [secondary],
		});

		const result = await fanOut.getLatestState("users", "r1");
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toEqual({ name: "Alice" });
		}
		expect(primary.getLatestStateCalls).toHaveLength(1);
		expect(secondary.getLatestStateCalls).toHaveLength(0);
	});

	it("ensureSchema applies to primary and secondaries", async () => {
		const primary = createMockAdapter();
		const secondaryA = createMockAdapter();
		const secondaryB = createMockAdapter();

		const fanOut = new FanOutAdapter({
			primary,
			secondaries: [secondaryA, secondaryB],
		});

		const schema: TableSchema = { table: "users", columns: [{ name: "name", type: "string" }] };
		const result = await fanOut.ensureSchema(schema);

		expect(result.ok).toBe(true);
		expect(primary.ensureSchemaCalls).toHaveLength(1);
		expect(primary.ensureSchemaCalls[0]).toEqual(schema);

		// Allow fire-and-forget to settle
		await new Promise((resolve) => setTimeout(resolve, 10));

		expect(secondaryA.ensureSchemaCalls).toHaveLength(1);
		expect(secondaryA.ensureSchemaCalls[0]).toEqual(schema);
		expect(secondaryB.ensureSchemaCalls).toHaveLength(1);
		expect(secondaryB.ensureSchemaCalls[0]).toEqual(schema);
	});

	it("ensureSchema secondary failure does not affect return value", async () => {
		const primary = createMockAdapter();
		const secondary = createMockAdapter();
		secondary.ensureSchemaError = true;

		const fanOut = new FanOutAdapter({
			primary,
			secondaries: [secondary],
		});

		const schema: TableSchema = { table: "users", columns: [{ name: "name", type: "string" }] };
		const result = await fanOut.ensureSchema(schema);

		expect(result.ok).toBe(true);

		// Allow fire-and-forget to settle (error is silently caught)
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(secondary.ensureSchemaCalls).toHaveLength(1);
	});

	it("close() closes primary and all secondaries", async () => {
		const primary = createMockAdapter();
		const secondaryA = createMockAdapter();
		const secondaryB = createMockAdapter();

		const fanOut = new FanOutAdapter({
			primary,
			secondaries: [secondaryA, secondaryB],
		});

		await fanOut.close();
		expect(primary.closeCalled).toBe(true);
		expect(secondaryA.closeCalled).toBe(true);
		expect(secondaryB.closeCalled).toBe(true);
	});

	it("works with empty secondaries array", async () => {
		const primary = createMockAdapter();

		const fanOut = new FanOutAdapter({
			primary,
			secondaries: [],
		});

		const deltas = [makeDelta({ table: "users", hlc: hlc(1) })];
		const insertResult = await fanOut.insertDeltas(deltas);
		expect(insertResult.ok).toBe(true);
		expect(primary.insertCalls).toHaveLength(1);

		const schema: TableSchema = { table: "users", columns: [{ name: "name", type: "string" }] };
		const schemaResult = await fanOut.ensureSchema(schema);
		expect(schemaResult.ok).toBe(true);

		await fanOut.close();
		expect(primary.closeCalled).toBe(true);
	});

	describe("materialise", () => {
		it("delegates to primary when materialisable", async () => {
			const primary = createMockAdapter();
			const materialiseCalls: Array<{ deltas: RowDelta[]; schemas: TableSchema[] }> = [];
			(primary as unknown as Materialisable).materialise = async (deltas, schemas) => {
				materialiseCalls.push({ deltas: [...deltas], schemas: [...schemas] });
				return Ok(undefined);
			};
			const secondary = createMockAdapter();

			const fanOut = new FanOutAdapter({ primary, secondaries: [secondary] });
			const deltas = [makeDelta({ table: "users", hlc: hlc(1) })];
			const schemas: TableSchema[] = [
				{ table: "users", columns: [{ name: "name", type: "string" }] },
			];

			const result = await fanOut.materialise(deltas, schemas);
			expect(result.ok).toBe(true);
			expect(materialiseCalls).toHaveLength(1);
			expect(materialiseCalls[0]!.deltas).toEqual(deltas);
		});

		it("no-op when primary is not materialisable", async () => {
			const primary = createMockAdapter();
			const secondary = createMockAdapter();
			const fanOut = new FanOutAdapter({ primary, secondaries: [secondary] });

			const deltas = [makeDelta({ table: "users", hlc: hlc(1) })];
			const schemas: TableSchema[] = [
				{ table: "users", columns: [{ name: "name", type: "string" }] },
			];

			const result = await fanOut.materialise(deltas, schemas);
			expect(result.ok).toBe(true);
		});

		it("delegates to materialisable secondaries (fire-and-forget)", async () => {
			const primary = createMockAdapter();
			(primary as unknown as Materialisable).materialise = async () => Ok(undefined);

			const secondary = createMockAdapter();
			const secCalls: unknown[] = [];
			(secondary as unknown as Materialisable).materialise = async (
				deltas: RowDelta[],
				schemas: ReadonlyArray<TableSchema>,
			) => {
				secCalls.push({ deltas, schemas });
				return Ok(undefined);
			};

			const fanOut = new FanOutAdapter({ primary, secondaries: [secondary] });
			const deltas = [makeDelta({ table: "users", hlc: hlc(1) })];
			const schemas: TableSchema[] = [
				{ table: "users", columns: [{ name: "name", type: "string" }] },
			];

			await fanOut.materialise(deltas, schemas);
			await new Promise((r) => setTimeout(r, 10));
			expect(secCalls).toHaveLength(1);
		});
	});
});
