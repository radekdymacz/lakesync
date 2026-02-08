import { PostgresAdapter } from "@lakesync/adapter";
import type { HLCTimestamp, RowDelta, TableSchema } from "@lakesync/core";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

const POSTGRES_URL = process.env.POSTGRES_URL;
const canRun = !!POSTGRES_URL;

/** Helper to create a RowDelta for testing. */
function makeDelta(overrides: Partial<RowDelta> = {}): RowDelta {
	return {
		deltaId: `delta-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
		table: "todos",
		rowId: "row-1",
		clientId: "client-a",
		columns: [{ column: "title", value: "Buy milk" }],
		hlc: BigInt(1000) as HLCTimestamp,
		op: "INSERT",
		...overrides,
	};
}

describe.skipIf(!canRun)("PostgresAdapter integration", () => {
	let adapter: PostgresAdapter;

	const schema: TableSchema = {
		table: "todos",
		columns: [
			{ name: "title", type: "string" },
			{ name: "done", type: "boolean" },
		],
	};

	beforeAll(async () => {
		adapter = new PostgresAdapter({ connectionString: POSTGRES_URL! });
		const result = await adapter.ensureSchema(schema);
		expect(result.ok).toBe(true);
	});

	beforeEach(async () => {
		// Clean the table between tests
		await adapter.pool.query("DELETE FROM lakesync_deltas");
	});

	afterAll(async () => {
		// Drop test table and close connection
		await adapter.pool.query("DROP TABLE IF EXISTS lakesync_deltas");
		await adapter.close();
	});

	it("ensureSchema creates tables and indices", async () => {
		// Table already created in beforeAll; verify it exists
		const result = await adapter.pool.query(
			"SELECT to_regclass('public.lakesync_deltas') AS exists",
		);
		expect(result.rows[0]!.exists).not.toBeNull();
	});

	it("insertDeltas + queryDeltasSince roundtrip", async () => {
		const delta = makeDelta({
			deltaId: "roundtrip-1",
			hlc: BigInt(5000) as HLCTimestamp,
		});

		const insertResult = await adapter.insertDeltas([delta]);
		expect(insertResult.ok).toBe(true);

		const queryResult = await adapter.queryDeltasSince(BigInt(0) as HLCTimestamp);
		expect(queryResult.ok).toBe(true);
		if (queryResult.ok) {
			expect(queryResult.value).toHaveLength(1);
			const retrieved = queryResult.value[0]!;
			expect(retrieved.deltaId).toBe("roundtrip-1");
			expect(retrieved.table).toBe("todos");
			expect(retrieved.rowId).toBe("row-1");
			expect(retrieved.hlc).toBe(BigInt(5000));
			expect(retrieved.clientId).toBe("client-a");
			expect(retrieved.op).toBe("INSERT");
			expect(retrieved.columns).toEqual([{ column: "title", value: "Buy milk" }]);
		}
	});

	it("idempotent insert (same deltaId twice)", async () => {
		const delta = makeDelta({ deltaId: "idempotent-1" });

		const first = await adapter.insertDeltas([delta]);
		expect(first.ok).toBe(true);

		const second = await adapter.insertDeltas([delta]);
		expect(second.ok).toBe(true);

		const queryResult = await adapter.queryDeltasSince(BigInt(0) as HLCTimestamp);
		expect(queryResult.ok).toBe(true);
		if (queryResult.ok) {
			expect(queryResult.value).toHaveLength(1);
		}
	});

	it("queryDeltasSince filters by HLC", async () => {
		const deltas = [
			makeDelta({ deltaId: "hlc-1", hlc: BigInt(100) as HLCTimestamp }),
			makeDelta({ deltaId: "hlc-2", hlc: BigInt(200) as HLCTimestamp }),
			makeDelta({ deltaId: "hlc-3", hlc: BigInt(300) as HLCTimestamp }),
		];

		await adapter.insertDeltas(deltas);

		const result = await adapter.queryDeltasSince(BigInt(150) as HLCTimestamp);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toHaveLength(2);
			expect(result.value[0]!.deltaId).toBe("hlc-2");
			expect(result.value[1]!.deltaId).toBe("hlc-3");
		}
	});

	it("queryDeltasSince filters by table", async () => {
		const deltas = [
			makeDelta({
				deltaId: "tbl-1",
				table: "todos",
				hlc: BigInt(100) as HLCTimestamp,
			}),
			makeDelta({
				deltaId: "tbl-2",
				table: "users",
				hlc: BigInt(200) as HLCTimestamp,
			}),
			makeDelta({
				deltaId: "tbl-3",
				table: "todos",
				hlc: BigInt(300) as HLCTimestamp,
			}),
		];

		await adapter.insertDeltas(deltas);

		const result = await adapter.queryDeltasSince(BigInt(0) as HLCTimestamp, ["todos"]);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toHaveLength(2);
			expect(result.value.every((d) => d.table === "todos")).toBe(true);
		}
	});

	it("getLatestState returns LWW-merged state", async () => {
		const deltas = [
			makeDelta({
				deltaId: "state-1",
				rowId: "merge-row",
				hlc: BigInt(100) as HLCTimestamp,
				columns: [
					{ column: "title", value: "Original" },
					{ column: "done", value: false },
				],
			}),
			makeDelta({
				deltaId: "state-2",
				rowId: "merge-row",
				hlc: BigInt(200) as HLCTimestamp,
				op: "UPDATE",
				columns: [{ column: "title", value: "Updated" }],
			}),
		];

		await adapter.insertDeltas(deltas);

		const result = await adapter.getLatestState("todos", "merge-row");
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toEqual({ title: "Updated", done: false });
		}
	});

	it("getLatestState returns null for nonexistent row", async () => {
		const result = await adapter.getLatestState("todos", "nonexistent");
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toBeNull();
		}
	});

	it("getLatestState returns null after DELETE", async () => {
		const deltas = [
			makeDelta({
				deltaId: "del-1",
				rowId: "del-row",
				hlc: BigInt(100) as HLCTimestamp,
				columns: [{ column: "title", value: "Doomed" }],
			}),
			makeDelta({
				deltaId: "del-2",
				rowId: "del-row",
				hlc: BigInt(200) as HLCTimestamp,
				op: "DELETE",
				columns: [],
			}),
		];

		await adapter.insertDeltas(deltas);

		const result = await adapter.getLatestState("todos", "del-row");
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toBeNull();
		}
	});

	it("multiple tables work independently", async () => {
		const deltas = [
			makeDelta({
				deltaId: "multi-1",
				table: "todos",
				rowId: "r1",
				hlc: BigInt(100) as HLCTimestamp,
				columns: [{ column: "title", value: "Todo item" }],
			}),
			makeDelta({
				deltaId: "multi-2",
				table: "users",
				rowId: "r1",
				hlc: BigInt(200) as HLCTimestamp,
				columns: [{ column: "title", value: "User item" }],
			}),
		];

		await adapter.insertDeltas(deltas);

		const todoState = await adapter.getLatestState("todos", "r1");
		const userState = await adapter.getLatestState("users", "r1");

		expect(todoState.ok).toBe(true);
		expect(userState.ok).toBe(true);
		if (todoState.ok && userState.ok) {
			expect(todoState.value).toEqual({ title: "Todo item" });
			expect(userState.value).toEqual({ title: "User item" });
		}
	});
});
