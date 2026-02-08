import { MySQLAdapter } from "@lakesync/adapter";
import type { HLCTimestamp, RowDelta, TableSchema } from "@lakesync/core";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

const MYSQL_URL = process.env.MYSQL_URL;
const canRun = !!MYSQL_URL;

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

describe.skipIf(!canRun)("MySQLAdapter integration", () => {
	let adapter: MySQLAdapter;

	beforeAll(async () => {
		adapter = new MySQLAdapter({ connectionString: MYSQL_URL! });

		// Ensure the deltas table exists
		const schema: TableSchema = {
			table: "test_table",
			columns: [{ name: "title", type: "string" }],
		};
		const result = await adapter.ensureSchema(schema);
		expect(result.ok).toBe(true);
	});

	beforeEach(async () => {
		// Clean slate for each test
		await adapter.pool.execute("DELETE FROM lakesync_deltas");
	});

	afterAll(async () => {
		// Clean up
		await adapter.pool.execute("DROP TABLE IF EXISTS lakesync_deltas");
		await adapter.pool.execute("DROP TABLE IF EXISTS test_table");
		await adapter.close();
	});

	it("roundtrip: insertDeltas then queryDeltasSince", async () => {
		const delta = makeDelta({
			deltaId: "rt-1",
			hlc: BigInt(5000) as HLCTimestamp,
		});

		const insertResult = await adapter.insertDeltas([delta]);
		expect(insertResult.ok).toBe(true);

		const queryResult = await adapter.queryDeltasSince(BigInt(0) as HLCTimestamp);
		expect(queryResult.ok).toBe(true);
		if (queryResult.ok) {
			expect(queryResult.value).toHaveLength(1);
			const found = queryResult.value[0]!;
			expect(found.deltaId).toBe("rt-1");
			expect(found.table).toBe("todos");
			expect(found.rowId).toBe("row-1");
			expect(found.hlc).toBe(BigInt(5000));
			expect(found.clientId).toBe("client-a");
			expect(found.op).toBe("INSERT");
			expect(found.columns).toEqual([{ column: "title", value: "Buy milk" }]);
		}
	});

	it("idempotent insert: duplicate deltaId is silently ignored", async () => {
		const delta = makeDelta({ deltaId: "idem-1" });

		await adapter.insertDeltas([delta]);
		const result = await adapter.insertDeltas([delta]);
		expect(result.ok).toBe(true);

		const queryResult = await adapter.queryDeltasSince(BigInt(0) as HLCTimestamp);
		expect(queryResult.ok).toBe(true);
		if (queryResult.ok) {
			expect(queryResult.value).toHaveLength(1);
		}
	});

	it("queryDeltasSince filters by HLC", async () => {
		const d1 = makeDelta({
			deltaId: "hlc-1",
			hlc: BigInt(1000) as HLCTimestamp,
		});
		const d2 = makeDelta({
			deltaId: "hlc-2",
			hlc: BigInt(2000) as HLCTimestamp,
		});
		const d3 = makeDelta({
			deltaId: "hlc-3",
			hlc: BigInt(3000) as HLCTimestamp,
		});

		await adapter.insertDeltas([d1, d2, d3]);

		const result = await adapter.queryDeltasSince(BigInt(1500) as HLCTimestamp);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toHaveLength(2);
			expect(result.value.map((d) => d.deltaId)).toEqual(["hlc-2", "hlc-3"]);
		}
	});

	it("queryDeltasSince filters by table", async () => {
		const d1 = makeDelta({
			deltaId: "tbl-1",
			table: "todos",
			hlc: BigInt(1000) as HLCTimestamp,
		});
		const d2 = makeDelta({
			deltaId: "tbl-2",
			table: "users",
			hlc: BigInt(2000) as HLCTimestamp,
		});

		await adapter.insertDeltas([d1, d2]);

		const result = await adapter.queryDeltasSince(BigInt(0) as HLCTimestamp, ["users"]);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toHaveLength(1);
			expect(result.value[0]!.table).toBe("users");
		}
	});

	it("getLatestState merges LWW columns correctly", async () => {
		const d1 = makeDelta({
			deltaId: "lww-1",
			columns: [
				{ column: "title", value: "Old title" },
				{ column: "done", value: false },
			],
			hlc: BigInt(1000) as HLCTimestamp,
		});
		const d2 = makeDelta({
			deltaId: "lww-2",
			columns: [{ column: "title", value: "New title" }],
			hlc: BigInt(2000) as HLCTimestamp,
			op: "UPDATE",
		});

		await adapter.insertDeltas([d1, d2]);

		const result = await adapter.getLatestState("todos", "row-1");
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toEqual({
				title: "New title",
				done: false,
			});
		}
	});

	it("getLatestState returns null for nonexistent row", async () => {
		const result = await adapter.getLatestState("todos", "nonexistent");
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toBeNull();
		}
	});

	it("getLatestState handles DELETE tombstone", async () => {
		const d1 = makeDelta({
			deltaId: "del-1",
			columns: [{ column: "title", value: "Doomed" }],
			hlc: BigInt(1000) as HLCTimestamp,
		});
		const d2 = makeDelta({
			deltaId: "del-2",
			columns: [],
			hlc: BigInt(2000) as HLCTimestamp,
			op: "DELETE",
		});

		await adapter.insertDeltas([d1, d2]);

		const result = await adapter.getLatestState("todos", "row-1");
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toBeNull();
		}
	});

	it("ensureSchema creates deltas table and user table", async () => {
		const schema: TableSchema = {
			table: "integration_test",
			columns: [
				{ name: "name", type: "string" },
				{ name: "age", type: "number" },
				{ name: "active", type: "boolean" },
			],
		};

		const result = await adapter.ensureSchema(schema);
		expect(result.ok).toBe(true);

		// Verify user table was created by inserting a row
		await adapter.pool.execute(
			"INSERT INTO `integration_test` (row_id, `name`, age, active) VALUES (?, ?, ?, ?)",
			["r1", "Alice", 30, 1],
		);

		const [rows] = await adapter.pool.execute("SELECT * FROM `integration_test` WHERE row_id = ?", [
			"r1",
		]);
		expect(rows as Array<Record<string, unknown>>).toHaveLength(1);

		// Clean up
		await adapter.pool.execute("DROP TABLE IF EXISTS `integration_test`");
	});

	it("multi-table support: deltas from different tables", async () => {
		const d1 = makeDelta({
			deltaId: "mt-1",
			table: "todos",
			rowId: "t1",
			hlc: BigInt(1000) as HLCTimestamp,
		});
		const d2 = makeDelta({
			deltaId: "mt-2",
			table: "users",
			rowId: "u1",
			columns: [{ column: "name", value: "Alice" }],
			hlc: BigInt(2000) as HLCTimestamp,
		});

		await adapter.insertDeltas([d1, d2]);

		const allResult = await adapter.queryDeltasSince(BigInt(0) as HLCTimestamp);
		expect(allResult.ok).toBe(true);
		if (allResult.ok) {
			expect(allResult.value).toHaveLength(2);
		}

		const todosResult = await adapter.queryDeltasSince(BigInt(0) as HLCTimestamp, ["todos"]);
		expect(todosResult.ok).toBe(true);
		if (todosResult.ok) {
			expect(todosResult.value).toHaveLength(1);
			expect(todosResult.value[0]!.table).toBe("todos");
		}
	});
});
