import { HLC, type RowDelta, type TableSchema } from "@lakesync/core";
import { writeDeltasToParquet } from "@lakesync/parquet";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { DuckDBClient } from "../duckdb";
import { TimeTraveller } from "../time-travel";

/** Schema for generating test Parquet data */
const todoSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "completed", type: "boolean" },
		{ name: "priority", type: "number" },
	],
};

/**
 * Base wall clock used across all test helpers.
 * Timestamps are built as offsets from this base.
 */
const BASE_WALL = 1700000000000;

/** HLC at T+1 (wall=BASE+1, counter=0) */
const HLC_T1 = HLC.encode(BASE_WALL + 1, 0);
/** HLC at T+2 */
const HLC_T2 = HLC.encode(BASE_WALL + 2, 0);
/** HLC at T+3 */
const HLC_T3 = HLC.encode(BASE_WALL + 3, 0);
/** HLC at T+4 */
const HLC_T4 = HLC.encode(BASE_WALL + 4, 0);
/** HLC at T+5 */
const HLC_T5 = HLC.encode(BASE_WALL + 5, 0);
/** HLC at T+6 */
const HLC_T6 = HLC.encode(BASE_WALL + 6, 0);
/** HLC at T+7 */
const HLC_T7 = HLC.encode(BASE_WALL + 7, 0);

/**
 * Creates a comprehensive set of deltas for time-travel testing.
 *
 * Timeline:
 * - T1: INSERT row-1 (title="Buy milk", completed=false, priority=1)
 * - T2: INSERT row-2 (title="Write tests", completed=false, priority=2)
 * - T3: UPDATE row-1 (title="Buy oat milk") — only title changes
 * - T4: UPDATE row-1 (completed=true) — only completed changes
 * - T5: INSERT row-3 (title="Deploy", completed=false, priority=3)
 * - T6: DELETE row-2
 * - T7: UPDATE row-3 (title="Deploy to prod", priority=1)
 */
function createTimelineDeltas(): RowDelta[] {
	return [
		{
			op: "INSERT",
			table: "todos",
			rowId: "row-1",
			clientId: "client-a",
			columns: [
				{ column: "title", value: "Buy milk" },
				{ column: "completed", value: false },
				{ column: "priority", value: 1 },
			],
			hlc: HLC_T1,
			deltaId: "delta-t1",
		},
		{
			op: "INSERT",
			table: "todos",
			rowId: "row-2",
			clientId: "client-a",
			columns: [
				{ column: "title", value: "Write tests" },
				{ column: "completed", value: false },
				{ column: "priority", value: 2 },
			],
			hlc: HLC_T2,
			deltaId: "delta-t2",
		},
		{
			op: "UPDATE",
			table: "todos",
			rowId: "row-1",
			clientId: "client-b",
			columns: [{ column: "title", value: "Buy oat milk" }],
			hlc: HLC_T3,
			deltaId: "delta-t3",
		},
		{
			op: "UPDATE",
			table: "todos",
			rowId: "row-1",
			clientId: "client-a",
			columns: [{ column: "completed", value: true }],
			hlc: HLC_T4,
			deltaId: "delta-t4",
		},
		{
			op: "INSERT",
			table: "todos",
			rowId: "row-3",
			clientId: "client-a",
			columns: [
				{ column: "title", value: "Deploy" },
				{ column: "completed", value: false },
				{ column: "priority", value: 3 },
			],
			hlc: HLC_T5,
			deltaId: "delta-t5",
		},
		{
			op: "DELETE",
			table: "todos",
			rowId: "row-2",
			clientId: "client-a",
			columns: [],
			hlc: HLC_T6,
			deltaId: "delta-t6",
		},
		{
			op: "UPDATE",
			table: "todos",
			rowId: "row-3",
			clientId: "client-b",
			columns: [
				{ column: "title", value: "Deploy to prod" },
				{ column: "priority", value: 1 },
			],
			hlc: HLC_T7,
			deltaId: "delta-t7",
		},
	];
}

/**
 * Generates a Parquet buffer from the timeline deltas.
 */
async function createTimelineParquet(): Promise<Uint8Array> {
	const deltas = createTimelineDeltas();
	const result = await writeDeltasToParquet(deltas, todoSchema);
	if (!result.ok) {
		throw new Error(`Failed to create test Parquet: ${result.error.message}`);
	}
	return result.value;
}

let duckDBAvailable = true;
try {
	await import("@duckdb/duckdb-wasm/blocking");
} catch {
	duckDBAvailable = false;
}

describe.skipIf(!duckDBAvailable)("TimeTraveller", () => {
	let client: DuckDBClient;
	let traveller: TimeTraveller;

	beforeEach(async () => {
		client = new DuckDBClient({ logger: false });
		const initResult = await client.init();
		expect(initResult.ok).toBe(true);

		traveller = new TimeTraveller({ duckdb: client });
	});

	afterEach(async () => {
		await client.close();
	});

	describe("registerDeltas", () => {
		it("should register Parquet buffers successfully", async () => {
			const parquetData = await createTimelineParquet();
			const result = await traveller.registerDeltas([
				{ name: "reg-test.parquet", data: parquetData },
			]);
			expect(result.ok).toBe(true);
		});

		it("should return an error when DuckDB is closed", async () => {
			await client.close();
			const result = await traveller.registerDeltas([
				{ name: "err-reg.parquet", data: new Uint8Array(0) },
			]);
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.code).toBe("ANALYST_ERROR");
		});
	});

	describe("queryAsOf", () => {
		it("should return only data that existed at T1", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "asof-t1.parquet", data: parquetData }]);

			const result = await traveller.queryAsOf(
				HLC_T1,
				'SELECT "rowId", title, completed, priority FROM _state ORDER BY "rowId"',
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			// At T1, only row-1 has been inserted
			expect(result.value).toHaveLength(1);
			expect(result.value[0]!.rowId).toBe("row-1");
			expect(result.value[0]!.title).toBe("Buy milk");
			// Boolean columns are stored as Int8 in Parquet (0=false, 1=true)
			expect(Number(result.value[0]!.completed)).toBe(0);
			expect(Number(result.value[0]!.priority)).toBe(1);
		});

		it("should return two rows at T2", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "asof-t2.parquet", data: parquetData }]);

			const result = await traveller.queryAsOf(
				HLC_T2,
				'SELECT "rowId", title FROM _state ORDER BY "rowId"',
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(result.value).toHaveLength(2);
			expect(result.value[0]!.rowId).toBe("row-1");
			expect(result.value[0]!.title).toBe("Buy milk");
			expect(result.value[1]!.rowId).toBe("row-2");
			expect(result.value[1]!.title).toBe("Write tests");
		});

		it("should reflect column-level LWW updates at T3", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "asof-t3.parquet", data: parquetData }]);

			const result = await traveller.queryAsOf(
				HLC_T3,
				'SELECT "rowId", title, completed FROM _state WHERE "rowId" = \'row-1\'',
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(result.value).toHaveLength(1);
			// Title was updated at T3
			expect(result.value[0]!.title).toBe("Buy oat milk");
			// Completed was NOT changed at T3 — should retain original value from T1
			expect(Number(result.value[0]!.completed)).toBe(0);
		});

		it("should reflect multiple column-level updates at T4", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "asof-t4.parquet", data: parquetData }]);

			const result = await traveller.queryAsOf(
				HLC_T4,
				'SELECT "rowId", title, completed FROM _state WHERE "rowId" = \'row-1\'',
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(result.value).toHaveLength(1);
			// Title from T3 update
			expect(result.value[0]!.title).toBe("Buy oat milk");
			// Completed from T4 update
			expect(Number(result.value[0]!.completed)).toBe(1);
		});

		it("should support WHERE clauses on the materialised view", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "asof-where.parquet", data: parquetData }]);

			// At T5, three rows exist. Filter for completed ones only.
			const result = await traveller.queryAsOf(
				HLC_T5,
				'SELECT "rowId" FROM _state WHERE completed = 1',
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			// Only row-1 is completed (updated at T4)
			expect(result.value).toHaveLength(1);
			expect(result.value[0]!.rowId).toBe("row-1");
		});

		it("should return empty array when no sources are registered", async () => {
			const result = await traveller.queryAsOf(HLC_T1, "SELECT * FROM _state");
			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value).toHaveLength(0);
		});
	});

	describe("queryBetween", () => {
		it("should return only deltas in the specified range", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "between-1.parquet", data: parquetData }]);

			// Query deltas between T2 and T4 (exclusive lower, inclusive upper)
			// Should include T3 and T4 deltas only
			const result = await traveller.queryBetween(
				HLC_T2,
				HLC_T4,
				'SELECT "rowId", op FROM _deltas ORDER BY CAST(hlc AS BIGINT) ASC',
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(result.value).toHaveLength(2);
			// T3: UPDATE row-1
			expect(result.value[0]!.rowId).toBe("row-1");
			expect(result.value[0]!.op).toBe("UPDATE");
			// T4: UPDATE row-1
			expect(result.value[1]!.rowId).toBe("row-1");
			expect(result.value[1]!.op).toBe("UPDATE");
		});

		it("should return all deltas when range covers entire timeline", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "between-all.parquet", data: parquetData }]);

			// Use a range that covers all timestamps
			const beforeAll = HLC.encode(BASE_WALL, 0);
			const afterAll = HLC.encode(BASE_WALL + 100, 0);

			const result = await traveller.queryBetween(
				beforeAll,
				afterAll,
				"SELECT COUNT(*) AS cnt FROM _deltas",
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(Number(result.value[0]!.cnt)).toBe(7);
		});

		it("should return empty array when range is empty", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "between-empty.parquet", data: parquetData }]);

			// Query with fromHlc >= toHlc: no results
			const result = await traveller.queryBetween(
				HLC_T5,
				HLC_T1,
				"SELECT COUNT(*) AS cnt FROM _deltas",
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(Number(result.value[0]!.cnt)).toBe(0);
		});

		it("should return empty array when no sources are registered", async () => {
			const result = await traveller.queryBetween(HLC_T1, HLC_T7, "SELECT * FROM _deltas");
			expect(result.ok).toBe(true);
			if (!result.ok) return;
			expect(result.value).toHaveLength(0);
		});

		it("should include DELETE deltas in raw output", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "between-del.parquet", data: parquetData }]);

			// Range T5..T6 should include the DELETE at T6
			const result = await traveller.queryBetween(
				HLC_T5,
				HLC_T6,
				'SELECT op, "rowId" FROM _deltas',
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(result.value).toHaveLength(1);
			expect(result.value[0]!.op).toBe("DELETE");
			expect(result.value[0]!.rowId).toBe("row-2");
		});
	});

	describe("materialiseAsOf", () => {
		it("should materialise full state at T5", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "mat-t5.parquet", data: parquetData }]);

			const result = await traveller.materialiseAsOf(HLC_T5);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			// At T5, rows: row-1 (updated), row-2 (original), row-3 (just inserted)
			expect(result.value).toHaveLength(3);

			const byRowId = new Map(result.value.map((r) => [r.rowId, r]));

			const row1 = byRowId.get("row-1");
			expect(row1).toBeDefined();
			expect(row1!.title).toBe("Buy oat milk");
			expect(Number(row1!.completed)).toBe(1);
			expect(Number(row1!.priority)).toBe(1);

			const row2 = byRowId.get("row-2");
			expect(row2).toBeDefined();
			expect(row2!.title).toBe("Write tests");

			const row3 = byRowId.get("row-3");
			expect(row3).toBeDefined();
			expect(row3!.title).toBe("Deploy");
			expect(Number(row3!.completed)).toBe(0);
		});

		it("should materialise full state at T7 (end of timeline)", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "mat-t7.parquet", data: parquetData }]);

			const result = await traveller.materialiseAsOf(HLC_T7);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			// At T7: row-1 (updated), row-2 DELETED, row-3 (updated)
			expect(result.value).toHaveLength(2);

			const byRowId = new Map(result.value.map((r) => [r.rowId, r]));

			expect(byRowId.has("row-2")).toBe(false); // Deleted at T6

			const row1 = byRowId.get("row-1");
			expect(row1).toBeDefined();
			expect(row1!.title).toBe("Buy oat milk");

			const row3 = byRowId.get("row-3");
			expect(row3).toBeDefined();
			expect(row3!.title).toBe("Deploy to prod");
			expect(Number(row3!.priority)).toBe(1); // Updated at T7
		});
	});

	describe("DELETE handling", () => {
		it("should include row-2 before its deletion at T5", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "del-before.parquet", data: parquetData }]);

			const result = await traveller.materialiseAsOf(HLC_T5);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			const rowIds = result.value.map((r) => r.rowId);
			expect(rowIds).toContain("row-2");
		});

		it("should exclude row-2 after its deletion at T6", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "del-after.parquet", data: parquetData }]);

			const result = await traveller.materialiseAsOf(HLC_T6);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			const rowIds = result.value.map((r) => r.rowId);
			expect(rowIds).not.toContain("row-2");
		});

		it("should still exclude row-2 at T7 (after further changes)", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "del-later.parquet", data: parquetData }]);

			const result = await traveller.materialiseAsOf(HLC_T7);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			const rowIds = result.value.map((r) => r.rowId);
			expect(rowIds).not.toContain("row-2");
			// Other rows should still be present
			expect(rowIds).toContain("row-1");
			expect(rowIds).toContain("row-3");
		});
	});

	describe("column-level LWW", () => {
		it("should pick the latest value per column independently", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "lww-cols.parquet", data: parquetData }]);

			// At T4, row-1 has:
			// - title="Buy oat milk" (from T3)
			// - completed=true (from T4)
			// - priority=1 (from T1, never updated)
			const result = await traveller.queryAsOf(
				HLC_T4,
				"SELECT title, completed, priority FROM _state WHERE \"rowId\" = 'row-1'",
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(result.value).toHaveLength(1);
			expect(result.value[0]!.title).toBe("Buy oat milk");
			expect(Number(result.value[0]!.completed)).toBe(1);
			expect(Number(result.value[0]!.priority)).toBe(1);
		});

		it("should handle partial updates correctly", async () => {
			// At T3, only title was updated for row-1. Other columns should
			// retain their T1 values.
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "lww-partial.parquet", data: parquetData }]);

			const result = await traveller.queryAsOf(
				HLC_T3,
				"SELECT title, completed, priority FROM _state WHERE \"rowId\" = 'row-1'",
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(result.value).toHaveLength(1);
			expect(result.value[0]!.title).toBe("Buy oat milk"); // Updated at T3
			expect(Number(result.value[0]!.completed)).toBe(0); // Original from T1
			expect(Number(result.value[0]!.priority)).toBe(1); // Original from T1
		});

		it("should handle updates to multiple columns at once", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "lww-multi.parquet", data: parquetData }]);

			// At T7, row-3 was updated with both title and priority at once
			const result = await traveller.queryAsOf(
				HLC_T7,
				"SELECT title, completed, priority FROM _state WHERE \"rowId\" = 'row-3'",
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(result.value).toHaveLength(1);
			expect(result.value[0]!.title).toBe("Deploy to prod"); // Updated at T7
			expect(Number(result.value[0]!.completed)).toBe(0); // Original from T5
			expect(Number(result.value[0]!.priority)).toBe(1); // Updated at T7
		});
	});

	describe("multiple Parquet sources", () => {
		it("should combine deltas from multiple Parquet files", async () => {
			// Split deltas across two files
			const earlyDeltas = createTimelineDeltas().slice(0, 3); // T1-T3
			const lateDeltas = createTimelineDeltas().slice(3); // T4-T7

			const earlyResult = await writeDeltasToParquet(earlyDeltas, todoSchema);
			const lateResult = await writeDeltasToParquet(lateDeltas, todoSchema);
			if (!earlyResult.ok || !lateResult.ok) {
				throw new Error("Failed to create test Parquet files");
			}

			await traveller.registerDeltas([
				{ name: "multi-early.parquet", data: earlyResult.value },
				{ name: "multi-late.parquet", data: lateResult.value },
			]);

			// Query at T7 should see the combined result
			const result = await traveller.materialiseAsOf(HLC_T7);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(result.value).toHaveLength(2); // row-2 deleted
			const byRowId = new Map(result.value.map((r) => [r.rowId, r]));
			expect(byRowId.get("row-1")!.title).toBe("Buy oat milk");
			expect(byRowId.get("row-3")!.title).toBe("Deploy to prod");
		});
	});

	describe("error handling", () => {
		it("should return an error for invalid SQL in queryAsOf", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "err-sql.parquet", data: parquetData }]);

			const result = await traveller.queryAsOf(HLC_T1, "SELEKT broken syntax!!!");
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.code).toBe("ANALYST_ERROR");
		});

		it("should return an error for invalid SQL in queryBetween", async () => {
			const parquetData = await createTimelineParquet();
			await traveller.registerDeltas([{ name: "err-between.parquet", data: parquetData }]);

			const result = await traveller.queryBetween(HLC_T1, HLC_T7, "SELEKT broken!!!");
			expect(result.ok).toBe(false);
			if (result.ok) return;
			expect(result.error.code).toBe("ANALYST_ERROR");
		});
	});
});
