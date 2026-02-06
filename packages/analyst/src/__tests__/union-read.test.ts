import { HLC, type RowDelta, type TableSchema } from "@lakesync/core";
import { writeDeltasToParquet } from "@lakesync/parquet";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { DuckDBClient } from "../duckdb";
import { UnionReader } from "../union-read";

/** Schema for generating test Parquet data */
const todoSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "completed", type: "boolean" },
	],
};

/**
 * Creates test deltas representing cold (historical) data.
 */
function createColdDeltas(): RowDelta[] {
	const baseWall = 1700000000000;
	return [
		{
			op: "INSERT",
			table: "todos",
			rowId: "row-1",
			clientId: "client-a",
			columns: [
				{ column: "title", value: "Buy milk" },
				{ column: "completed", value: false },
			],
			hlc: HLC.encode(baseWall, 1),
			deltaId: "delta-1",
		},
		{
			op: "INSERT",
			table: "todos",
			rowId: "row-2",
			clientId: "client-a",
			columns: [
				{ column: "title", value: "Write tests" },
				{ column: "completed", value: true },
			],
			hlc: HLC.encode(baseWall + 1, 0),
			deltaId: "delta-2",
		},
	];
}

/**
 * Generates a Parquet buffer from cold deltas.
 */
async function createColdParquetBuffer(): Promise<Uint8Array> {
	const deltas = createColdDeltas();
	const result = await writeDeltasToParquet(deltas, todoSchema);
	if (!result.ok) {
		throw new Error(`Failed to create test Parquet: ${result.error.message}`);
	}
	return result.value;
}

/**
 * Creates hot rows that match the Parquet schema shape (flattened delta columns).
 * These represent in-memory data that has not yet been flushed to Parquet.
 */
function createHotRows(): Record<string, unknown>[] {
	const baseWall = 1700000010000;
	return [
		{
			op: "INSERT",
			table: "todos",
			rowId: "row-3",
			clientId: "client-b",
			title: "Deploy to prod",
			completed: false,
			hlc: String(HLC.encode(baseWall, 0)),
			deltaId: "delta-hot-1",
		},
		{
			op: "UPDATE",
			table: "todos",
			rowId: "row-1",
			clientId: "client-b",
			title: "Buy oat milk",
			completed: true,
			hlc: String(HLC.encode(baseWall + 1, 0)),
			deltaId: "delta-hot-2",
		},
	];
}

let duckDBAvailable = true;
try {
	await import("@duckdb/duckdb-wasm/blocking");
} catch {
	duckDBAvailable = false;
}

describe.skipIf(!duckDBAvailable)("UnionReader", () => {
	let client: DuckDBClient;
	let reader: UnionReader;

	beforeEach(async () => {
		client = new DuckDBClient({ logger: false });
		const initResult = await client.init();
		expect(initResult.ok).toBe(true);

		reader = new UnionReader({ duckdb: client, tableName: "todos" });
	});

	afterEach(async () => {
		await client.close();
	});

	describe("cold-only queries", () => {
		it("should query registered Parquet data", async () => {
			const parquetData = await createColdParquetBuffer();
			const regResult = await reader.registerColdData([
				{ name: "cold-batch-1.parquet", data: parquetData },
			]);
			expect(regResult.ok).toBe(true);

			const result = await reader.queryColdOnly("SELECT rowId, title FROM _union ORDER BY rowId");
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(result.value).toHaveLength(2);
			expect(result.value[0]!.rowId).toBe("row-1");
			expect(result.value[0]!.title).toBe("Buy milk");
			expect(result.value[1]!.rowId).toBe("row-2");
			expect(result.value[1]!.title).toBe("Write tests");
		});

		it("should support multiple Parquet files as cold sources", async () => {
			const parquetData = await createColdParquetBuffer();
			// Register the same data under two different names to simulate two batches
			const regResult = await reader.registerColdData([
				{ name: "cold-multi-1.parquet", data: parquetData },
				{ name: "cold-multi-2.parquet", data: parquetData },
			]);
			expect(regResult.ok).toBe(true);

			const result = await reader.queryColdOnly("SELECT COUNT(*) AS cnt FROM _union");
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			// 2 deltas per file x 2 files = 4 total rows
			expect(Number(result.value[0]!.cnt)).toBe(4);
		});

		it("should return empty array when no data is registered", async () => {
			const result = await reader.queryColdOnly("SELECT * FROM _union");
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(result.value).toHaveLength(0);
		});
	});

	describe("hot-only queries", () => {
		it("should query in-memory hot rows", async () => {
			const hotRows = createHotRows();
			const result = await reader.query("SELECT rowId, title FROM _union ORDER BY rowId", hotRows);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(result.value).toHaveLength(2);
			expect(result.value[0]!.rowId).toBe("row-1");
			expect(result.value[0]!.title).toBe("Buy oat milk");
			expect(result.value[1]!.rowId).toBe("row-3");
			expect(result.value[1]!.title).toBe("Deploy to prod");
		});

		it("should handle empty hot rows array as cold-only", async () => {
			const result = await reader.query("SELECT * FROM _union", []);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			// No cold sources and empty hot rows = empty result
			expect(result.value).toHaveLength(0);
		});
	});

	describe("union queries (hot + cold)", () => {
		it("should merge hot and cold data with UNION ALL", async () => {
			const parquetData = await createColdParquetBuffer();
			const regResult = await reader.registerColdData([
				{ name: "union-cold.parquet", data: parquetData },
			]);
			expect(regResult.ok).toBe(true);

			const hotRows = createHotRows();
			const result = await reader.query("SELECT rowId, title FROM _union ORDER BY rowId", hotRows);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			// 2 cold rows + 2 hot rows = 4 total
			expect(result.value).toHaveLength(4);
			const rowIds = result.value.map((r) => r.rowId);
			expect(rowIds).toContain("row-1");
			expect(rowIds).toContain("row-2");
			expect(rowIds).toContain("row-3");
		});

		it("should preserve all rows without deduplication", async () => {
			const parquetData = await createColdParquetBuffer();
			await reader.registerColdData([{ name: "union-nodup.parquet", data: parquetData }]);

			// Hot rows include an update to row-1, so row-1 appears twice
			const hotRows = createHotRows();
			const result = await reader.query("SELECT rowId FROM _union WHERE rowId = 'row-1'", hotRows);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			// row-1 exists in both cold (INSERT) and hot (UPDATE) = 2 rows
			expect(result.value).toHaveLength(2);
		});
	});

	describe("filtering", () => {
		it("should apply WHERE clause on unioned data", async () => {
			const parquetData = await createColdParquetBuffer();
			await reader.registerColdData([{ name: "filter-cold.parquet", data: parquetData }]);

			const hotRows = createHotRows();
			const result = await reader.query(
				"SELECT rowId, title FROM _union WHERE op = 'INSERT' ORDER BY rowId",
				hotRows,
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			// Cold: row-1 (INSERT), row-2 (INSERT). Hot: row-3 (INSERT), row-1 (UPDATE)
			// Filter on INSERT: row-1, row-2, row-3
			expect(result.value).toHaveLength(3);
			const rowIds = result.value.map((r) => r.rowId);
			expect(rowIds).toContain("row-1");
			expect(rowIds).toContain("row-2");
			expect(rowIds).toContain("row-3");
		});

		it("should support LIKE filtering", async () => {
			const parquetData = await createColdParquetBuffer();
			await reader.registerColdData([{ name: "like-cold.parquet", data: parquetData }]);

			const hotRows = createHotRows();
			const result = await reader.query(
				"SELECT title FROM _union WHERE title LIKE '%milk%' ORDER BY title",
				hotRows,
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			// "Buy milk" (cold) and "Buy oat milk" (hot)
			expect(result.value).toHaveLength(2);
			const titles = result.value.map((r) => r.title);
			expect(titles).toContain("Buy milk");
			expect(titles).toContain("Buy oat milk");
		});
	});

	describe("aggregation", () => {
		it("should support COUNT aggregation on unioned data", async () => {
			const parquetData = await createColdParquetBuffer();
			await reader.registerColdData([{ name: "agg-count.parquet", data: parquetData }]);

			const hotRows = createHotRows();
			const result = await reader.query("SELECT COUNT(*) AS cnt FROM _union", hotRows);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			// 2 cold + 2 hot = 4
			expect(Number(result.value[0]!.cnt)).toBe(4);
		});

		it("should support GROUP BY aggregation", async () => {
			const parquetData = await createColdParquetBuffer();
			await reader.registerColdData([{ name: "agg-group.parquet", data: parquetData }]);

			const hotRows = createHotRows();
			const result = await reader.query(
				"SELECT op, COUNT(*) AS cnt FROM _union GROUP BY op ORDER BY op",
				hotRows,
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			// INSERT: row-1(cold), row-2(cold), row-3(hot) = 3
			// UPDATE: row-1(hot) = 1
			const insertRow = result.value.find((r) => r.op === "INSERT");
			const updateRow = result.value.find((r) => r.op === "UPDATE");
			expect(insertRow).toBeDefined();
			expect(updateRow).toBeDefined();
			expect(Number(insertRow!.cnt)).toBe(3);
			expect(Number(updateRow!.cnt)).toBe(1);
		});

		it("should support SUM-like aggregation with COUNT and conditions", async () => {
			const parquetData = await createColdParquetBuffer();
			await reader.registerColdData([{ name: "agg-sum.parquet", data: parquetData }]);

			const hotRows = createHotRows();
			const result = await reader.query(
				"SELECT COUNT(*) AS total, COUNT(CASE WHEN op = 'INSERT' THEN 1 END) AS inserts FROM _union",
				hotRows,
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(Number(result.value[0]!.total)).toBe(4);
			expect(Number(result.value[0]!.inserts)).toBe(3);
		});
	});

	describe("error handling", () => {
		it("should return an error for invalid SQL", async () => {
			const parquetData = await createColdParquetBuffer();
			await reader.registerColdData([{ name: "err-cold.parquet", data: parquetData }]);

			const result = await reader.query("SELEKT broken syntax!!!");
			expect(result.ok).toBe(false);
			if (result.ok) return;

			expect(result.error.code).toBe("ANALYST_ERROR");
		});

		it("should return an error when querying after DuckDB is closed", async () => {
			const parquetData = await createColdParquetBuffer();
			await reader.registerColdData([{ name: "err-closed.parquet", data: parquetData }]);

			await client.close();

			const result = await reader.query("SELECT * FROM _union");
			expect(result.ok).toBe(false);
			if (result.ok) return;

			expect(result.error.code).toBe("ANALYST_ERROR");
		});

		it("should return an error when registering cold data after DuckDB is closed", async () => {
			await client.close();

			const result = await reader.registerColdData([
				{ name: "err-reg.parquet", data: new Uint8Array(0) },
			]);
			expect(result.ok).toBe(false);
			if (result.ok) return;

			expect(result.error.code).toBe("ANALYST_ERROR");
		});

		it("should handle multiple sequential queries with different hot rows", async () => {
			const parquetData = await createColdParquetBuffer();
			await reader.registerColdData([{ name: "seq-cold.parquet", data: parquetData }]);

			// First query with one set of hot rows
			const result1 = await reader.query("SELECT COUNT(*) AS cnt FROM _union", [
				{
					op: "INSERT",
					table: "todos",
					rowId: "row-10",
					clientId: "c",
					title: "Task A",
					completed: false,
					hlc: "0",
					deltaId: "d-10",
				},
			]);
			expect(result1.ok).toBe(true);
			if (!result1.ok) return;
			expect(Number(result1.value[0]!.cnt)).toBe(3); // 2 cold + 1 hot

			// Second query with different hot rows
			const result2 = await reader.query("SELECT COUNT(*) AS cnt FROM _union", [
				{
					op: "INSERT",
					table: "todos",
					rowId: "row-20",
					clientId: "c",
					title: "Task B",
					completed: false,
					hlc: "0",
					deltaId: "d-20",
				},
				{
					op: "INSERT",
					table: "todos",
					rowId: "row-21",
					clientId: "c",
					title: "Task C",
					completed: true,
					hlc: "0",
					deltaId: "d-21",
				},
			]);
			expect(result2.ok).toBe(true);
			if (!result2.ok) return;
			expect(Number(result2.value[0]!.cnt)).toBe(4); // 2 cold + 2 hot
		});
	});
});
