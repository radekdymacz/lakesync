import { HLC, type RowDelta, type TableSchema } from "@lakesync/core";
import { writeDeltasToParquet } from "@lakesync/parquet";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { DuckDBClient } from "../duckdb";

/** Simple schema for generating test Parquet data */
const todoSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "completed", type: "boolean" },
	],
};

/**
 * Creates a small set of test deltas for Parquet generation.
 */
function createTestDeltas(): RowDelta[] {
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
		{
			op: "UPDATE",
			table: "todos",
			rowId: "row-1",
			clientId: "client-b",
			columns: [
				{ column: "title", value: "Buy oat milk" },
				{ column: "completed", value: true },
			],
			hlc: HLC.encode(baseWall + 2, 0),
			deltaId: "delta-3",
		},
	];
}

/**
 * Generates a Parquet buffer from test deltas using @lakesync/parquet.
 */
async function createTestParquetBuffer(): Promise<Uint8Array> {
	const deltas = createTestDeltas();
	const result = await writeDeltasToParquet(deltas, todoSchema);
	if (!result.ok) {
		throw new Error(`Failed to create test Parquet: ${result.error.message}`);
	}
	return result.value;
}

let duckDBAvailable = true;
try {
	// Quick check that the blocking bindings can be loaded
	await import("@duckdb/duckdb-wasm/blocking");
} catch {
	duckDBAvailable = false;
}

describe.skipIf(!duckDBAvailable)("DuckDBClient", () => {
	let client: DuckDBClient;

	beforeEach(async () => {
		client = new DuckDBClient({ logger: false });
		const result = await client.init();
		expect(result.ok).toBe(true);
	});

	afterEach(async () => {
		await client.close();
	});

	describe("init / close lifecycle", () => {
		it("should initialise and close without errors", async () => {
			const freshClient = new DuckDBClient();
			const initResult = await freshClient.init();
			expect(initResult.ok).toBe(true);
			await freshClient.close();
		});

		it("should handle double close gracefully", async () => {
			const freshClient = new DuckDBClient();
			const initResult = await freshClient.init();
			expect(initResult.ok).toBe(true);

			await freshClient.close();
			// Second close should not throw
			await freshClient.close();
		});
	});

	describe("query", () => {
		it("should execute a simple arithmetic query", async () => {
			const result = await client.query<{ result: number }>("SELECT 1 + 1 AS result");
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(result.value).toHaveLength(1);
			expect(result.value[0]!.result).toBe(2);
		});

		it("should execute a multi-row query", async () => {
			const result = await client.query<{ n: bigint }>(
				"SELECT * FROM generate_series(1, 5) AS t(n)",
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(result.value).toHaveLength(5);
			const values = result.value.map((r) => Number(r.n));
			expect(values).toEqual([1, 2, 3, 4, 5]);
		});

		it("should handle string and null values", async () => {
			const result = await client.query<{ greeting: string; nothing: null }>(
				"SELECT 'hello' AS greeting, NULL AS nothing",
			);
			expect(result.ok).toBe(true);
			if (!result.ok) return;

			expect(result.value).toHaveLength(1);
			expect(result.value[0]!.greeting).toBe("hello");
			expect(result.value[0]!.nothing).toBeNull();
		});
	});

	describe("registerParquetBuffer", () => {
		it("should register and query a Parquet buffer", async () => {
			const parquetData = await createTestParquetBuffer();
			const regResult = await client.registerParquetBuffer("test.parquet", parquetData);
			expect(regResult.ok).toBe(true);

			const queryResult = await client.query<{ op: string; rowId: string; title: string }>(
				"SELECT op, rowId, title FROM 'test.parquet' ORDER BY rowId, op",
			);
			expect(queryResult.ok).toBe(true);
			if (!queryResult.ok) return;

			expect(queryResult.value).toHaveLength(3);
			// Verify data from the Parquet file is accessible
			const rowIds = queryResult.value.map((r) => r.rowId);
			expect(rowIds).toContain("row-1");
			expect(rowIds).toContain("row-2");
		});

		it("should support aggregation queries on Parquet data", async () => {
			const parquetData = await createTestParquetBuffer();
			const regResult = await client.registerParquetBuffer("agg.parquet", parquetData);
			expect(regResult.ok).toBe(true);

			const countResult = await client.query<{ cnt: number | bigint }>(
				"SELECT COUNT(*) AS cnt FROM 'agg.parquet'",
			);
			expect(countResult.ok).toBe(true);
			if (!countResult.ok) return;

			expect(Number(countResult.value[0]!.cnt)).toBe(3);
		});

		it("should support filtering queries on Parquet data", async () => {
			const parquetData = await createTestParquetBuffer();
			await client.registerParquetBuffer("filter.parquet", parquetData);

			const filterResult = await client.query<{ title: string }>(
				"SELECT title FROM 'filter.parquet' WHERE op = 'INSERT' ORDER BY title",
			);
			expect(filterResult.ok).toBe(true);
			if (!filterResult.ok) return;

			expect(filterResult.value).toHaveLength(2);
			expect(filterResult.value[0]!.title).toBe("Buy milk");
			expect(filterResult.value[1]!.title).toBe("Write tests");
		});
	});

	describe("error handling", () => {
		it("should return an error for invalid SQL", async () => {
			const result = await client.query("SELEKT invalid syntax!!!");
			expect(result.ok).toBe(false);
			if (result.ok) return;

			expect(result.error.code).toBe("ANALYST_ERROR");
			expect(result.error.message).toContain("DuckDB query failed");
		});

		it("should return an error when querying after close", async () => {
			await client.close();

			const result = await client.query("SELECT 1");
			expect(result.ok).toBe(false);
			if (result.ok) return;

			expect(result.error.code).toBe("ANALYST_ERROR");
			expect(result.error.message).toContain("closed or not initialised");
		});

		it("should return an error when registering Parquet buffer after close", async () => {
			await client.close();

			const result = await client.registerParquetBuffer("test.parquet", new Uint8Array(0));
			expect(result.ok).toBe(false);
			if (result.ok) return;

			expect(result.error.code).toBe("ANALYST_ERROR");
			expect(result.error.message).toContain("closed or not initialised");
		});

		it("should return an error when registering Parquet URL after close", async () => {
			await client.close();

			const result = await client.registerParquetUrl(
				"remote.parquet",
				"https://example.com/data.parquet",
			);
			expect(result.ok).toBe(false);
			if (result.ok) return;

			expect(result.error.code).toBe("ANALYST_ERROR");
			expect(result.error.message).toContain("closed or not initialised");
		});
	});
});
