import type { HLCTimestamp, RowDelta, TableSchema } from "@lakesync/core";
import { AdapterError } from "@lakesync/core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { BigQueryAdapter } from "../bigquery";

/** Helper to create a RowDelta for testing. */
function makeDelta(overrides: Partial<RowDelta> = {}): RowDelta {
	return {
		deltaId: "delta-1",
		table: "todos",
		rowId: "row-1",
		clientId: "client-a",
		columns: [{ column: "title", value: "Buy milk" }],
		hlc: BigInt(1000) as HLCTimestamp,
		op: "INSERT",
		...overrides,
	};
}

/** Create an adapter with a mocked BigQuery client. */
function createMockedAdapter() {
	const adapter = new BigQueryAdapter({
		projectId: "test-project",
		dataset: "test_dataset",
		location: "US",
	});

	const mockQuery = vi.fn().mockResolvedValue([[]]);
	const mockExists = vi.fn().mockResolvedValue([true]);
	const mockCreateDataset = vi.fn().mockResolvedValue([{}]);
	const mockDataset = vi.fn().mockReturnValue({ exists: mockExists });

	// Replace client methods with mocks
	(adapter.client as unknown as { query: typeof mockQuery }).query = mockQuery;
	(adapter.client as unknown as { dataset: typeof mockDataset }).dataset = mockDataset;
	(adapter.client as unknown as { createDataset: typeof mockCreateDataset }).createDataset =
		mockCreateDataset;

	return { adapter, mockQuery, mockDataset, mockExists, mockCreateDataset };
}

describe("BigQueryAdapter", () => {
	let adapter: BigQueryAdapter;
	let mockQuery: ReturnType<typeof vi.fn>;
	let mockDataset: ReturnType<typeof vi.fn>;
	let mockExists: ReturnType<typeof vi.fn>;
	let mockCreateDataset: ReturnType<typeof vi.fn>;

	beforeEach(() => {
		const mocked = createMockedAdapter();
		adapter = mocked.adapter;
		mockQuery = mocked.mockQuery;
		mockDataset = mocked.mockDataset;
		mockExists = mocked.mockExists;
		mockCreateDataset = mocked.mockCreateDataset;
	});

	describe("insertDeltas", () => {
		it("returns Ok for empty deltas array", async () => {
			const result = await adapter.insertDeltas([]);
			expect(result.ok).toBe(true);
			expect(mockQuery).not.toHaveBeenCalled();
		});

		it("generates correct MERGE SQL for a single delta", async () => {
			const delta = makeDelta();
			const result = await adapter.insertDeltas([delta]);

			expect(result.ok).toBe(true);
			expect(mockQuery).toHaveBeenCalledTimes(1);

			const callArg = mockQuery.mock.calls[0]![0];
			expect(callArg.query).toContain("MERGE `test_dataset.lakesync_deltas` AS target");
			expect(callArg.query).toContain("WHEN NOT MATCHED THEN INSERT");
			expect(callArg.query).toContain("CAST(@hlc_0 AS INT64)");

			expect(callArg.params).toEqual({
				did_0: "delta-1",
				tbl_0: "todos",
				rid_0: "row-1",
				col_0: JSON.stringify([{ column: "title", value: "Buy milk" }]),
				hlc_0: "1000",
				cid_0: "client-a",
				op_0: "INSERT",
			});
		});

		it("batches multiple deltas into a single MERGE with UNION ALL", async () => {
			const deltas = [
				makeDelta({ deltaId: "d1", rowId: "r1" }),
				makeDelta({ deltaId: "d2", rowId: "r2" }),
			];

			const result = await adapter.insertDeltas(deltas);
			expect(result.ok).toBe(true);
			expect(mockQuery).toHaveBeenCalledTimes(1);

			const callArg = mockQuery.mock.calls[0]![0];
			expect(callArg.query).toContain("UNION ALL");
			expect(callArg.params.did_0).toBe("d1");
			expect(callArg.params.did_1).toBe("d2");
			expect(callArg.params.rid_0).toBe("r1");
			expect(callArg.params.rid_1).toBe("r2");
		});

		it("serialises HLC bigint as string in params", async () => {
			const delta = makeDelta({ hlc: BigInt("281474976710656") as HLCTimestamp });
			await adapter.insertDeltas([delta]);

			const callArg = mockQuery.mock.calls[0]![0];
			expect(callArg.params.hlc_0).toBe("281474976710656");
			expect(typeof callArg.params.hlc_0).toBe("string");
		});

		it("returns Err(AdapterError) when query throws", async () => {
			mockQuery.mockRejectedValueOnce(new Error("quota exceeded"));

			const result = await adapter.insertDeltas([makeDelta()]);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error).toBeInstanceOf(AdapterError);
				expect(result.error.message).toBe("Failed to insert deltas");
				expect(result.error.cause).toBeInstanceOf(Error);
			}
		});
	});

	describe("queryDeltasSince", () => {
		it("generates correct SQL without table filter", async () => {
			const hlc = BigInt(500) as HLCTimestamp;
			await adapter.queryDeltasSince(hlc);

			const callArg = mockQuery.mock.calls[0]![0];
			expect(callArg.query).toContain("WHERE hlc > CAST(@sinceHlc AS INT64)");
			expect(callArg.query).toContain("ORDER BY hlc ASC");
			expect(callArg.query).not.toContain("UNNEST");
			expect(callArg.params).toEqual({ sinceHlc: "500" });
		});

		it("generates correct SQL with table filter", async () => {
			const hlc = BigInt(500) as HLCTimestamp;
			await adapter.queryDeltasSince(hlc, ["todos", "users"]);

			const callArg = mockQuery.mock.calls[0]![0];
			expect(callArg.query).toContain("IN UNNEST(@tables)");
			expect(callArg.params).toEqual({
				sinceHlc: "500",
				tables: ["todos", "users"],
			});
		});

		it("converts rows back to RowDelta format", async () => {
			mockQuery.mockResolvedValueOnce([
				[
					{
						delta_id: "d1",
						table: "todos",
						row_id: "r1",
						columns: JSON.stringify([{ column: "title", value: "Test" }]),
						hlc: { value: "1000" },
						client_id: "c1",
						op: "INSERT",
					},
				],
			]);

			const result = await adapter.queryDeltasSince(BigInt(0) as HLCTimestamp);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(1);
				const delta = result.value[0]!;
				expect(delta.deltaId).toBe("d1");
				expect(delta.table).toBe("todos");
				expect(delta.rowId).toBe("r1");
				expect(delta.hlc).toBe(BigInt(1000));
				expect(delta.clientId).toBe("c1");
				expect(delta.op).toBe("INSERT");
				expect(delta.columns).toEqual([{ column: "title", value: "Test" }]);
			}
		});

		it("handles INT64 returned as { value: string } object", async () => {
			mockQuery.mockResolvedValueOnce([
				[
					{
						delta_id: "d1",
						table: "todos",
						row_id: "r1",
						columns: JSON.stringify([{ column: "done", value: true }]),
						hlc: { value: "281474976710656" },
						client_id: "c1",
						op: "UPDATE",
					},
				],
			]);

			const result = await adapter.queryDeltasSince(BigInt(0) as HLCTimestamp);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value[0]!.hlc).toBe(BigInt("281474976710656"));
			}
		});
	});

	describe("getLatestState", () => {
		it("returns null when no deltas exist", async () => {
			mockQuery.mockResolvedValueOnce([[]]);

			const result = await adapter.getLatestState("todos", "r1");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBeNull();
			}
		});

		it("merges columns with LWW (later values overwrite)", async () => {
			mockQuery.mockResolvedValueOnce([
				[
					{
						columns: JSON.stringify([
							{ column: "title", value: "Old" },
							{ column: "done", value: false },
						]),
						hlc: { value: "1000" },
						client_id: "c1",
						op: "INSERT",
					},
					{
						columns: JSON.stringify([{ column: "title", value: "New" }]),
						hlc: { value: "2000" },
						client_id: "c1",
						op: "UPDATE",
					},
				],
			]);

			const result = await adapter.getLatestState("todos", "r1");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toEqual({ title: "New", done: false });
			}
		});

		it("returns null when last delta is a DELETE", async () => {
			mockQuery.mockResolvedValueOnce([
				[
					{
						columns: JSON.stringify([{ column: "title", value: "Test" }]),
						hlc: { value: "1000" },
						client_id: "c1",
						op: "INSERT",
					},
					{
						columns: JSON.stringify([]),
						hlc: { value: "2000" },
						client_id: "c1",
						op: "DELETE",
					},
				],
			]);

			const result = await adapter.getLatestState("todos", "r1");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBeNull();
			}
		});

		it("handles resurrection after DELETE", async () => {
			mockQuery.mockResolvedValueOnce([
				[
					{
						columns: JSON.stringify([{ column: "title", value: "First" }]),
						hlc: { value: "1000" },
						client_id: "c1",
						op: "INSERT",
					},
					{
						columns: JSON.stringify([]),
						hlc: { value: "2000" },
						client_id: "c1",
						op: "DELETE",
					},
					{
						columns: JSON.stringify([{ column: "title", value: "Resurrected" }]),
						hlc: { value: "3000" },
						client_id: "c1",
						op: "INSERT",
					},
				],
			]);

			const result = await adapter.getLatestState("todos", "r1");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toEqual({ title: "Resurrected" });
			}
		});
	});

	describe("ensureSchema", () => {
		it("checks dataset existence and creates table", async () => {
			const schema: TableSchema = {
				table: "todos",
				columns: [
					{ name: "title", type: "string" },
					{ name: "done", type: "boolean" },
				],
			};

			const result = await adapter.ensureSchema(schema);
			expect(result.ok).toBe(true);
			expect(mockDataset).toHaveBeenCalledWith("test_dataset");
			expect(mockExists).toHaveBeenCalledTimes(1);

			const callArg = mockQuery.mock.calls[0]![0];
			const sql = callArg.query as string;
			expect(sql).toContain("CREATE TABLE IF NOT EXISTS `test_dataset.lakesync_deltas`");
			expect(sql).toContain("delta_id STRING NOT NULL");
			expect(sql).toContain("`table` STRING NOT NULL");
			expect(sql).toContain("columns JSON NOT NULL");
			expect(sql).toContain("hlc INT64 NOT NULL");
			expect(sql).toContain("CLUSTER BY `table`, hlc");
		});

		it("creates dataset when it does not exist", async () => {
			mockExists.mockResolvedValueOnce([false]);

			const schema: TableSchema = {
				table: "todos",
				columns: [{ name: "title", type: "string" }],
			};

			const result = await adapter.ensureSchema(schema);
			expect(result.ok).toBe(true);
			expect(mockCreateDataset).toHaveBeenCalledWith("test_dataset", {
				location: "US",
			});
		});

		it("skips dataset creation when it already exists", async () => {
			const schema: TableSchema = {
				table: "todos",
				columns: [{ name: "title", type: "string" }],
			};

			await adapter.ensureSchema(schema);
			expect(mockCreateDataset).not.toHaveBeenCalled();
		});

		it("returns Err(AdapterError) when query fails", async () => {
			mockQuery.mockRejectedValueOnce(new Error("access denied"));

			const schema: TableSchema = {
				table: "todos",
				columns: [{ name: "title", type: "string" }],
			};

			const result = await adapter.ensureSchema(schema);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error).toBeInstanceOf(AdapterError);
				expect(result.error.message).toBe("Failed to ensure schema");
			}
		});
	});

	describe("close", () => {
		it("resolves without error (no-op)", async () => {
			await expect(adapter.close()).resolves.toBeUndefined();
		});
	});
});
