import type { HLCTimestamp, TableSchema } from "@lakesync/core";
import { AdapterError } from "@lakesync/core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { BigQueryAdapter } from "../bigquery";
import { makeDelta } from "./test-helpers";

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

	describe("materialise", () => {
		const todosSchema: TableSchema = {
			table: "todos",
			columns: [
				{ name: "title", type: "string" },
				{ name: "done", type: "boolean" },
			],
			softDelete: false,
		};

		it("returns Ok without querying when deltas are empty", async () => {
			const result = await adapter.materialise([], [todosSchema]);
			expect(result.ok).toBe(true);
			expect(mockQuery).not.toHaveBeenCalled();
		});

		it("creates destination table with props JSON and synced_at TIMESTAMP", async () => {
			// 1st call: CREATE TABLE
			// 2nd call: SELECT delta history
			mockQuery.mockResolvedValueOnce([[]]).mockResolvedValueOnce([[]]);

			const delta = makeDelta();
			await adapter.materialise([delta], [todosSchema]);

			const createCall = mockQuery.mock.calls[0]![0];
			const sql = createCall.query as string;
			expect(sql).toContain("CREATE TABLE IF NOT EXISTS `test_dataset.todos`");
			expect(sql).toContain("row_id STRING NOT NULL");
			expect(sql).toContain("title STRING");
			expect(sql).toContain("done BOOL");
			expect(sql).toContain("props JSON DEFAULT '{}'");
			expect(sql).toContain("synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()");
		});

		it("generates MERGE SQL that excludes props from UPDATE SET but includes synced_at", async () => {
			// 1st: CREATE TABLE, 2nd: delta history query, 3rd: MERGE
			mockQuery
				.mockResolvedValueOnce([[]])
				.mockResolvedValueOnce([
					[
						{
							row_id: "row-1",
							columns: JSON.stringify([
								{ column: "title", value: "Buy milk" },
								{ column: "done", value: false },
							]),
							op: "INSERT",
						},
					],
				])
				.mockResolvedValueOnce([[]]);

			const delta = makeDelta();
			await adapter.materialise([delta], [todosSchema]);

			expect(mockQuery).toHaveBeenCalledTimes(3);
			const mergeCall = mockQuery.mock.calls[2]![0];
			const sql = mergeCall.query as string;

			expect(sql).toContain("MERGE `test_dataset.todos` AS t");
			expect(sql).toContain("ON t.row_id = s.row_id");
			// UPDATE SET should contain columns + synced_at, but NOT props or deleted_at
			expect(sql).toContain("title = s.title");
			expect(sql).toContain("done = s.done");
			expect(sql).toContain("synced_at = s.synced_at");
			// props should NOT appear in the UPDATE SET line
			const updateLine = sql.split("\n").find((l) => l.includes("UPDATE SET"));
			expect(updateLine).toBeDefined();
			expect(updateLine).not.toContain("props");
			expect(updateLine).not.toContain("deleted_at");
		});

		it("INSERT includes props with default '{}' value", async () => {
			mockQuery
				.mockResolvedValueOnce([[]])
				.mockResolvedValueOnce([
					[
						{
							row_id: "row-1",
							columns: JSON.stringify([
								{ column: "title", value: "Buy milk" },
								{ column: "done", value: false },
							]),
							op: "INSERT",
						},
					],
				])
				.mockResolvedValueOnce([[]]);

			const delta = makeDelta();
			await adapter.materialise([delta], [todosSchema]);

			const mergeCall = mockQuery.mock.calls[2]![0];
			const sql = mergeCall.query as string;

			expect(sql).toContain("WHEN NOT MATCHED THEN INSERT (row_id, title, done, props, synced_at)");
			expect(sql).toContain("VALUES (s.row_id, s.title, s.done, '{}', s.synced_at)");
		});

		it("issues DELETE for tombstoned rows using UNNEST", async () => {
			// 1st: CREATE TABLE, 2nd: delta history, 3rd: DELETE
			mockQuery
				.mockResolvedValueOnce([[]])
				.mockResolvedValueOnce([
					[
						{
							row_id: "row-1",
							columns: JSON.stringify([{ column: "title", value: "Test" }]),
							op: "INSERT",
						},
						{
							row_id: "row-1",
							columns: JSON.stringify([]),
							op: "DELETE",
						},
					],
				])
				.mockResolvedValueOnce([[]]);

			const delta = makeDelta();
			await adapter.materialise([delta], [todosSchema]);

			expect(mockQuery).toHaveBeenCalledTimes(3);
			const deleteCall = mockQuery.mock.calls[2]![0];
			expect(deleteCall.query).toContain("DELETE FROM `test_dataset.todos`");
			expect(deleteCall.query).toContain("WHERE row_id IN UNNEST(@rowIds)");
			expect(deleteCall.params).toEqual({ rowIds: ["row-1"] });
		});

		it("skips tables without matching schema", async () => {
			const delta = makeDelta({ table: "unknown_table" });
			// No query calls expected beyond the early return
			const result = await adapter.materialise([delta], [todosSchema]);
			expect(result.ok).toBe(true);
			expect(mockQuery).not.toHaveBeenCalled();
		});

		it("uses sourceTable for schema matching and destination table name", async () => {
			const renamedSchema: TableSchema = {
				table: "tasks",
				sourceTable: "todos",
				columns: [
					{ name: "title", type: "string" },
					{ name: "done", type: "boolean" },
				],
			};

			// 1st: CREATE TABLE (for "tasks"), 2nd: delta history query
			mockQuery.mockResolvedValueOnce([[]]).mockResolvedValueOnce([[]]);

			const delta = makeDelta({ table: "todos" });
			await adapter.materialise([delta], [renamedSchema]);

			// CREATE TABLE should use the destination name "tasks"
			const createCall = mockQuery.mock.calls[0]![0];
			expect(createCall.query).toContain("CREATE TABLE IF NOT EXISTS `test_dataset.tasks`");

			// Delta query should use sourceTable "todos"
			const historyCall = mockQuery.mock.calls[1]![0];
			expect(historyCall.params.sourceTable).toBe("todos");
		});

		it("returns Err(AdapterError) when query throws", async () => {
			mockQuery.mockRejectedValueOnce(new Error("quota exceeded"));

			const delta = makeDelta();
			const result = await adapter.materialise([delta], [todosSchema]);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error).toBeInstanceOf(AdapterError);
				expect(result.error.message).toBe("Failed to materialise deltas");
			}
		});
	});
});
