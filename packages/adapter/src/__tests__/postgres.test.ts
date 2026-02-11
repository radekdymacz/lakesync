import type { HLCTimestamp, RowDelta, TableSchema } from "@lakesync/core";
import { AdapterError } from "@lakesync/core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { PostgresAdapter } from "../postgres";

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

/** Create an adapter with a mocked pool. */
function createMockedAdapter() {
	const adapter = new PostgresAdapter({
		connectionString: "postgres://localhost/test",
	});

	const mockQuery = vi.fn().mockResolvedValue({ rows: [], rowCount: 0 });
	const mockEnd = vi.fn().mockResolvedValue(undefined);

	// Replace pool methods with mocks
	(adapter.pool as unknown as { query: typeof mockQuery }).query = mockQuery;
	(adapter.pool as unknown as { end: typeof mockEnd }).end = mockEnd;

	return { adapter, mockQuery, mockEnd };
}

describe("PostgresAdapter", () => {
	let adapter: PostgresAdapter;
	let mockQuery: ReturnType<typeof vi.fn>;
	let mockEnd: ReturnType<typeof vi.fn>;

	beforeEach(() => {
		const mocked = createMockedAdapter();
		adapter = mocked.adapter;
		mockQuery = mocked.mockQuery;
		mockEnd = mocked.mockEnd;
	});

	describe("insertDeltas", () => {
		it("returns Ok for empty deltas array", async () => {
			const result = await adapter.insertDeltas([]);
			expect(result.ok).toBe(true);
			expect(mockQuery).not.toHaveBeenCalled();
		});

		it("generates correct INSERT SQL with ON CONFLICT", async () => {
			const delta = makeDelta();
			const result = await adapter.insertDeltas([delta]);

			expect(result.ok).toBe(true);
			expect(mockQuery).toHaveBeenCalledTimes(1);

			const [sql, params] = mockQuery.mock.calls[0]!;
			expect(sql).toContain("INSERT INTO lakesync_deltas");
			expect(sql).toContain("ON CONFLICT (delta_id) DO NOTHING");
			expect(sql).toContain('"table"');

			expect(params).toEqual([
				"delta-1",
				"todos",
				"row-1",
				JSON.stringify([{ column: "title", value: "Buy milk" }]),
				"1000",
				"client-a",
				"INSERT",
			]);
		});

		it("batches multiple deltas into a single INSERT", async () => {
			const deltas = [
				makeDelta({ deltaId: "d1", rowId: "r1" }),
				makeDelta({ deltaId: "d2", rowId: "r2" }),
			];

			const result = await adapter.insertDeltas(deltas);
			expect(result.ok).toBe(true);
			expect(mockQuery).toHaveBeenCalledTimes(1);

			const [sql, params] = mockQuery.mock.calls[0]!;
			// Should have two value groups
			expect(sql).toContain("($1, $2, $3, $4, $5, $6, $7)");
			expect(sql).toContain("($8, $9, $10, $11, $12, $13, $14)");
			expect(params).toHaveLength(14);
		});

		it("serialises HLC bigint as string", async () => {
			const delta = makeDelta({ hlc: BigInt("281474976710656") as HLCTimestamp });
			await adapter.insertDeltas([delta]);

			const params = mockQuery.mock.calls[0]![1];
			// hlc is at index 4 (0-based)
			expect(params[4]).toBe("281474976710656");
			expect(typeof params[4]).toBe("string");
		});

		it("returns Err(AdapterError) when query throws", async () => {
			mockQuery.mockRejectedValueOnce(new Error("connection refused"));

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

			const [sql, params] = mockQuery.mock.calls[0]!;
			expect(sql).toContain("WHERE hlc > $1");
			expect(sql).toContain("ORDER BY hlc ASC");
			expect(sql).not.toContain("ANY");
			expect(params).toEqual(["500"]);
		});

		it("generates correct SQL with table filter", async () => {
			const hlc = BigInt(500) as HLCTimestamp;
			await adapter.queryDeltasSince(hlc, ["todos", "users"]);

			const [sql, params] = mockQuery.mock.calls[0]!;
			expect(sql).toContain('"table" = ANY($2)');
			expect(params).toEqual(["500", ["todos", "users"]]);
		});

		it("converts rows back to RowDelta format", async () => {
			mockQuery.mockResolvedValueOnce({
				rows: [
					{
						delta_id: "d1",
						table: "todos",
						row_id: "r1",
						columns: [{ column: "title", value: "Test" }],
						hlc: "1000",
						client_id: "c1",
						op: "INSERT",
					},
				],
				rowCount: 1,
			});

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

		it("handles string-encoded JSONB columns", async () => {
			mockQuery.mockResolvedValueOnce({
				rows: [
					{
						delta_id: "d1",
						table: "todos",
						row_id: "r1",
						columns: JSON.stringify([{ column: "done", value: true }]),
						hlc: "2000",
						client_id: "c1",
						op: "UPDATE",
					},
				],
				rowCount: 1,
			});

			const result = await adapter.queryDeltasSince(BigInt(0) as HLCTimestamp);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value[0]!.columns).toEqual([{ column: "done", value: true }]);
			}
		});
	});

	describe("getLatestState", () => {
		it("returns null when no deltas exist", async () => {
			mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 });

			const result = await adapter.getLatestState("todos", "r1");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBeNull();
			}
		});

		it("merges columns with LWW (later values overwrite)", async () => {
			mockQuery.mockResolvedValueOnce({
				rows: [
					{
						columns: [
							{ column: "title", value: "Old" },
							{ column: "done", value: false },
						],
						hlc: "1000",
						client_id: "c1",
						op: "INSERT",
					},
					{
						columns: [{ column: "title", value: "New" }],
						hlc: "2000",
						client_id: "c1",
						op: "UPDATE",
					},
				],
				rowCount: 2,
			});

			const result = await adapter.getLatestState("todos", "r1");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toEqual({ title: "New", done: false });
			}
		});

		it("returns null when last delta is a DELETE", async () => {
			mockQuery.mockResolvedValueOnce({
				rows: [
					{
						columns: [{ column: "title", value: "Test" }],
						hlc: "1000",
						client_id: "c1",
						op: "INSERT",
					},
					{
						columns: [],
						hlc: "2000",
						client_id: "c1",
						op: "DELETE",
					},
				],
				rowCount: 2,
			});

			const result = await adapter.getLatestState("todos", "r1");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBeNull();
			}
		});

		it("handles resurrection after DELETE", async () => {
			mockQuery.mockResolvedValueOnce({
				rows: [
					{
						columns: [{ column: "title", value: "First" }],
						hlc: "1000",
						client_id: "c1",
						op: "INSERT",
					},
					{
						columns: [],
						hlc: "2000",
						client_id: "c1",
						op: "DELETE",
					},
					{
						columns: [{ column: "title", value: "Resurrected" }],
						hlc: "3000",
						client_id: "c1",
						op: "INSERT",
					},
				],
				rowCount: 3,
			});

			const result = await adapter.getLatestState("todos", "r1");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toEqual({ title: "Resurrected" });
			}
		});
	});

	describe("ensureSchema", () => {
		it("generates CREATE TABLE and CREATE INDEX statements", async () => {
			const schema: TableSchema = {
				table: "todos",
				columns: [
					{ name: "title", type: "string" },
					{ name: "done", type: "boolean" },
				],
			};

			const result = await adapter.ensureSchema(schema);
			expect(result.ok).toBe(true);
			expect(mockQuery).toHaveBeenCalledTimes(1);

			const sql = mockQuery.mock.calls[0]![0] as string;
			expect(sql).toContain("CREATE TABLE IF NOT EXISTS lakesync_deltas");
			expect(sql).toContain("delta_id TEXT PRIMARY KEY");
			expect(sql).toContain('"table" TEXT NOT NULL');
			expect(sql).toContain("columns JSONB NOT NULL");
			expect(sql).toContain("hlc BIGINT NOT NULL");
			expect(sql).toContain("CREATE INDEX IF NOT EXISTS idx_lakesync_deltas_hlc");
			expect(sql).toContain("CREATE INDEX IF NOT EXISTS idx_lakesync_deltas_table_row");
		});
	});

	describe("close", () => {
		it("calls pool.end()", async () => {
			await adapter.close();
			expect(mockEnd).toHaveBeenCalledTimes(1);
		});
	});

	describe("materialise", () => {
		const schema: TableSchema = {
			table: "todos",
			columns: [
				{ name: "title", type: "string" },
				{ name: "done", type: "boolean" },
			],
			softDelete: false,
		};

		it("returns Ok without querying for empty deltas", async () => {
			const result = await adapter.materialise([], [schema]);
			expect(result.ok).toBe(true);
			expect(mockQuery).not.toHaveBeenCalled();
		});

		it("creates destination table with typed columns, props JSONB, and synced_at TIMESTAMPTZ", async () => {
			// CREATE TABLE
			mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 });
			// SELECT deltas
			mockQuery.mockResolvedValueOnce({
				rows: [
					{
						row_id: "r1",
						columns: [
							{ column: "title", value: "Buy milk" },
							{ column: "done", value: false },
						],
						op: "INSERT",
					},
				],
				rowCount: 1,
			});
			// UPSERT
			mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 1 });

			const result = await adapter.materialise([makeDelta()], [schema]);
			expect(result.ok).toBe(true);

			const createSql = mockQuery.mock.calls[0]![0] as string;
			expect(createSql).toContain('CREATE TABLE IF NOT EXISTS "todos"');
			expect(createSql).toContain('"title" TEXT');
			expect(createSql).toContain('"done" BOOLEAN');
			expect(createSql).toContain("props JSONB NOT NULL DEFAULT '{}'");
			expect(createSql).toContain("synced_at TIMESTAMPTZ NOT NULL DEFAULT now()");
			expect(createSql).toContain('PRIMARY KEY ("row_id")');
		});

		it("UPSERT SQL excludes props and includes synced_at in SET clause", async () => {
			mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 });
			mockQuery.mockResolvedValueOnce({
				rows: [
					{
						row_id: "r1",
						columns: [
							{ column: "title", value: "Test" },
							{ column: "done", value: true },
						],
						op: "INSERT",
					},
				],
				rowCount: 1,
			});
			mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 1 });

			await adapter.materialise([makeDelta()], [schema]);

			const upsertSql = mockQuery.mock.calls[2]![0] as string;
			expect(upsertSql).toContain("INSERT INTO");
			expect(upsertSql).toContain('ON CONFLICT ("row_id") DO UPDATE SET');
			expect(upsertSql).toContain('"title" = EXCLUDED."title"');
			expect(upsertSql).toContain('"done" = EXCLUDED."done"');
			expect(upsertSql).toContain('"synced_at" = EXCLUDED."synced_at"');
			expect(upsertSql).not.toContain("props = EXCLUDED");
			expect(upsertSql).not.toContain('"props" = EXCLUDED');
		});

		it("deletes tombstoned rows", async () => {
			mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 });
			mockQuery.mockResolvedValueOnce({
				rows: [
					{
						row_id: "r1",
						columns: [{ column: "title", value: "Test" }],
						op: "INSERT",
					},
					{
						row_id: "r1",
						columns: [],
						op: "DELETE",
					},
				],
				rowCount: 2,
			});
			// DELETE
			mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 1 });

			const result = await adapter.materialise([makeDelta()], [schema]);
			expect(result.ok).toBe(true);

			// 3 calls: CREATE TABLE, SELECT, DELETE
			expect(mockQuery).toHaveBeenCalledTimes(3);
			const deleteSql = mockQuery.mock.calls[2]![0] as string;
			expect(deleteSql).toContain('DELETE FROM "todos"');
			expect(deleteSql).toContain("row_id = ANY($1)");
			expect(mockQuery.mock.calls[2]![1]).toEqual([["r1"]]);
		});

		it("skips tables without matching schema", async () => {
			const delta = makeDelta({ table: "unknown_table" });
			const result = await adapter.materialise([delta], [schema]);
			expect(result.ok).toBe(true);
			// No queries should be made (no matching schema)
			expect(mockQuery).not.toHaveBeenCalled();
		});

		it("uses sourceTable mapping for delta matching and destination table name", async () => {
			const mappedSchema: TableSchema = {
				table: "tickets",
				sourceTable: "jira_issues",
				columns: [
					{ name: "summary", type: "string" },
					{ name: "priority", type: "number" },
				],
			};

			const delta = makeDelta({
				table: "jira_issues",
				rowId: "issue-1",
				columns: [
					{ column: "summary", value: "Fix bug" },
					{ column: "priority", value: 1 },
				],
			});

			// CREATE TABLE
			mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 0 });
			// SELECT deltas — query uses sourceTable ("jira_issues")
			mockQuery.mockResolvedValueOnce({
				rows: [
					{
						row_id: "issue-1",
						columns: [
							{ column: "summary", value: "Fix bug" },
							{ column: "priority", value: 1 },
						],
						op: "INSERT",
					},
				],
				rowCount: 1,
			});
			// UPSERT
			mockQuery.mockResolvedValueOnce({ rows: [], rowCount: 1 });

			const result = await adapter.materialise([delta], [mappedSchema]);
			expect(result.ok).toBe(true);

			// CREATE TABLE uses destination name "tickets"
			const createSql = mockQuery.mock.calls[0]![0] as string;
			expect(createSql).toContain('CREATE TABLE IF NOT EXISTS "tickets"');
			expect(createSql).toContain('"summary" TEXT');
			expect(createSql).toContain('"priority" DOUBLE PRECISION');

			// SELECT uses source table name "jira_issues"
			const selectParams = mockQuery.mock.calls[1]![1] as unknown[];
			expect(selectParams[0]).toBe("jira_issues");

			// UPSERT targets destination "tickets"
			const upsertSql = mockQuery.mock.calls[2]![0] as string;
			expect(upsertSql).toContain('INSERT INTO "tickets"');
		});

		it("returns Err(AdapterError) when query throws", async () => {
			mockQuery.mockRejectedValueOnce(new Error("connection refused"));

			const result = await adapter.materialise([makeDelta()], [schema]);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error).toBeInstanceOf(AdapterError);
				expect(result.error.message).toBe("Failed to materialise deltas");
			}
		});
	});
});
