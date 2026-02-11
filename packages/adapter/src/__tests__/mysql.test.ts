import type { HLCTimestamp, TableSchema } from "@lakesync/core";
import { AdapterError } from "@lakesync/core";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { MySQLAdapter } from "../mysql";
import { makeDelta } from "./test-helpers";

/** Create an adapter with a mocked pool. */
function createMockedAdapter() {
	const adapter = new MySQLAdapter({
		connectionString: "mysql://localhost/test",
	});

	const mockExecute = vi.fn().mockResolvedValue([[], []]);
	const mockEnd = vi.fn().mockResolvedValue(undefined);

	// Replace pool methods with mocks
	(adapter.pool as unknown as { execute: typeof mockExecute }).execute = mockExecute;
	(adapter.pool as unknown as { end: typeof mockEnd }).end = mockEnd;

	return { adapter, mockExecute, mockEnd };
}

describe("MySQLAdapter", () => {
	let adapter: MySQLAdapter;
	let mockExecute: ReturnType<typeof vi.fn>;
	let mockEnd: ReturnType<typeof vi.fn>;

	beforeEach(() => {
		const mocked = createMockedAdapter();
		adapter = mocked.adapter;
		mockExecute = mocked.mockExecute;
		mockEnd = mocked.mockEnd;
	});

	describe("insertDeltas", () => {
		it("returns Ok for empty deltas array", async () => {
			const result = await adapter.insertDeltas([]);
			expect(result.ok).toBe(true);
			expect(mockExecute).not.toHaveBeenCalled();
		});

		it("generates correct INSERT IGNORE SQL", async () => {
			const delta = makeDelta();
			const result = await adapter.insertDeltas([delta]);

			expect(result.ok).toBe(true);
			expect(mockExecute).toHaveBeenCalledTimes(1);

			const [sql, params] = mockExecute.mock.calls[0]!;
			expect(sql).toContain("INSERT IGNORE INTO lakesync_deltas");
			expect(sql).toContain("`table`");
			expect(sql).toContain("(?, ?, ?, ?, ?, ?, ?)");

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
			expect(mockExecute).toHaveBeenCalledTimes(1);

			const [sql, params] = mockExecute.mock.calls[0]!;
			// Should have two value groups
			const matches = sql.match(/\(\?, \?, \?, \?, \?, \?, \?\)/g);
			expect(matches).toHaveLength(2);
			expect(params).toHaveLength(14);
		});

		it("serialises HLC bigint as string", async () => {
			const delta = makeDelta({ hlc: BigInt("281474976710656") as HLCTimestamp });
			await adapter.insertDeltas([delta]);

			const params = mockExecute.mock.calls[0]![1];
			// hlc is at index 4 (0-based)
			expect(params[4]).toBe("281474976710656");
			expect(typeof params[4]).toBe("string");
		});

		it("returns Err(AdapterError) when query throws", async () => {
			mockExecute.mockRejectedValueOnce(new Error("connection refused"));

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

			const [sql, params] = mockExecute.mock.calls[0]!;
			expect(sql).toContain("WHERE hlc > ?");
			expect(sql).toContain("ORDER BY hlc ASC");
			expect(sql).not.toContain("IN");
			expect(params).toEqual(["500"]);
		});

		it("generates correct SQL with table filter", async () => {
			const hlc = BigInt(500) as HLCTimestamp;
			await adapter.queryDeltasSince(hlc, ["todos", "users"]);

			const [sql, params] = mockExecute.mock.calls[0]!;
			expect(sql).toContain("`table` IN (?, ?)");
			expect(params).toEqual(["500", "todos", "users"]);
		});

		it("converts rows back to RowDelta format", async () => {
			mockExecute.mockResolvedValueOnce([
				[
					{
						delta_id: "d1",
						table: "todos",
						row_id: "r1",
						columns: JSON.stringify([{ column: "title", value: "Test" }]),
						hlc: "1000",
						client_id: "c1",
						op: "INSERT",
					},
				],
				[],
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

		it("handles string-encoded JSON columns", async () => {
			mockExecute.mockResolvedValueOnce([
				[
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
				[],
			]);

			const result = await adapter.queryDeltasSince(BigInt(0) as HLCTimestamp);
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value[0]!.columns).toEqual([{ column: "done", value: true }]);
			}
		});
	});

	describe("getLatestState", () => {
		it("returns null when no deltas exist", async () => {
			mockExecute.mockResolvedValueOnce([[], []]);

			const result = await adapter.getLatestState("todos", "r1");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBeNull();
			}
		});

		it("merges columns with LWW (later values overwrite)", async () => {
			mockExecute.mockResolvedValueOnce([
				[
					{
						columns: JSON.stringify([
							{ column: "title", value: "Old" },
							{ column: "done", value: false },
						]),
						hlc: "1000",
						client_id: "c1",
						op: "INSERT",
					},
					{
						columns: JSON.stringify([{ column: "title", value: "New" }]),
						hlc: "2000",
						client_id: "c1",
						op: "UPDATE",
					},
				],
				[],
			]);

			const result = await adapter.getLatestState("todos", "r1");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toEqual({ title: "New", done: false });
			}
		});

		it("returns null when last delta is a DELETE", async () => {
			mockExecute.mockResolvedValueOnce([
				[
					{
						columns: JSON.stringify([{ column: "title", value: "Test" }]),
						hlc: "1000",
						client_id: "c1",
						op: "INSERT",
					},
					{
						columns: JSON.stringify([]),
						hlc: "2000",
						client_id: "c1",
						op: "DELETE",
					},
				],
				[],
			]);

			const result = await adapter.getLatestState("todos", "r1");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toBeNull();
			}
		});

		it("handles resurrection after DELETE", async () => {
			mockExecute.mockResolvedValueOnce([
				[
					{
						columns: JSON.stringify([{ column: "title", value: "First" }]),
						hlc: "1000",
						client_id: "c1",
						op: "INSERT",
					},
					{
						columns: JSON.stringify([]),
						hlc: "2000",
						client_id: "c1",
						op: "DELETE",
					},
					{
						columns: JSON.stringify([{ column: "title", value: "Resurrected" }]),
						hlc: "3000",
						client_id: "c1",
						op: "INSERT",
					},
				],
				[],
			]);

			const result = await adapter.getLatestState("todos", "r1");
			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toEqual({ title: "Resurrected" });
			}
		});
	});

	describe("ensureSchema", () => {
		it("generates CREATE TABLE with MySQL syntax", async () => {
			const schema: TableSchema = {
				table: "todos",
				columns: [
					{ name: "title", type: "string" },
					{ name: "done", type: "boolean" },
				],
			};

			const result = await adapter.ensureSchema(schema);
			expect(result.ok).toBe(true);
			// Two execute calls: deltas table + user table
			expect(mockExecute).toHaveBeenCalledTimes(2);

			const deltaSql = mockExecute.mock.calls[0]![0] as string;
			expect(deltaSql).toContain("CREATE TABLE IF NOT EXISTS lakesync_deltas");
			expect(deltaSql).toContain("delta_id VARCHAR(255) PRIMARY KEY");
			expect(deltaSql).toContain("`table` VARCHAR(255) NOT NULL");
			expect(deltaSql).toContain("columns JSON NOT NULL");
			expect(deltaSql).toContain("hlc BIGINT NOT NULL");
			expect(deltaSql).toContain("INDEX idx_hlc (hlc)");
			expect(deltaSql).toContain("INDEX idx_table_row");

			const userSql = mockExecute.mock.calls[1]![0] as string;
			expect(userSql).toContain("CREATE TABLE IF NOT EXISTS `todos`");
			expect(userSql).toContain("`title` TEXT");
			expect(userSql).toContain("`done` TINYINT(1)");
			expect(userSql).toContain("props JSON NOT NULL DEFAULT ('{}')");
			expect(userSql).toContain("synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP");
		});

		it("returns Err(AdapterError) when CREATE TABLE fails", async () => {
			mockExecute.mockRejectedValueOnce(new Error("access denied"));

			const schema: TableSchema = {
				table: "todos",
				columns: [{ name: "title", type: "string" }],
			};

			const result = await adapter.ensureSchema(schema);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error).toBeInstanceOf(AdapterError);
			}
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
			expect(mockExecute).not.toHaveBeenCalled();
		});

		it("creates destination table with props JSON and synced_at", async () => {
			// CREATE TABLE, SELECT deltas (returns empty), no upsert/delete needed
			mockExecute
				.mockResolvedValueOnce([[], []]) // CREATE TABLE
				.mockResolvedValueOnce([[], []]); // SELECT deltas

			const delta = makeDelta();
			const result = await adapter.materialise([delta], [schema]);

			expect(result.ok).toBe(true);
			const createSql = mockExecute.mock.calls[0]![0] as string;
			expect(createSql).toContain("CREATE TABLE IF NOT EXISTS `todos`");
			expect(createSql).toContain("props JSON NOT NULL DEFAULT ('{}')");
			expect(createSql).toContain("synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP");
			expect(createSql).toContain("PRIMARY KEY (`row_id`)");
		});

		it("generates UPSERT with ON DUPLICATE KEY UPDATE, excludes props, includes synced_at", async () => {
			mockExecute
				.mockResolvedValueOnce([[], []]) // CREATE TABLE
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
					[],
				]) // SELECT deltas
				.mockResolvedValueOnce([[], []]); // INSERT ... ON DUPLICATE KEY UPDATE

			const delta = makeDelta();
			const result = await adapter.materialise([delta], [schema]);

			expect(result.ok).toBe(true);
			expect(mockExecute).toHaveBeenCalledTimes(3);

			const [upsertSql, upsertParams] = mockExecute.mock.calls[2]!;
			expect(upsertSql).toContain("INSERT INTO `todos`");
			expect(upsertSql).toContain("ON DUPLICATE KEY UPDATE");
			expect(upsertSql).toContain("`title` = VALUES(`title`)");
			expect(upsertSql).toContain("`done` = VALUES(`done`)");
			expect(upsertSql).toContain("synced_at = VALUES(synced_at)");
			expect(upsertSql).not.toContain("props = VALUES(props)");
			expect(upsertSql).not.toContain("`props`");
			// params: row_id, title, done
			expect(upsertParams).toEqual(["row-1", "Buy milk", false]);
		});

		it("generates DELETE for tombstoned rows", async () => {
			mockExecute
				.mockResolvedValueOnce([[], []]) // CREATE TABLE
				.mockResolvedValueOnce([
					[
						{
							row_id: "row-1",
							columns: JSON.stringify([{ column: "title", value: "Buy milk" }]),
							op: "INSERT",
						},
						{
							row_id: "row-1",
							columns: JSON.stringify([]),
							op: "DELETE",
						},
					],
					[],
				]) // SELECT deltas
				.mockResolvedValueOnce([[], []]); // DELETE

			const delta = makeDelta({ op: "DELETE", columns: [] });
			const result = await adapter.materialise([delta], [schema]);

			expect(result.ok).toBe(true);

			const [deleteSql, deleteParams] = mockExecute.mock.calls[2]!;
			expect(deleteSql).toContain("DELETE FROM `todos`");
			expect(deleteSql).toContain("WHERE row_id IN (?)");
			expect(deleteParams).toEqual(["row-1"]);
		});

		it("skips tables without matching schema", async () => {
			const delta = makeDelta({ table: "unknown_table" });
			// CREATE TABLE should not be called for unmatched tables
			const result = await adapter.materialise([delta], [schema]);

			expect(result.ok).toBe(true);
			expect(mockExecute).not.toHaveBeenCalled();
		});

		it("uses sourceTable mapping to match deltas to schemas", async () => {
			const mappedSchema: TableSchema = {
				table: "todo_items",
				sourceTable: "todos",
				columns: [{ name: "title", type: "string" }],
			};

			mockExecute
				.mockResolvedValueOnce([[], []]) // CREATE TABLE
				.mockResolvedValueOnce([
					[
						{
							row_id: "row-1",
							columns: JSON.stringify([{ column: "title", value: "Test" }]),
							op: "INSERT",
						},
					],
					[],
				]) // SELECT deltas
				.mockResolvedValueOnce([[], []]); // UPSERT

			const delta = makeDelta();
			const result = await adapter.materialise([delta], [mappedSchema]);

			expect(result.ok).toBe(true);
			// Should create destination table with the mapped name
			const createSql = mockExecute.mock.calls[0]![0] as string;
			expect(createSql).toContain("CREATE TABLE IF NOT EXISTS `todo_items`");

			// Should upsert into the mapped destination table
			const upsertSql = mockExecute.mock.calls[2]![0] as string;
			expect(upsertSql).toContain("INSERT INTO `todo_items`");
		});

		it("returns Err(AdapterError) when materialise fails", async () => {
			mockExecute.mockRejectedValueOnce(new Error("connection lost"));

			const delta = makeDelta();
			const result = await adapter.materialise([delta], [schema]);

			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error).toBeInstanceOf(AdapterError);
				expect(result.error.message).toBe("Failed to materialise deltas");
			}
		});
	});
});
