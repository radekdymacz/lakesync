import { HLC } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { convertChangesToDeltas } from "../cdc/cdc-source";
import {
	buildTriggerSql,
	type ChangelogRow,
	extractColumnsFromJson,
	mysqlTypeToColumnType,
	parseChangelogRows,
} from "../cdc/mysql-dialect";

// ---------------------------------------------------------------------------
// Unit tests — parseChangelogRows (ChangelogRow[] -> CdcRawChange[])
// ---------------------------------------------------------------------------

describe("parseChangelogRows", () => {
	describe("INSERT", () => {
		it("converts a simple insert changelog row to a CdcRawChange", () => {
			const rows: ChangelogRow[] = [
				{
					id: 1,
					table_name: "users",
					row_id: "42",
					op: "insert",
					columns: JSON.stringify([
						{ column: "id", value: 42 },
						{ column: "name", value: "Alice" },
						{ column: "email", value: "alice@example.com" },
					]),
					captured_at: 1700000000000,
				},
			];

			const result = parseChangelogRows(rows);

			expect(result).toHaveLength(1);
			const r = result[0]!;
			expect(r.kind).toBe("insert");
			expect(r.schema).toBe("");
			expect(r.table).toBe("users");
			expect(r.rowId).toBe("42");
			expect(r.columns).toEqual([
				{ column: "id", value: 42 },
				{ column: "name", value: "Alice" },
				{ column: "email", value: "alice@example.com" },
			]);
		});
	});

	describe("UPDATE", () => {
		it("converts an update changelog row to a CdcRawChange with all columns", () => {
			const rows: ChangelogRow[] = [
				{
					id: 2,
					table_name: "users",
					row_id: "42",
					op: "update",
					columns: JSON.stringify([
						{ column: "id", value: 42 },
						{ column: "name", value: "Alice Smith" },
						{ column: "email", value: "alice@example.com" },
					]),
					captured_at: 1700000001000,
				},
			];

			const result = parseChangelogRows(rows);

			expect(result).toHaveLength(1);
			const r = result[0]!;
			expect(r.kind).toBe("update");
			expect(r.table).toBe("users");
			expect(r.rowId).toBe("42");
			expect(r.columns).toEqual([
				{ column: "id", value: 42 },
				{ column: "name", value: "Alice Smith" },
				{ column: "email", value: "alice@example.com" },
			]);
		});
	});

	describe("DELETE", () => {
		it("converts a delete changelog row to a CdcRawChange with empty columns", () => {
			const rows: ChangelogRow[] = [
				{
					id: 3,
					table_name: "users",
					row_id: "42",
					op: "delete",
					columns: null,
					captured_at: 1700000002000,
				},
			];

			const result = parseChangelogRows(rows);

			expect(result).toHaveLength(1);
			const r = result[0]!;
			expect(r.kind).toBe("delete");
			expect(r.table).toBe("users");
			expect(r.rowId).toBe("42");
			expect(r.columns).toEqual([]);
		});
	});

	describe("mixed operations in a single batch", () => {
		it("handles insert, update, and delete together", () => {
			const rows: ChangelogRow[] = [
				{
					id: 1,
					table_name: "users",
					row_id: "1",
					op: "insert",
					columns: JSON.stringify([
						{ column: "id", value: 1 },
						{ column: "name", value: "Alice" },
					]),
					captured_at: 1700000000000,
				},
				{
					id: 2,
					table_name: "users",
					row_id: "2",
					op: "update",
					columns: JSON.stringify([
						{ column: "id", value: 2 },
						{ column: "name", value: "Bob Updated" },
					]),
					captured_at: 1700000001000,
				},
				{
					id: 3,
					table_name: "users",
					row_id: "3",
					op: "delete",
					columns: null,
					captured_at: 1700000002000,
				},
			];

			const result = parseChangelogRows(rows);

			expect(result).toHaveLength(3);
			expect(result[0]!.kind).toBe("insert");
			expect(result[1]!.kind).toBe("update");
			expect(result[2]!.kind).toBe("delete");
		});
	});

	describe("composite primary keys", () => {
		it("handles composite row_id (joined with colon)", () => {
			const rows: ChangelogRow[] = [
				{
					id: 1,
					table_name: "order_items",
					row_id: "100:200",
					op: "update",
					columns: JSON.stringify([
						{ column: "order_id", value: 100 },
						{ column: "product_id", value: 200 },
						{ column: "quantity", value: 5 },
					]),
					captured_at: 1700000000000,
				},
			];

			const result = parseChangelogRows(rows);
			expect(result[0]!.rowId).toBe("100:200");
		});

		it("handles composite row_id on delete", () => {
			const rows: ChangelogRow[] = [
				{
					id: 2,
					table_name: "order_items",
					row_id: "100:200",
					op: "delete",
					columns: null,
					captured_at: 1700000001000,
				},
			];

			const result = parseChangelogRows(rows);
			expect(result[0]!.rowId).toBe("100:200");
		});
	});

	describe("null column values", () => {
		it("preserves null values as null (not undefined)", () => {
			const rows: ChangelogRow[] = [
				{
					id: 1,
					table_name: "users",
					row_id: "1",
					op: "insert",
					columns: JSON.stringify([
						{ column: "id", value: 1 },
						{ column: "name", value: "Alice" },
						{ column: "email", value: null },
					]),
					captured_at: 1700000000000,
				},
			];

			const result = parseChangelogRows(rows);
			expect(result[0]!.columns[2]).toEqual({ column: "email", value: null });
		});
	});
});

// ---------------------------------------------------------------------------
// Unit tests — extractColumnsFromJson
// ---------------------------------------------------------------------------

describe("extractColumnsFromJson", () => {
	it("returns empty array for delete operations", () => {
		expect(extractColumnsFromJson(null, "delete")).toEqual([]);
	});

	it("returns empty array when JSON is null", () => {
		expect(extractColumnsFromJson(null, "insert")).toEqual([]);
	});

	it("parses a valid JSON array of column/value objects", () => {
		const json = JSON.stringify([
			{ column: "id", value: 1 },
			{ column: "name", value: "Alice" },
		]);

		const result = extractColumnsFromJson(json, "insert");
		expect(result).toEqual([
			{ column: "id", value: 1 },
			{ column: "name", value: "Alice" },
		]);
	});

	it("handles already-parsed JSON (non-string input)", () => {
		const parsed = [
			{ column: "id", value: 1 },
			{ column: "name", value: "Bob" },
		];

		// mysql2 can return already-parsed JSON objects
		const result = extractColumnsFromJson(parsed as unknown as string, "insert");
		expect(result).toEqual([
			{ column: "id", value: 1 },
			{ column: "name", value: "Bob" },
		]);
	});

	it("skips malformed entries without column/value keys", () => {
		const json = JSON.stringify([
			{ column: "id", value: 1 },
			{ foo: "bar" },
			{ column: "name", value: "Alice" },
		]);

		const result = extractColumnsFromJson(json, "insert");
		expect(result).toEqual([
			{ column: "id", value: 1 },
			{ column: "name", value: "Alice" },
		]);
	});

	it("returns empty array for non-array JSON", () => {
		const json = JSON.stringify({ column: "id", value: 1 });
		const result = extractColumnsFromJson(json, "insert");
		expect(result).toEqual([]);
	});
});

// ---------------------------------------------------------------------------
// Unit tests — buildTriggerSql
// ---------------------------------------------------------------------------

describe("buildTriggerSql", () => {
	const columns = ["id", "name", "email"];
	const pkColumns = ["id"];
	const changelogTable = "_lakesync_cdc_log";

	describe("trigger naming", () => {
		it("generates correct INSERT trigger name", () => {
			const result = buildTriggerSql("users", "insert", columns, pkColumns, changelogTable);
			expect(result.triggerName).toBe("_lakesync_cdc_users_ai");
		});

		it("generates correct UPDATE trigger name", () => {
			const result = buildTriggerSql("users", "update", columns, pkColumns, changelogTable);
			expect(result.triggerName).toBe("_lakesync_cdc_users_au");
		});

		it("generates correct DELETE trigger name", () => {
			const result = buildTriggerSql("users", "delete", columns, pkColumns, changelogTable);
			expect(result.triggerName).toBe("_lakesync_cdc_users_ad");
		});
	});

	describe("INSERT trigger", () => {
		it("generates AFTER INSERT trigger with NEW prefix", () => {
			const result = buildTriggerSql("users", "insert", columns, pkColumns, changelogTable);
			expect(result.sql).toContain("AFTER INSERT ON `users`");
			expect(result.sql).toContain("NEW.`id`");
			expect(result.sql).toContain("NEW.`name`");
			expect(result.sql).toContain("NEW.`email`");
			expect(result.sql).toContain("'insert'");
			expect(result.sql).toContain(changelogTable);
		});
	});

	describe("UPDATE trigger", () => {
		it("generates AFTER UPDATE trigger with OLD rowId and NEW columns", () => {
			const result = buildTriggerSql("users", "update", columns, pkColumns, changelogTable);
			expect(result.sql).toContain("AFTER UPDATE ON `users`");
			// rowId uses OLD prefix for updates (to track the original row)
			expect(result.sql).toContain("CAST(OLD.`id` AS CHAR)");
			// columns use NEW prefix (new values)
			expect(result.sql).toContain("NEW.`name`");
			expect(result.sql).toContain("'update'");
		});
	});

	describe("DELETE trigger", () => {
		it("generates AFTER DELETE trigger with NULL columns", () => {
			const result = buildTriggerSql("users", "delete", columns, pkColumns, changelogTable);
			expect(result.sql).toContain("AFTER DELETE ON `users`");
			expect(result.sql).toContain("CAST(OLD.`id` AS CHAR)");
			expect(result.sql).toContain("NULL");
			expect(result.sql).toContain("'delete'");
		});
	});

	describe("composite primary keys", () => {
		it("uses CONCAT with colon separator for multiple PK columns", () => {
			const compositePk = ["order_id", "product_id"];
			const cols = ["order_id", "product_id", "quantity"];

			const result = buildTriggerSql("order_items", "insert", cols, compositePk, changelogTable);
			expect(result.sql).toContain("CONCAT(");
			expect(result.sql).toContain("CAST(NEW.`order_id` AS CHAR)");
			expect(result.sql).toContain("':'");
			expect(result.sql).toContain("CAST(NEW.`product_id` AS CHAR)");
		});
	});

	describe("no primary key fallback", () => {
		it("uses first column as rowId when no PK columns exist", () => {
			const result = buildTriggerSql("logs", "insert", columns, [], changelogTable);
			expect(result.sql).toContain("CAST(NEW.`id` AS CHAR)");
		});
	});
});

// ---------------------------------------------------------------------------
// Unit tests — mysqlTypeToColumnType
// ---------------------------------------------------------------------------

describe("mysqlTypeToColumnType", () => {
	it("maps integer types to number", () => {
		expect(mysqlTypeToColumnType("int")).toBe("number");
		expect(mysqlTypeToColumnType("bigint")).toBe("number");
		expect(mysqlTypeToColumnType("smallint")).toBe("number");
		expect(mysqlTypeToColumnType("mediumint")).toBe("number");
	});

	it("maps float/double/decimal to number", () => {
		expect(mysqlTypeToColumnType("float")).toBe("number");
		expect(mysqlTypeToColumnType("double")).toBe("number");
		expect(mysqlTypeToColumnType("decimal")).toBe("number");
		expect(mysqlTypeToColumnType("numeric")).toBe("number");
	});

	it("maps tinyint to boolean", () => {
		expect(mysqlTypeToColumnType("tinyint")).toBe("boolean");
	});

	it("maps bool/boolean to boolean", () => {
		expect(mysqlTypeToColumnType("bool")).toBe("boolean");
		expect(mysqlTypeToColumnType("boolean")).toBe("boolean");
	});

	it("maps json to json", () => {
		expect(mysqlTypeToColumnType("json")).toBe("json");
	});

	it("maps string types to string", () => {
		expect(mysqlTypeToColumnType("varchar")).toBe("string");
		expect(mysqlTypeToColumnType("text")).toBe("string");
		expect(mysqlTypeToColumnType("char")).toBe("string");
		expect(mysqlTypeToColumnType("blob")).toBe("string");
		expect(mysqlTypeToColumnType("date")).toBe("string");
		expect(mysqlTypeToColumnType("datetime")).toBe("string");
		expect(mysqlTypeToColumnType("timestamp")).toBe("string");
		expect(mysqlTypeToColumnType("enum")).toBe("string");
	});
});

// ---------------------------------------------------------------------------
// Unit tests — convertChangesToDeltas with MySQL changelog data
// ---------------------------------------------------------------------------

describe("convertChangesToDeltas (MySQL changelog)", () => {
	const hlc = HLC.encode(1_700_000_000_000, 0);
	const clientId = "cdc:mysql:_lakesync_cdc_log";

	it("converts MySQL changelog CdcRawChanges to RowDeltas with correct ops", async () => {
		const changes = parseChangelogRows([
			{
				id: 1,
				table_name: "users",
				row_id: "1",
				op: "insert",
				columns: JSON.stringify([
					{ column: "id", value: 1 },
					{ column: "name", value: "Alice" },
				]),
				captured_at: 1700000000000,
			},
			{
				id: 2,
				table_name: "users",
				row_id: "2",
				op: "update",
				columns: JSON.stringify([
					{ column: "id", value: 2 },
					{ column: "name", value: "Bob" },
				]),
				captured_at: 1700000001000,
			},
			{
				id: 3,
				table_name: "users",
				row_id: "3",
				op: "delete",
				columns: null,
				captured_at: 1700000002000,
			},
		]);

		const deltas = await convertChangesToDeltas(changes, hlc, clientId, null);

		expect(deltas).toHaveLength(3);
		expect(deltas[0]!.op).toBe("INSERT");
		expect(deltas[0]!.table).toBe("users");
		expect(deltas[0]!.rowId).toBe("1");
		expect(deltas[0]!.clientId).toBe(clientId);
		expect(deltas[0]!.hlc).toBe(hlc);
		expect(deltas[0]!.deltaId).toMatch(/^[a-f0-9]{64}$/);

		expect(deltas[1]!.op).toBe("UPDATE");
		expect(deltas[1]!.rowId).toBe("2");

		expect(deltas[2]!.op).toBe("DELETE");
		expect(deltas[2]!.rowId).toBe("3");
		expect(deltas[2]!.columns).toEqual([]);
	});

	it("filters by table set", async () => {
		const changes = parseChangelogRows([
			{
				id: 1,
				table_name: "users",
				row_id: "1",
				op: "insert",
				columns: JSON.stringify([{ column: "id", value: 1 }]),
				captured_at: 1700000000000,
			},
			{
				id: 2,
				table_name: "orders",
				row_id: "1",
				op: "insert",
				columns: JSON.stringify([{ column: "id", value: 1 }]),
				captured_at: 1700000001000,
			},
			{
				id: 3,
				table_name: "users",
				row_id: "2",
				op: "insert",
				columns: JSON.stringify([{ column: "id", value: 2 }]),
				captured_at: 1700000002000,
			},
		]);

		const tables = new Set(["users"]);
		const deltas = await convertChangesToDeltas(changes, hlc, clientId, tables);

		expect(deltas).toHaveLength(2);
		expect(deltas.every((d) => d.table === "users")).toBe(true);
	});

	describe("deterministic deltaId", () => {
		it("produces the same deltaId for identical inputs", async () => {
			const changes = parseChangelogRows([
				{
					id: 1,
					table_name: "users",
					row_id: "1",
					op: "insert",
					columns: JSON.stringify([
						{ column: "id", value: 1 },
						{ column: "name", value: "Alice" },
					]),
					captured_at: 1700000000000,
				},
			]);

			const deltas1 = await convertChangesToDeltas(changes, hlc, clientId, null);
			const deltas2 = await convertChangesToDeltas(changes, hlc, clientId, null);

			expect(deltas1[0]!.deltaId).toBe(deltas2[0]!.deltaId);
		});

		it("produces different deltaIds for different data", async () => {
			const changes1 = parseChangelogRows([
				{
					id: 1,
					table_name: "users",
					row_id: "1",
					op: "insert",
					columns: JSON.stringify([
						{ column: "id", value: 1 },
						{ column: "name", value: "Alice" },
					]),
					captured_at: 1700000000000,
				},
			]);

			const changes2 = parseChangelogRows([
				{
					id: 2,
					table_name: "users",
					row_id: "1",
					op: "insert",
					columns: JSON.stringify([
						{ column: "id", value: 1 },
						{ column: "name", value: "Bob" },
					]),
					captured_at: 1700000000000,
				},
			]);

			const deltas1 = await convertChangesToDeltas(changes1, hlc, clientId, null);
			const deltas2 = await convertChangesToDeltas(changes2, hlc, clientId, null);

			expect(deltas1[0]!.deltaId).not.toBe(deltas2[0]!.deltaId);
		});
	});
});

// ---------------------------------------------------------------------------
// Cursor management (via CdcSource + MySqlCdcDialect)
// ---------------------------------------------------------------------------

describe("MySqlCdcDialect cursor", () => {
	// We can't instantiate MySqlCdcDialect without a real MySQL connection,
	// but we can test the default cursor shape and verify it matches the
	// expected format that fetchChanges uses.

	it("default cursor shape is { lastId: 0 }", async () => {
		// Import dynamically to avoid triggering mysql2 connection
		const { MySqlCdcDialect } = await import("../cdc/mysql-dialect");
		const dialect = new MySqlCdcDialect({
			connectionString: "mysql://localhost/test",
		});

		expect(dialect.defaultCursor()).toEqual({ lastId: 0 });
	});

	it("uses correct dialect name", async () => {
		const { MySqlCdcDialect } = await import("../cdc/mysql-dialect");
		const dialect = new MySqlCdcDialect({
			connectionString: "mysql://localhost/test",
		});

		expect(dialect.name).toBe("mysql:_lakesync_cdc_log");
	});

	it("uses custom changelog table in name", async () => {
		const { MySqlCdcDialect } = await import("../cdc/mysql-dialect");
		const dialect = new MySqlCdcDialect({
			connectionString: "mysql://localhost/test",
			changelogTable: "my_cdc_log",
		});

		expect(dialect.name).toBe("mysql:my_cdc_log");
	});

	it("close is safe to call without connect", async () => {
		const { MySqlCdcDialect } = await import("../cdc/mysql-dialect");
		const dialect = new MySqlCdcDialect({
			connectionString: "mysql://localhost/test",
		});

		// Should not throw
		await dialect.close();
		await dialect.close();
	});
});

// ---------------------------------------------------------------------------
// Integration tests — require a running MySQL instance
// ---------------------------------------------------------------------------

describe.skipIf(!process.env.MYSQL_URL)("MySqlCdcDialect integration", () => {
	it("placeholder — requires MYSQL_URL env var", () => {
		expect(true).toBe(true);
	});
});
