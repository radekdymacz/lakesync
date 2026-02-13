import { describe, expect, it } from "vitest";
import {
	type MsSqlCdcRow,
	MsSqlCdcDialect,
	bufferToHex,
	compareLsn,
	deriveCaptureInstanceName,
	hexToBuffer,
	mapOperationToKind,
	parseMsSqlCdcRows,
} from "../cdc/mssql-dialect";

// ---------------------------------------------------------------------------
// Unit tests — parseMsSqlCdcRows (returns CdcRawChange[])
// ---------------------------------------------------------------------------

describe("parseMsSqlCdcRows", () => {
	describe("INSERT (operation = 2)", () => {
		it("converts an insert row to a CdcRawChange", () => {
			const rows: MsSqlCdcRow[] = [
				{
					__$operation: 2,
					__$start_lsn: Buffer.alloc(10),
					id: 1,
					name: "Alice",
					email: "alice@example.com",
				},
			];

			const result = parseMsSqlCdcRows(rows, "users", "dbo");

			expect(result).toHaveLength(1);
			const r = result[0]!;
			expect(r.kind).toBe("insert");
			expect(r.schema).toBe("dbo");
			expect(r.table).toBe("users");
			expect(r.rowId).toBe("1");
			expect(r.columns).toEqual([
				{ column: "id", value: 1 },
				{ column: "name", value: "Alice" },
				{ column: "email", value: "alice@example.com" },
			]);
		});
	});

	describe("UPDATE (operation = 4)", () => {
		it("converts an update after-image row to a CdcRawChange", () => {
			const rows: MsSqlCdcRow[] = [
				{
					__$operation: 4,
					__$start_lsn: Buffer.alloc(10),
					id: 1,
					name: "Alice Smith",
					email: "alice@example.com",
				},
			];

			const result = parseMsSqlCdcRows(rows, "users", "dbo");

			expect(result).toHaveLength(1);
			const r = result[0]!;
			expect(r.kind).toBe("update");
			expect(r.table).toBe("users");
			expect(r.rowId).toBe("1");
			expect(r.columns).toEqual([
				{ column: "id", value: 1 },
				{ column: "name", value: "Alice Smith" },
				{ column: "email", value: "alice@example.com" },
			]);
		});
	});

	describe("UPDATE before-image (operation = 3)", () => {
		it("skips update before-image rows", () => {
			const rows: MsSqlCdcRow[] = [
				{
					__$operation: 3,
					__$start_lsn: Buffer.alloc(10),
					id: 1,
					name: "Alice",
					email: "alice@example.com",
				},
				{
					__$operation: 4,
					__$start_lsn: Buffer.alloc(10),
					id: 1,
					name: "Alice Smith",
					email: "alice@example.com",
				},
			];

			const result = parseMsSqlCdcRows(rows, "users", "dbo");

			expect(result).toHaveLength(1);
			expect(result[0]!.kind).toBe("update");
			expect(result[0]!.columns).toEqual([
				{ column: "id", value: 1 },
				{ column: "name", value: "Alice Smith" },
				{ column: "email", value: "alice@example.com" },
			]);
		});
	});

	describe("DELETE (operation = 1)", () => {
		it("converts a delete row to a CdcRawChange with empty columns", () => {
			const rows: MsSqlCdcRow[] = [
				{
					__$operation: 1,
					__$start_lsn: Buffer.alloc(10),
					id: 42,
					name: "Bob",
				},
			];

			const result = parseMsSqlCdcRows(rows, "users", "dbo");

			expect(result).toHaveLength(1);
			const r = result[0]!;
			expect(r.kind).toBe("delete");
			expect(r.table).toBe("users");
			expect(r.rowId).toBe("42");
			expect(r.columns).toEqual([]);
		});
	});

	describe("CDC system columns", () => {
		it("excludes all CDC system columns from user data", () => {
			const rows: MsSqlCdcRow[] = [
				{
					__$operation: 2,
					__$start_lsn: Buffer.alloc(10),
					__$end_lsn: Buffer.alloc(10),
					__$seqval: Buffer.alloc(10),
					__$update_mask: Buffer.alloc(2),
					__$command_id: 1,
					id: 1,
					name: "Alice",
				},
			];

			const result = parseMsSqlCdcRows(rows, "users", "dbo");

			expect(result).toHaveLength(1);
			const columnNames = result[0]!.columns.map((c) => c.column);
			expect(columnNames).toEqual(["id", "name"]);
			expect(columnNames).not.toContain("__$start_lsn");
			expect(columnNames).not.toContain("__$end_lsn");
			expect(columnNames).not.toContain("__$seqval");
			expect(columnNames).not.toContain("__$operation");
			expect(columnNames).not.toContain("__$update_mask");
			expect(columnNames).not.toContain("__$command_id");
		});
	});

	describe("null column values", () => {
		it("preserves null values as null", () => {
			const rows: MsSqlCdcRow[] = [
				{
					__$operation: 2,
					__$start_lsn: Buffer.alloc(10),
					id: 1,
					name: "Alice",
					email: null,
				},
			];

			const result = parseMsSqlCdcRows(rows, "users", "dbo");
			expect(result[0]!.columns[2]).toEqual({ column: "email", value: null });
		});
	});

	describe("mixed operations in a single batch", () => {
		it("handles insert, update before/after, and delete together", () => {
			const rows: MsSqlCdcRow[] = [
				{
					__$operation: 2,
					__$start_lsn: Buffer.alloc(10),
					id: 1,
					name: "Alice",
				},
				{
					__$operation: 3,
					__$start_lsn: Buffer.alloc(10),
					id: 2,
					name: "Bob",
				},
				{
					__$operation: 4,
					__$start_lsn: Buffer.alloc(10),
					id: 2,
					name: "Bob Updated",
				},
				{
					__$operation: 1,
					__$start_lsn: Buffer.alloc(10),
					id: 3,
					name: "Charlie",
				},
			];

			const result = parseMsSqlCdcRows(rows, "users", "dbo");

			expect(result).toHaveLength(3);
			expect(result[0]!.kind).toBe("insert");
			expect(result[1]!.kind).toBe("update");
			expect(result[2]!.kind).toBe("delete");
		});
	});

	describe("unknown operation codes", () => {
		it("skips rows with unsupported operation codes", () => {
			const rows: MsSqlCdcRow[] = [
				{
					__$operation: 99,
					__$start_lsn: Buffer.alloc(10),
					id: 1,
					name: "Alice",
				},
			];

			const result = parseMsSqlCdcRows(rows, "users", "dbo");
			expect(result).toHaveLength(0);
		});
	});
});

// ---------------------------------------------------------------------------
// Unit tests — mapOperationToKind
// ---------------------------------------------------------------------------

describe("mapOperationToKind", () => {
	it("maps 1 to delete", () => {
		expect(mapOperationToKind(1)).toBe("delete");
	});

	it("maps 2 to insert", () => {
		expect(mapOperationToKind(2)).toBe("insert");
	});

	it("maps 4 to update", () => {
		expect(mapOperationToKind(4)).toBe("update");
	});

	it("returns null for operation 3 (before-image)", () => {
		expect(mapOperationToKind(3)).toBeNull();
	});

	it("returns null for unknown operations", () => {
		expect(mapOperationToKind(0)).toBeNull();
		expect(mapOperationToKind(5)).toBeNull();
		expect(mapOperationToKind(-1)).toBeNull();
	});
});

// ---------------------------------------------------------------------------
// Unit tests — LSN helpers
// ---------------------------------------------------------------------------

describe("LSN utilities", () => {
	describe("hexToBuffer", () => {
		it("converts a hex string with 0x prefix to Buffer", () => {
			const buf = hexToBuffer("0x00000000000000000001");
			expect(buf).toBeInstanceOf(Buffer);
			expect(buf.length).toBe(10);
			expect(buf[9]).toBe(1);
		});

		it("converts a hex string without 0x prefix", () => {
			const buf = hexToBuffer("00000000000000000001");
			expect(buf.length).toBe(10);
			expect(buf[9]).toBe(1);
		});

		it("pads short hex strings to 10 bytes", () => {
			const buf = hexToBuffer("0x01");
			expect(buf.length).toBe(10);
			expect(buf[9]).toBe(1);
			expect(buf[0]).toBe(0);
		});
	});

	describe("bufferToHex", () => {
		it("converts a Buffer to a hex string with 0x prefix", () => {
			const buf = Buffer.alloc(10);
			buf[9] = 0xff;
			const hex = bufferToHex(buf);
			expect(hex).toBe("0x000000000000000000ff");
		});

		it("produces a zero LSN for an empty buffer", () => {
			const buf = Buffer.alloc(10);
			const hex = bufferToHex(buf);
			expect(hex).toBe("0x00000000000000000000");
		});
	});

	describe("roundtrip", () => {
		it("hexToBuffer -> bufferToHex returns the original", () => {
			const original = "0x0000002a000000010003";
			const buf = hexToBuffer(original);
			const result = bufferToHex(buf);
			expect(result).toBe(original);
		});
	});

	describe("compareLsn", () => {
		it("returns 0 for equal LSNs", () => {
			expect(compareLsn("0x00000000000000000001", "0x00000000000000000001")).toBe(0);
		});

		it("returns negative when a < b", () => {
			expect(compareLsn("0x00000000000000000001", "0x00000000000000000002")).toBeLessThan(0);
		});

		it("returns positive when a > b", () => {
			expect(compareLsn("0x00000000000000000002", "0x00000000000000000001")).toBeGreaterThan(0);
		});

		it("handles LSNs with different prefix formats", () => {
			expect(compareLsn("0x00000000000000000001", "00000000000000000001")).toBe(0);
		});

		it("compares the zero LSN correctly", () => {
			expect(
				compareLsn("0x00000000000000000000", "0x00000000000000000001"),
			).toBeLessThan(0);
		});
	});
});

// ---------------------------------------------------------------------------
// Unit tests — deriveCaptureInstanceName
// ---------------------------------------------------------------------------

describe("deriveCaptureInstanceName", () => {
	it("joins schema and table with underscore", () => {
		expect(deriveCaptureInstanceName("dbo", "users")).toBe("dbo_users");
	});

	it("works with non-dbo schemas", () => {
		expect(deriveCaptureInstanceName("sales", "orders")).toBe("sales_orders");
	});
});

// ---------------------------------------------------------------------------
// Unit tests — MsSqlCdcDialect (no connection)
// ---------------------------------------------------------------------------

describe("MsSqlCdcDialect", () => {
	describe("cursor management", () => {
		it("returns the default zero LSN cursor", () => {
			const dialect = new MsSqlCdcDialect({
				connectionString: "Server=localhost;Database=test;",
			});

			expect(dialect.defaultCursor()).toEqual({ lsn: "0x00000000000000000000" });
		});
	});

	describe("name", () => {
		it("uses default schema in name", () => {
			const dialect = new MsSqlCdcDialect({
				connectionString: "Server=localhost;Database=test;",
			});

			expect(dialect.name).toBe("mssql:dbo");
		});

		it("uses custom schema in name", () => {
			const dialect = new MsSqlCdcDialect({
				connectionString: "Server=localhost;Database=test;",
				schema: "sales",
			});

			expect(dialect.name).toBe("mssql:sales");
		});
	});

	describe("ensureCapture without connection", () => {
		it("returns error when not connected", async () => {
			const dialect = new MsSqlCdcDialect({
				connectionString: "Server=localhost;Database=test;",
			});

			const result = await dialect.ensureCapture(["users"]);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.message).toContain("not connected");
			}
		});
	});

	describe("fetchChanges without connection", () => {
		it("returns error when not connected", async () => {
			const dialect = new MsSqlCdcDialect({
				connectionString: "Server=localhost;Database=test;",
			});

			const result = await dialect.fetchChanges(dialect.defaultCursor());
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.message).toContain("not connected");
			}
		});
	});

	describe("discoverSchemas without connection", () => {
		it("returns error when not connected", async () => {
			const dialect = new MsSqlCdcDialect({
				connectionString: "Server=localhost;Database=test;",
			});

			const result = await dialect.discoverSchemas(null);
			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.message).toContain("not connected");
			}
		});
	});

	describe("close (idempotent)", () => {
		it("can be called multiple times without error", async () => {
			const dialect = new MsSqlCdcDialect({
				connectionString: "Server=localhost;Database=test;",
			});

			// close without connect should be a no-op
			await dialect.close();
			await dialect.close();
		});
	});
});

// ---------------------------------------------------------------------------
// Integration tests — require a running SQL Server with CDC enabled
// ---------------------------------------------------------------------------

describe.skipIf(!process.env.MSSQL_URL)("MsSqlCdcDialect integration", () => {
	it("placeholder — requires MSSQL_URL env var", () => {
		expect(true).toBe(true);
	});
});
