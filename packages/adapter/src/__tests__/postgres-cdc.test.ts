import { HLC } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { convertChangesToDeltas } from "../cdc/cdc-source";
import { PostgresCdcSource } from "../cdc/postgres-cdc";
import { parseWal2JsonChanges, type Wal2JsonChange } from "../cdc/postgres-dialect";

// ---------------------------------------------------------------------------
// Unit tests — parseWal2JsonChanges (returns CdcRawChange[])
// ---------------------------------------------------------------------------

describe("parseWal2JsonChanges", () => {
	describe("INSERT", () => {
		it("converts a simple insert to a CdcRawChange", () => {
			const changes: Wal2JsonChange[] = [
				{
					kind: "insert",
					schema: "public",
					table: "users",
					columnnames: ["id", "name", "email"],
					columntypes: ["integer", "text", "text"],
					columnvalues: [1, "Alice", "alice@example.com"],
				},
			];

			const result = parseWal2JsonChanges(changes);

			expect(result).toHaveLength(1);
			const r = result[0]!;
			expect(r.kind).toBe("insert");
			expect(r.schema).toBe("public");
			expect(r.table).toBe("users");
			expect(r.rowId).toBe("1"); // first column as fallback rowId
			expect(r.columns).toEqual([
				{ column: "id", value: 1 },
				{ column: "name", value: "Alice" },
				{ column: "email", value: "alice@example.com" },
			]);
		});

		it("uses oldkeys for rowId when present on insert", () => {
			const changes: Wal2JsonChange[] = [
				{
					kind: "insert",
					schema: "public",
					table: "users",
					columnnames: ["id", "name"],
					columntypes: ["integer", "text"],
					columnvalues: [42, "Bob"],
					oldkeys: { keynames: ["id"], keyvalues: [42] },
				},
			];

			const result = parseWal2JsonChanges(changes);
			expect(result[0]!.rowId).toBe("42");
		});
	});

	describe("UPDATE", () => {
		it("converts an update to a CdcRawChange with all columns", () => {
			const changes: Wal2JsonChange[] = [
				{
					kind: "update",
					schema: "public",
					table: "users",
					columnnames: ["id", "name", "email"],
					columntypes: ["integer", "text", "text"],
					columnvalues: [1, "Alice Smith", "alice@example.com"],
					oldkeys: { keynames: ["id"], keyvalues: [1] },
				},
			];

			const result = parseWal2JsonChanges(changes);

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

	describe("DELETE", () => {
		it("converts a delete to a CdcRawChange with empty columns", () => {
			const changes: Wal2JsonChange[] = [
				{
					kind: "delete",
					schema: "public",
					table: "users",
					oldkeys: { keynames: ["id"], keyvalues: [1] },
				},
			];

			const result = parseWal2JsonChanges(changes);

			expect(result).toHaveLength(1);
			const r = result[0]!;
			expect(r.kind).toBe("delete");
			expect(r.table).toBe("users");
			expect(r.rowId).toBe("1");
			expect(r.columns).toEqual([]);
		});

		it("skips delete with missing oldkeys", () => {
			const changes: Wal2JsonChange[] = [
				{
					kind: "delete",
					schema: "public",
					table: "users",
				},
			];

			const result = parseWal2JsonChanges(changes);
			expect(result).toHaveLength(0);
		});
	});

	describe("composite primary keys", () => {
		it("joins multiple PK values with colon", () => {
			const changes: Wal2JsonChange[] = [
				{
					kind: "update",
					schema: "public",
					table: "order_items",
					columnnames: ["order_id", "product_id", "quantity"],
					columntypes: ["integer", "integer", "integer"],
					columnvalues: [100, 200, 5],
					oldkeys: {
						keynames: ["order_id", "product_id"],
						keyvalues: [100, 200],
					},
				},
			];

			const result = parseWal2JsonChanges(changes);
			expect(result[0]!.rowId).toBe("100:200");
		});

		it("handles composite PK on delete", () => {
			const changes: Wal2JsonChange[] = [
				{
					kind: "delete",
					schema: "public",
					table: "order_items",
					oldkeys: {
						keynames: ["order_id", "product_id"],
						keyvalues: [100, 200],
					},
				},
			];

			const result = parseWal2JsonChanges(changes);
			expect(result[0]!.rowId).toBe("100:200");
		});
	});

	describe("mixed operations in a single batch", () => {
		it("handles insert, update, and delete together", () => {
			const changes: Wal2JsonChange[] = [
				{
					kind: "insert",
					schema: "public",
					table: "users",
					columnnames: ["id", "name"],
					columntypes: ["integer", "text"],
					columnvalues: [1, "Alice"],
				},
				{
					kind: "update",
					schema: "public",
					table: "users",
					columnnames: ["id", "name"],
					columntypes: ["integer", "text"],
					columnvalues: [2, "Bob Updated"],
					oldkeys: { keynames: ["id"], keyvalues: [2] },
				},
				{
					kind: "delete",
					schema: "public",
					table: "users",
					oldkeys: { keynames: ["id"], keyvalues: [3] },
				},
			];

			const result = parseWal2JsonChanges(changes);

			expect(result).toHaveLength(3);
			expect(result[0]!.kind).toBe("insert");
			expect(result[1]!.kind).toBe("update");
			expect(result[2]!.kind).toBe("delete");
		});
	});

	describe("null column values", () => {
		it("preserves null values as null (not undefined)", () => {
			const changes: Wal2JsonChange[] = [
				{
					kind: "insert",
					schema: "public",
					table: "users",
					columnnames: ["id", "name", "email"],
					columntypes: ["integer", "text", "text"],
					columnvalues: [1, "Alice", null],
				},
			];

			const result = parseWal2JsonChanges(changes);
			expect(result[0]!.columns[2]).toEqual({ column: "email", value: null });
		});
	});
});

// ---------------------------------------------------------------------------
// Unit tests — convertChangesToDeltas (CdcRawChange[] -> RowDelta[])
// ---------------------------------------------------------------------------

describe("convertChangesToDeltas", () => {
	const hlc = HLC.encode(1_700_000_000_000, 0);
	const clientId = "cdc:postgres:lakesync_cdc";

	it("converts CdcRawChanges to RowDeltas with correct ops", async () => {
		const changes = parseWal2JsonChanges([
			{
				kind: "insert",
				schema: "public",
				table: "users",
				columnnames: ["id", "name"],
				columntypes: ["integer", "text"],
				columnvalues: [1, "Alice"],
			},
			{
				kind: "update",
				schema: "public",
				table: "users",
				columnnames: ["id", "name"],
				columntypes: ["integer", "text"],
				columnvalues: [2, "Bob"],
				oldkeys: { keynames: ["id"], keyvalues: [2] },
			},
			{
				kind: "delete",
				schema: "public",
				table: "users",
				oldkeys: { keynames: ["id"], keyvalues: [3] },
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
		const changes = parseWal2JsonChanges([
			{
				kind: "insert",
				schema: "public",
				table: "users",
				columnnames: ["id"],
				columntypes: ["integer"],
				columnvalues: [1],
			},
			{
				kind: "insert",
				schema: "public",
				table: "orders",
				columnnames: ["id"],
				columntypes: ["integer"],
				columnvalues: [1],
			},
			{
				kind: "insert",
				schema: "public",
				table: "users",
				columnnames: ["id"],
				columntypes: ["integer"],
				columnvalues: [2],
			},
		]);

		const tables = new Set(["users"]);
		const deltas = await convertChangesToDeltas(changes, hlc, clientId, tables);

		expect(deltas).toHaveLength(2);
		expect(deltas.every((d) => d.table === "users")).toBe(true);
	});

	it("returns all changes when tables is null", async () => {
		const changes = parseWal2JsonChanges([
			{
				kind: "insert",
				schema: "public",
				table: "users",
				columnnames: ["id"],
				columntypes: ["integer"],
				columnvalues: [1],
			},
			{
				kind: "insert",
				schema: "public",
				table: "orders",
				columnnames: ["id"],
				columntypes: ["integer"],
				columnvalues: [1],
			},
		]);

		const deltas = await convertChangesToDeltas(changes, hlc, clientId, null);
		expect(deltas).toHaveLength(2);
	});

	describe("deterministic deltaId", () => {
		it("produces the same deltaId for identical inputs", async () => {
			const changes = parseWal2JsonChanges([
				{
					kind: "insert",
					schema: "public",
					table: "users",
					columnnames: ["id", "name"],
					columntypes: ["integer", "text"],
					columnvalues: [1, "Alice"],
				},
			]);

			const deltas1 = await convertChangesToDeltas(changes, hlc, clientId, null);
			const deltas2 = await convertChangesToDeltas(changes, hlc, clientId, null);

			expect(deltas1[0]!.deltaId).toBe(deltas2[0]!.deltaId);
		});

		it("produces different deltaIds for different data", async () => {
			const changes1 = parseWal2JsonChanges([
				{
					kind: "insert",
					schema: "public",
					table: "users",
					columnnames: ["id", "name"],
					columntypes: ["integer", "text"],
					columnvalues: [1, "Alice"],
				},
			]);

			const changes2 = parseWal2JsonChanges([
				{
					kind: "insert",
					schema: "public",
					table: "users",
					columnnames: ["id", "name"],
					columntypes: ["integer", "text"],
					columnvalues: [1, "Bob"],
				},
			]);

			const deltas1 = await convertChangesToDeltas(changes1, hlc, clientId, null);
			const deltas2 = await convertChangesToDeltas(changes2, hlc, clientId, null);

			expect(deltas1[0]!.deltaId).not.toBe(deltas2[0]!.deltaId);
		});
	});
});

// ---------------------------------------------------------------------------
// PostgresCdcSource unit tests (backward compatibility)
// ---------------------------------------------------------------------------

describe("PostgresCdcSource", () => {
	describe("cursor management", () => {
		it("starts with default cursor lsn 0/0", () => {
			const source = new PostgresCdcSource({
				connectionString: "postgres://localhost/test",
			});

			expect(source.getCursor()).toEqual({ lsn: "0/0" });
		});

		it("setCursor updates the cursor", () => {
			const source = new PostgresCdcSource({
				connectionString: "postgres://localhost/test",
			});

			source.setCursor({ lsn: "0/16B3748" });
			expect(source.getCursor()).toEqual({ lsn: "0/16B3748" });
		});
	});

	describe("name and clientId", () => {
		it("uses default slot name in name and clientId", () => {
			const source = new PostgresCdcSource({
				connectionString: "postgres://localhost/test",
			});

			expect(source.name).toBe("postgres-cdc:lakesync_cdc");
			expect(source.clientId).toBe("cdc:lakesync_cdc");
		});

		it("uses custom slot name in name and clientId", () => {
			const source = new PostgresCdcSource({
				connectionString: "postgres://localhost/test",
				slotName: "my_slot",
			});

			expect(source.name).toBe("postgres-cdc:my_slot");
			expect(source.clientId).toBe("cdc:my_slot");
		});
	});

	describe("stop (idempotent)", () => {
		it("can be called multiple times without error", async () => {
			const source = new PostgresCdcSource({
				connectionString: "postgres://localhost/test",
			});

			// stop without start should be a no-op
			await source.stop();
			await source.stop();
		});
	});
});

// ---------------------------------------------------------------------------
// Integration tests — require a running Postgres with wal2json
// ---------------------------------------------------------------------------

describe.skipIf(!process.env.POSTGRES_URL)("PostgresCdcSource integration", () => {
	it("placeholder — requires POSTGRES_URL env var", () => {
		expect(true).toBe(true);
	});
});
