import * as arrow from "apache-arrow";
import { describe, expect, it } from "vitest";
import type { RowDelta, TableSchema } from "../../delta/types";
import { HLC } from "../../hlc/hlc";
import { buildArrowSchema, deltasToArrowTable } from "../schema";

const testSchema: TableSchema = {
	table: "users",
	columns: [
		{ name: "name", type: "string" },
		{ name: "age", type: "number" },
		{ name: "active", type: "boolean" },
		{ name: "meta", type: "json" },
		{ name: "deleted_reason", type: "null" },
	],
};

describe("buildArrowSchema", () => {
	it("should include all system columns with correct types", () => {
		const schema = buildArrowSchema(testSchema);
		const fields = schema.fields;

		// System columns
		const opField = fields.find((f) => f.name === "op");
		expect(opField).toBeDefined();
		expect(arrow.DataType.isUtf8(opField!.type)).toBe(true);

		const tableField = fields.find((f) => f.name === "table");
		expect(tableField).toBeDefined();
		expect(arrow.DataType.isUtf8(tableField!.type)).toBe(true);

		const rowIdField = fields.find((f) => f.name === "rowId");
		expect(rowIdField).toBeDefined();
		expect(arrow.DataType.isUtf8(rowIdField!.type)).toBe(true);

		const clientIdField = fields.find((f) => f.name === "clientId");
		expect(clientIdField).toBeDefined();
		expect(arrow.DataType.isUtf8(clientIdField!.type)).toBe(true);

		const hlcField = fields.find((f) => f.name === "hlc");
		expect(hlcField).toBeDefined();
		expect(arrow.DataType.isInt(hlcField!.type)).toBe(true);

		const deltaIdField = fields.find((f) => f.name === "deltaId");
		expect(deltaIdField).toBeDefined();
		expect(arrow.DataType.isUtf8(deltaIdField!.type)).toBe(true);
	});

	it("should include user columns with correct Arrow types", () => {
		const schema = buildArrowSchema(testSchema);
		const fields = schema.fields;

		const nameField = fields.find((f) => f.name === "name");
		expect(nameField).toBeDefined();
		expect(arrow.DataType.isUtf8(nameField!.type)).toBe(true);

		const ageField = fields.find((f) => f.name === "age");
		expect(ageField).toBeDefined();
		expect(arrow.DataType.isFloat(ageField!.type)).toBe(true);

		const activeField = fields.find((f) => f.name === "active");
		expect(activeField).toBeDefined();
		expect(arrow.DataType.isBool(activeField!.type)).toBe(true);

		const metaField = fields.find((f) => f.name === "meta");
		expect(metaField).toBeDefined();
		expect(arrow.DataType.isUtf8(metaField!.type)).toBe(true);

		const deletedReasonField = fields.find((f) => f.name === "deleted_reason");
		expect(deletedReasonField).toBeDefined();
		expect(arrow.DataType.isUtf8(deletedReasonField!.type)).toBe(true);
		expect(deletedReasonField!.nullable).toBe(true);
	});

	it("should have system columns before user columns", () => {
		const schema = buildArrowSchema(testSchema);
		const names = schema.fields.map((f) => f.name);

		expect(names.indexOf("op")).toBeLessThan(names.indexOf("name"));
		expect(names.indexOf("deltaId")).toBeLessThan(names.indexOf("name"));
	});
});

describe("deltasToArrowTable", () => {
	const makeDelta = (
		op: RowDelta["op"],
		rowId: string,
		wall: number,
		counter: number,
		columns: RowDelta["columns"],
	): RowDelta => ({
		op,
		table: "users",
		rowId,
		clientId: "client-1",
		columns,
		hlc: HLC.encode(wall, counter),
		deltaId: `delta-${rowId}-${wall}-${counter}`,
	});

	it("should convert 5 mixed deltas into an Arrow Table with correct row count", () => {
		const deltas: RowDelta[] = [
			makeDelta("INSERT", "row-1", 1_700_000_001_000, 0, [
				{ column: "name", value: "Alice" },
				{ column: "age", value: 30 },
				{ column: "active", value: true },
				{ column: "meta", value: { role: "admin" } },
			]),
			makeDelta("INSERT", "row-2", 1_700_000_002_000, 0, [
				{ column: "name", value: "Bob" },
				{ column: "age", value: 25 },
				{ column: "active", value: false },
			]),
			makeDelta("UPDATE", "row-1", 1_700_000_003_000, 1, [{ column: "age", value: 31 }]),
			makeDelta("UPDATE", "row-2", 1_700_000_004_000, 2, [
				{ column: "name", value: "Robert" },
				{ column: "active", value: true },
			]),
			makeDelta("DELETE", "row-1", 1_700_000_005_000, 0, []),
		];

		const table = deltasToArrowTable(deltas, testSchema);

		expect(table.numRows).toBe(5);
		expect(table.numCols).toBeGreaterThanOrEqual(11); // 6 system + 5 user
	});

	it("should produce an empty table with correct schema for empty deltas", () => {
		const table = deltasToArrowTable([], testSchema);

		expect(table.numRows).toBe(0);
		// The schema should still have the correct fields
		expect(table.schema.fields.length).toBe(11); // 6 system + 5 user
	});

	it("should preserve HLC bigint through Int64 roundtrip", () => {
		// Encode a realistic HLC: wall=1_700_000_000_000, counter=42
		const hlcTimestamp = HLC.encode(1_700_000_000_000, 42);
		const deltas: RowDelta[] = [
			{
				op: "INSERT",
				table: "users",
				rowId: "row-1",
				clientId: "client-1",
				columns: [{ column: "name", value: "Test" }],
				hlc: hlcTimestamp,
				deltaId: "delta-hlc-roundtrip",
			},
		];

		const table = deltasToArrowTable(deltas, testSchema);
		const hlcColumn = table.getChild("hlc");

		expect(hlcColumn).toBeDefined();
		// Verify the bigint survives the Arrow Int64 roundtrip
		const recovered = hlcColumn!.get(0) as bigint;
		expect(recovered).toBe(hlcTimestamp as bigint);

		// Also verify we can decode it back to the original wall + counter
		const decoded = HLC.decode(recovered as ReturnType<typeof HLC.encode>);
		expect(decoded.wall).toBe(1_700_000_000_000);
		expect(decoded.counter).toBe(42);
	});

	it("should JSON.stringify json-typed column values", () => {
		const metaValue = { role: "admin", level: 5 };
		const deltas: RowDelta[] = [
			makeDelta("INSERT", "row-1", 1_700_000_001_000, 0, [
				{ column: "name", value: "Alice" },
				{ column: "meta", value: metaValue },
			]),
		];

		const table = deltasToArrowTable(deltas, testSchema);
		const metaColumn = table.getChild("meta");

		expect(metaColumn).toBeDefined();
		expect(metaColumn!.get(0)).toBe(JSON.stringify(metaValue));
	});

	it("should fill missing columns with null", () => {
		const deltas: RowDelta[] = [
			makeDelta("UPDATE", "row-1", 1_700_000_001_000, 0, [
				{ column: "name", value: "Alice" },
				// age, active, meta, deleted_reason are all missing
			]),
		];

		const table = deltasToArrowTable(deltas, testSchema);

		const ageColumn = table.getChild("age");
		expect(ageColumn).toBeDefined();
		expect(ageColumn!.get(0)).toBeNull();

		const activeColumn = table.getChild("active");
		expect(activeColumn).toBeDefined();
		expect(activeColumn!.get(0)).toBeNull();

		const metaColumn = table.getChild("meta");
		expect(metaColumn).toBeDefined();
		expect(metaColumn!.get(0)).toBeNull();
	});

	it("should correctly map system column values", () => {
		const deltas: RowDelta[] = [
			makeDelta("INSERT", "row-42", 1_700_000_009_999, 7, [{ column: "name", value: "Zara" }]),
		];

		const table = deltasToArrowTable(deltas, testSchema);

		expect(table.getChild("op")!.get(0)).toBe("INSERT");
		expect(table.getChild("table")!.get(0)).toBe("users");
		expect(table.getChild("rowId")!.get(0)).toBe("row-42");
		expect(table.getChild("clientId")!.get(0)).toBe("client-1");
		expect(table.getChild("deltaId")!.get(0)).toBe("delta-row-42-1700000009999-7");
	});
});
