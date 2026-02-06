import { describe, expect, it } from "vitest";
import type { TableSchema } from "@lakesync/core";
import {
	buildPartitionSpec,
	lakeSyncTableName,
	tableSchemaToIceberg,
} from "../schema-mapping";

// ---------------------------------------------------------------------------
// Shared fixture — a schema with mixed column types
// ---------------------------------------------------------------------------

const MIXED_SCHEMA: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "priority", type: "number" },
		{ name: "completed", type: "boolean" },
		{ name: "metadata", type: "json" },
	],
};

// ---------------------------------------------------------------------------
// System column names in expected order
// ---------------------------------------------------------------------------

const SYSTEM_COLUMN_NAMES = [
	"op",
	"table",
	"rowId",
	"clientId",
	"hlc",
	"deltaId",
];

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("tableSchemaToIceberg", () => {
	it("maps mixed user types to correct Iceberg field types", () => {
		const iceberg = tableSchemaToIceberg(MIXED_SCHEMA);

		// Extract user fields (after the 6 system columns)
		const userFields = iceberg.fields.slice(6);

		expect(userFields).toHaveLength(4);
		expect(userFields[0]).toMatchObject({ name: "title", type: "string" });
		expect(userFields[1]).toMatchObject({ name: "priority", type: "double" });
		expect(userFields[2]).toMatchObject({
			name: "completed",
			type: "boolean",
		});
		expect(userFields[3]).toMatchObject({ name: "metadata", type: "string" });
	});

	it("includes system columns with correct types", () => {
		const iceberg = tableSchemaToIceberg(MIXED_SCHEMA);
		const systemFields = iceberg.fields.slice(0, 6);

		expect(systemFields[0]).toMatchObject({
			name: "op",
			type: "string",
			required: true,
		});
		expect(systemFields[1]).toMatchObject({
			name: "table",
			type: "string",
			required: true,
		});
		expect(systemFields[2]).toMatchObject({
			name: "rowId",
			type: "string",
			required: true,
		});
		expect(systemFields[3]).toMatchObject({
			name: "clientId",
			type: "string",
			required: true,
		});
		expect(systemFields[4]).toMatchObject({
			name: "hlc",
			type: "long",
			required: true,
		});
		expect(systemFields[5]).toMatchObject({
			name: "deltaId",
			type: "string",
			required: true,
		});
	});

	it("assigns sequential field IDs starting from 1", () => {
		const iceberg = tableSchemaToIceberg(MIXED_SCHEMA);

		const ids = iceberg.fields.map((f) => f.id);
		const expected = iceberg.fields.map((_, i) => i + 1);

		expect(ids).toEqual(expected);
	});

	it("places system columns before user columns", () => {
		const iceberg = tableSchemaToIceberg(MIXED_SCHEMA);

		const firstSix = iceberg.fields.slice(0, 6).map((f) => f.name);
		expect(firstSix).toEqual(SYSTEM_COLUMN_NAMES);

		// Remaining fields are user columns
		const userNames = iceberg.fields.slice(6).map((f) => f.name);
		expect(userNames).toEqual(["title", "priority", "completed", "metadata"]);
	});

	it("marks user columns as not required", () => {
		const iceberg = tableSchemaToIceberg(MIXED_SCHEMA);

		const userFields = iceberg.fields.slice(6);
		for (const field of userFields) {
			expect(field.required).toBe(false);
		}
	});

	it("sets schema-id to 0 and type to struct", () => {
		const iceberg = tableSchemaToIceberg(MIXED_SCHEMA);

		expect(iceberg["schema-id"]).toBe(0);
		expect(iceberg.type).toBe("struct");
	});

	it("handles a schema with no user columns", () => {
		const emptySchema: TableSchema = { table: "empty", columns: [] };
		const iceberg = tableSchemaToIceberg(emptySchema);

		expect(iceberg.fields).toHaveLength(6);
		expect(iceberg.fields.map((f) => f.name)).toEqual(SYSTEM_COLUMN_NAMES);
	});

	it("maps null column type to string", () => {
		const nullSchema: TableSchema = {
			table: "test",
			columns: [{ name: "removed", type: "null" }],
		};
		const iceberg = tableSchemaToIceberg(nullSchema);
		const removedField = iceberg.fields.find((f) => f.name === "removed");

		expect(removedField).toBeDefined();
		expect(removedField?.type).toBe("string");
		expect(removedField?.required).toBe(false);
	});
});

describe("buildPartitionSpec", () => {
	it("targets hlc with day transform", () => {
		const iceberg = tableSchemaToIceberg(MIXED_SCHEMA);
		const spec = buildPartitionSpec(iceberg);

		expect(spec["spec-id"]).toBe(0);
		expect(spec.fields).toHaveLength(1);

		const partitionField = spec.fields[0];
		expect(partitionField).toBeDefined();
		if (!partitionField) return;

		const hlcField = iceberg.fields.find((f) => f.name === "hlc");

		expect(partitionField["source-id"]).toBe(hlcField?.id);
		expect(partitionField["field-id"]).toBe(1000);
		expect(partitionField.name).toBe("hlc_day");
		expect(partitionField.transform).toBe("day");
	});

	it("throws when schema has no hlc field", () => {
		const noHlcSchema = {
			type: "struct" as const,
			"schema-id": 0,
			fields: [{ id: 1, name: "id", required: true, type: "long" }],
		};

		expect(() => buildPartitionSpec(noHlcSchema)).toThrow(
			"Schema must contain an 'hlc' field",
		);
	});
});

describe("lakeSyncTableName", () => {
	it("maps table name to lakesync namespace", () => {
		const result = lakeSyncTableName("todos");

		expect(result.namespace).toEqual(["lakesync"]);
		expect(result.name).toBe("todos");
	});

	it("preserves the original table name as-is", () => {
		const result = lakeSyncTableName("UserEvents");

		expect(result.namespace).toEqual(["lakesync"]);
		expect(result.name).toBe("UserEvents");
	});

	it("handles empty table name", () => {
		const result = lakeSyncTableName("");

		expect(result.namespace).toEqual(["lakesync"]);
		expect(result.name).toBe("");
	});
});
