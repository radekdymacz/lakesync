import type { TableSchema } from "@lakesync/core";
import type { IcebergField, IcebergSchema, PartitionSpec } from "./types";

/**
 * LakeSync column type as defined in the TableSchema interface.
 */
type ColumnType = TableSchema["columns"][number]["type"];

/**
 * Maps a LakeSync column type to an Iceberg type string.
 *
 * The mapping mirrors the Arrow schema produced by `buildArrowSchema` in
 * `@lakesync/parquet`:
 * - `"string"` → `"string"` (Utf8 equivalent)
 * - `"number"` → `"double"` (Float64 equivalent)
 * - `"boolean"` → `"boolean"` (Bool equivalent)
 * - `"json"` → `"string"` (serialised as JSON text, same as Utf8)
 * - `"null"` → `"string"` (nullable Utf8)
 *
 * @param colType - The LakeSync column type to convert
 * @returns The corresponding Iceberg type string
 */
function lakeSyncTypeToIceberg(colType: ColumnType): string {
	switch (colType) {
		case "string":
			return "string";
		case "number":
			return "double";
		case "boolean":
			return "boolean";
		case "json":
			return "string";
		case "null":
			return "string";
	}
}

/**
 * Converts a LakeSync `TableSchema` to an Iceberg `IcebergSchema`.
 *
 * The resulting schema always includes six system columns (required) followed
 * by user-defined columns (not required). Column order and types are aligned
 * with the Apache Arrow schema produced by `buildArrowSchema` from
 * `@lakesync/parquet`.
 *
 * System columns (always present, in this order):
 * 1. `op` — `"string"` (the delta operation type)
 * 2. `table` — `"string"` (the table name)
 * 3. `rowId` — `"string"` (the row identifier)
 * 4. `clientId` — `"string"` (the client identifier)
 * 5. `hlc` — `"long"` (HLC timestamp as Int64)
 * 6. `deltaId` — `"string"` (the deterministic delta identifier)
 *
 * User columns are mapped according to their declared LakeSync type:
 * - `"string"` → `"string"`
 * - `"number"` → `"double"`
 * - `"boolean"` → `"boolean"`
 * - `"json"` → `"string"` (JSON-serialised text)
 * - `"null"` → `"string"`
 *
 * @param schema - The LakeSync `TableSchema` to convert
 * @returns An `IcebergSchema` with system and user columns, `schema-id` 0
 */
export function tableSchemaToIceberg(schema: TableSchema): IcebergSchema {
	let fieldId = 1;

	const systemFields: IcebergField[] = [
		{ id: fieldId++, name: "op", required: true, type: "string" },
		{ id: fieldId++, name: "table", required: true, type: "string" },
		{ id: fieldId++, name: "rowId", required: true, type: "string" },
		{ id: fieldId++, name: "clientId", required: true, type: "string" },
		{ id: fieldId++, name: "hlc", required: true, type: "long" },
		{ id: fieldId++, name: "deltaId", required: true, type: "string" },
	];

	const userFields: IcebergField[] = schema.columns.map((col) => ({
		id: fieldId++,
		name: col.name,
		required: false,
		type: lakeSyncTypeToIceberg(col.type),
	}));

	return {
		type: "struct",
		"schema-id": 0,
		fields: [...systemFields, ...userFields],
	};
}

/**
 * Builds an Iceberg `PartitionSpec` from an `IcebergSchema`.
 *
 * The partition strategy extracts the day from the `hlc` column using the
 * Iceberg `day` transform, which partitions data by the wall-clock date
 * encoded in the HLC timestamp. This ensures efficient time-range queries.
 *
 * The resulting spec has a single partition field:
 * - `source-id`: the field ID of the `hlc` column
 * - `field-id`: 1000 (Iceberg convention — partition field IDs start at 1000)
 * - `name`: `"hlc_day"`
 * - `transform`: `"day"`
 *
 * @param schema - The Iceberg schema containing an `hlc` field
 * @returns A `PartitionSpec` with `spec-id` 0 and a single day-partitioned field
 * @throws If the schema does not contain an `hlc` field
 */
export function buildPartitionSpec(schema: IcebergSchema): PartitionSpec {
	const hlcField = schema.fields.find((f) => f.name === "hlc");
	if (!hlcField) {
		throw new Error("Schema must contain an 'hlc' field for partitioning");
	}

	return {
		"spec-id": 0,
		fields: [
			{
				"source-id": hlcField.id,
				"field-id": 1000,
				name: "hlc_day",
				transform: "day",
			},
		],
	};
}

/**
 * Maps a LakeSync table name to an Iceberg namespace and table name.
 *
 * All LakeSync tables reside under the `["lakesync"]` namespace. The table
 * name is passed through as-is, preserving the original casing and format.
 *
 * @param table - The LakeSync table name (e.g. `"todos"`)
 * @returns An object with `namespace` (`["lakesync"]`) and `name` (the table name)
 */
export function lakeSyncTableName(table: string): {
	namespace: string[];
	name: string;
} {
	return {
		namespace: ["lakesync"],
		name: table,
	};
}
