import type { RowDelta, TableSchema } from "@lakesync/core";
import * as arrow from "apache-arrow";

/**
 * Column type from a TableSchema definition.
 */
type ColumnType = TableSchema["columns"][number]["type"];

/**
 * Maps a LakeSync column type to an Apache Arrow data type.
 *
 * @param colType - The LakeSync column type to convert
 * @returns The corresponding Apache Arrow data type
 */
const ARROW_TYPE_MAP: Record<ColumnType, () => arrow.DataType> = {
	string: () => new arrow.Utf8(),
	number: () => new arrow.Float64(),
	boolean: () => new arrow.Bool(),
	json: () => new arrow.Utf8(),
	null: () => new arrow.Utf8(),
};

function lakeSyncTypeToArrow(colType: ColumnType): arrow.DataType {
	return ARROW_TYPE_MAP[colType]();
}

/**
 * Builds an Apache Arrow Schema from a LakeSync TableSchema.
 *
 * The resulting schema always includes the following system columns:
 * - `op` (Utf8) — the delta operation type (INSERT, UPDATE, DELETE)
 * - `table` (Utf8) — the table name
 * - `rowId` (Utf8) — the row identifier
 * - `clientId` (Utf8) — the client identifier
 * - `hlc` (Int64) — the HLC timestamp as a 64-bit integer
 * - `deltaId` (Utf8) — the deterministic delta identifier
 *
 * User-defined columns from the TableSchema are appended after system columns,
 * with types mapped as follows:
 * - `string` → Utf8
 * - `number` → Float64
 * - `boolean` → Bool
 * - `json` → Utf8 (values are JSON-serialised)
 * - `null` → Utf8, nullable
 *
 * @param schema - The LakeSync TableSchema to convert
 * @returns An Apache Arrow Schema with system and user columns
 */
export function buildArrowSchema(schema: TableSchema): arrow.Schema {
	const systemFields: arrow.Field[] = [
		new arrow.Field("op", new arrow.Utf8(), false),
		new arrow.Field("table", new arrow.Utf8(), false),
		new arrow.Field("rowId", new arrow.Utf8(), false),
		new arrow.Field("clientId", new arrow.Utf8(), false),
		new arrow.Field("hlc", new arrow.Int64(), false),
		new arrow.Field("deltaId", new arrow.Utf8(), false),
	];

	const userFields: arrow.Field[] = schema.columns.map((col) => {
		const nullable =
			col.type === "null" ||
			col.type === "json" ||
			col.type === "boolean" ||
			col.type === "number" ||
			col.type === "string";
		return new arrow.Field(col.name, lakeSyncTypeToArrow(col.type), nullable);
	});

	return new arrow.Schema([...systemFields, ...userFields]);
}

/**
 * Converts an array of RowDelta objects into an Apache Arrow Table.
 *
 * System columns (op, table, rowId, clientId, hlc, deltaId) are extracted
 * directly from each delta. User columns are looked up from each delta's
 * `columns` array; missing columns produce `null` for that row.
 *
 * Type conversions:
 * - `json` columns are serialised via `JSON.stringify`
 * - `hlc` is passed as a bigint directly to Int64 vectors
 * - All other types are passed through as-is
 *
 * @param deltas - Array of RowDelta objects to convert
 * @param schema - The LakeSync TableSchema describing user columns
 * @returns An Apache Arrow Table containing all deltas
 */
export function deltasToArrowTable(deltas: RowDelta[], schema: TableSchema): arrow.Table {
	const arrowSchema = buildArrowSchema(schema);

	if (deltas.length === 0) {
		return new arrow.Table(arrowSchema);
	}

	// Build column index: map column name → column type from schema
	const columnTypeMap = new Map<string, ColumnType>();
	for (const col of schema.columns) {
		columnTypeMap.set(col.name, col.type);
	}

	// System column arrays
	const ops: string[] = [];
	const tables: string[] = [];
	const rowIds: string[] = [];
	const clientIds: string[] = [];
	const hlcs: bigint[] = [];
	const deltaIds: string[] = [];

	// User column arrays — initialise as arrays of null
	const userColumns = new Map<string, (unknown | null)[]>();
	for (const col of schema.columns) {
		userColumns.set(col.name, []);
	}

	// Populate arrays from deltas
	for (const delta of deltas) {
		ops.push(delta.op);
		tables.push(delta.table);
		rowIds.push(delta.rowId);
		clientIds.push(delta.clientId);
		hlcs.push(delta.hlc as bigint);
		deltaIds.push(delta.deltaId);

		// Build a lookup for this delta's columns
		const deltaColMap = new Map<string, unknown>();
		for (const colDelta of delta.columns) {
			deltaColMap.set(colDelta.column, colDelta.value);
		}

		// Fill user columns — missing columns get null
		for (const col of schema.columns) {
			const arr = userColumns.get(col.name);
			if (!arr) continue;

			if (deltaColMap.has(col.name)) {
				const value = deltaColMap.get(col.name);
				if (col.type === "json") {
					arr.push(value != null ? JSON.stringify(value) : null);
				} else {
					arr.push(value ?? null);
				}
			} else {
				arr.push(null);
			}
		}
	}

	// Build the data map for arrow.tableFromArrays-style construction
	// We need to build vectors for each column and construct the table
	const columnData: Record<string, arrow.Vector> = {};

	// System vectors
	columnData.op = arrow.vectorFromArray(ops, new arrow.Utf8());
	columnData.table = arrow.vectorFromArray(tables, new arrow.Utf8());
	columnData.rowId = arrow.vectorFromArray(rowIds, new arrow.Utf8());
	columnData.clientId = arrow.vectorFromArray(clientIds, new arrow.Utf8());
	columnData.hlc = arrow.vectorFromArray(hlcs, new arrow.Int64());
	columnData.deltaId = arrow.vectorFromArray(deltaIds, new arrow.Utf8());

	// User vectors
	for (const col of schema.columns) {
		const values = userColumns.get(col.name);
		if (!values) continue;

		const arrowType = lakeSyncTypeToArrow(col.type);
		columnData[col.name] = arrow.vectorFromArray(values, arrowType);
	}

	// Build the table from the schema and vectors
	return new arrow.Table(columnData);
}
