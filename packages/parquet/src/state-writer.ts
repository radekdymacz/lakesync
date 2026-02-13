import { Err, FlushError, Ok, type Result, type TableSchema } from "@lakesync/core";
import * as arrow from "apache-arrow";
import {
	Compression,
	Table as WasmTable,
	WriterPropertiesBuilder,
	writeParquet,
} from "parquet-wasm/esm";
import { ensureWasmInitialised } from "./wasm";

/**
 * Column type from a TableSchema definition.
 */
type ColumnType = TableSchema["columns"][number]["type"];

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
 * A single current-state row produced by merging deltas.
 */
export interface StateRow {
	/** Row identifier. */
	rowId: string;
	/** Merged column values (column name to value). */
	state: Record<string, unknown>;
}

/**
 * Serialises current-state rows into Parquet bytes.
 *
 * Unlike `writeDeltasToParquet` which writes raw delta rows with system
 * columns, this function writes the **merged current state** of each row:
 * - `row_id` (Utf8) — the row identifier
 * - One column per schema column, typed according to the schema
 *
 * Boolean columns are stored as Int8 (1/0/null) to work around the same
 * Arrow JS IPC serialisation limitation documented in the delta writer.
 *
 * @param rows - Current-state rows (rowId + merged state).
 * @param schema - Table schema describing user-defined columns.
 * @returns Parquet file bytes or a FlushError on failure.
 */
export async function writeStateToParquet(
	rows: StateRow[],
	schema: TableSchema,
): Promise<Result<Uint8Array, FlushError>> {
	try {
		ensureWasmInitialised();

		if (rows.length === 0) {
			return Err(new FlushError("No rows to write"));
		}

		// Build column arrays
		const rowIds: string[] = [];
		const userColumns = new Map<string, (unknown | null)[]>();
		for (const col of schema.columns) {
			userColumns.set(col.name, []);
		}

		for (const row of rows) {
			rowIds.push(row.rowId);
			for (const col of schema.columns) {
				const arr = userColumns.get(col.name)!;
				const value = row.state[col.name];
				if (col.type === "json") {
					arr.push(value != null ? JSON.stringify(value) : null);
				} else {
					arr.push(value ?? null);
				}
			}
		}

		// Build Arrow vectors
		const columnData: Record<string, arrow.Vector> = {};
		columnData.row_id = arrow.vectorFromArray(rowIds, new arrow.Utf8());

		const boolColumnNames: string[] = [];
		for (const col of schema.columns) {
			const values = userColumns.get(col.name)!;
			if (col.type === "boolean") {
				boolColumnNames.push(col.name);
				// Convert Bool -> Int8 to avoid Arrow JS IPC boolean buffer bug
				const int8Values = values.map((v) => (v === null || v === undefined ? null : v ? 1 : 0));
				columnData[col.name] = arrow.vectorFromArray(int8Values, new arrow.Int8());
			} else {
				const arrowType = lakeSyncTypeToArrow(col.type);
				columnData[col.name] = arrow.vectorFromArray(values, arrowType);
			}
		}

		const table = new arrow.Table(columnData);
		const ipcBytes = arrow.tableToIPC(table, "stream");
		const wasmTable = WasmTable.fromIPCStream(ipcBytes);

		let builder = new WriterPropertiesBuilder();
		builder = builder.setCompression(Compression.SNAPPY);

		if (boolColumnNames.length > 0) {
			const metadata = new Map<string, string>();
			metadata.set("lakesync:bool_columns", JSON.stringify(boolColumnNames));
			builder = builder.setKeyValueMetadata(metadata);
		}

		const writerProperties = builder.build();
		const parquetBytes = writeParquet(wasmTable, writerProperties);

		return Ok(parquetBytes);
	} catch (err) {
		const cause = err instanceof Error ? err : new Error(String(err));
		return Err(new FlushError(`Failed to write state to Parquet: ${cause.message}`, cause));
	}
}
