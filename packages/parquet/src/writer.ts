import {
	deltasToArrowTable,
	Err,
	FlushError,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";
import * as arrow from "apache-arrow";
import {
	Compression,
	Table as WasmTable,
	WriterPropertiesBuilder,
	writeParquet,
} from "parquet-wasm/esm";
import { ensureWasmInitialised } from "./wasm";

/**
 * Metadata key used to store the names of columns that were originally
 * boolean but have been encoded as Int8 for Parquet compatibility.
 *
 * This works around an Arrow JS IPC serialisation issue where all-null
 * boolean columns produce a 0-byte data buffer that parquet-wasm rejects.
 */
const BOOL_COLUMNS_METADATA_KEY = "lakesync:bool_columns";

/**
 * Converts Bool columns in an Arrow Table to Int8 (true=1, false=0, null=null)
 * to work around an Arrow JS IPC serialisation bug where all-null boolean
 * columns produce invalid IPC bytes for parquet-wasm.
 *
 * @param table - The Arrow Table to patch
 * @returns A tuple of [patched table, list of boolean column names]
 */
function convertBoolColumnsToInt8(table: arrow.Table): [arrow.Table, string[]] {
	const boolColumnNames: string[] = [];
	const columns: Record<string, arrow.Vector> = {};

	for (const field of table.schema.fields) {
		const col = table.getChild(field.name);
		if (!col) continue;

		if (field.type instanceof arrow.Bool) {
			boolColumnNames.push(field.name);
			// Convert Bool -> Int8: true=1, false=0, null=null
			const int8Values: (number | null)[] = [];
			for (let i = 0; i < col.length; i++) {
				const val: unknown = col.get(i);
				if (val === null || val === undefined) {
					int8Values.push(null);
				} else {
					int8Values.push(val ? 1 : 0);
				}
			}
			columns[field.name] = arrow.vectorFromArray(int8Values, new arrow.Int8());
		} else {
			columns[field.name] = col;
		}
	}

	return [new arrow.Table(columns), boolColumnNames];
}

/**
 * Serialises an array of RowDelta objects into Parquet bytes.
 *
 * Converts deltas to an Apache Arrow Table via `deltasToArrowTable`,
 * then encodes the Arrow data as IPC stream bytes and writes them
 * to Parquet format using Snappy compression via parquet-wasm.
 *
 * Boolean columns are stored as Int8 (1/0/null) to work around an
 * Arrow JS IPC serialisation limitation. The original column types
 * are preserved in Parquet metadata for the reader to restore.
 *
 * @param deltas - The row deltas to serialise
 * @param schema - The table schema describing user-defined columns
 * @returns A Result containing the Parquet file as a Uint8Array, or a FlushError on failure
 */
export async function writeDeltasToParquet(
	deltas: RowDelta[],
	schema: TableSchema,
): Promise<Result<Uint8Array, FlushError>> {
	try {
		ensureWasmInitialised();

		// Convert deltas to Arrow Table
		const arrowTable = deltasToArrowTable(deltas, schema);

		// Convert Bool columns to Int8 to avoid Arrow JS IPC boolean buffer bug
		const [patchedTable, boolColumnNames] = convertBoolColumnsToInt8(arrowTable);

		// Serialise Arrow Table to IPC stream bytes
		const ipcBytes = arrow.tableToIPC(patchedTable, "stream");

		// Create a WASM table from the IPC stream
		const wasmTable = WasmTable.fromIPCStream(ipcBytes);

		// Configure writer properties with Snappy compression
		// and metadata to track original boolean columns.
		// Note: each builder method consumes the previous instance,
		// so the result must be reassigned at every step.
		let builder = new WriterPropertiesBuilder();
		builder = builder.setCompression(Compression.SNAPPY);

		if (boolColumnNames.length > 0) {
			const metadata = new Map<string, string>();
			metadata.set(BOOL_COLUMNS_METADATA_KEY, JSON.stringify(boolColumnNames));
			builder = builder.setKeyValueMetadata(metadata);
		}

		const writerProperties = builder.build();

		// Write to Parquet format
		const parquetBytes = writeParquet(wasmTable, writerProperties);

		return Ok(parquetBytes);
	} catch (err) {
		const cause = err instanceof Error ? err : new Error(String(err));
		return Err(new FlushError(`Failed to write deltas to Parquet: ${cause.message}`, cause));
	}
}
