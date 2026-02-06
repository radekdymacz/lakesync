import {
	type ColumnDelta,
	type DeltaOp,
	Err,
	FlushError,
	type HLCTimestamp,
	Ok,
	type Result,
	type RowDelta,
} from "@lakesync/core";
import { tableFromIPC } from "apache-arrow";
import { readParquet } from "parquet-wasm/esm";
import { ensureWasmInitialised } from "./wasm";

/** Set of system column names that are not user-defined columns */
const SYSTEM_COLUMNS = new Set(["op", "table", "rowId", "clientId", "hlc", "deltaId"]);

/**
 * Metadata key used to identify columns that were originally boolean
 * but stored as Int8 for Parquet compatibility.
 */
const BOOL_COLUMNS_METADATA_KEY = "lakesync:bool_columns";

/**
 * Deserialises Parquet bytes back into an array of RowDelta objects.
 *
 * Reads the Parquet data using parquet-wasm, converts to an Apache Arrow Table
 * via IPC stream, then iterates over rows to reconstruct RowDelta objects.
 * Int64 `hlc` values are cast back to branded HLCTimestamp bigints.
 * Columns stored as JSON-serialised Utf8 strings (objects and arrays) are
 * automatically parsed back to their original values.
 * Int8 columns marked as booleans in metadata are converted back to true/false.
 *
 * @param data - The Parquet file bytes to deserialise
 * @returns A Result containing the reconstructed RowDelta array, or a FlushError on failure
 */
export async function readParquetToDeltas(
	data: Uint8Array,
): Promise<Result<RowDelta[], FlushError>> {
	try {
		ensureWasmInitialised();

		// Read Parquet data into a WASM table
		const wasmTable = readParquet(data);

		// Convert to IPC stream, then to Arrow JS Table
		const ipcBytes = wasmTable.intoIPCStream();
		const arrowTable = tableFromIPC(ipcBytes);

		const numRows = arrowTable.numRows;
		const deltas: RowDelta[] = [];

		// Get system column vectors
		const opCol = arrowTable.getChild("op");
		const tableCol = arrowTable.getChild("table");
		const rowIdCol = arrowTable.getChild("rowId");
		const clientIdCol = arrowTable.getChild("clientId");
		const hlcCol = arrowTable.getChild("hlc");
		const deltaIdCol = arrowTable.getChild("deltaId");

		if (!opCol || !tableCol || !rowIdCol || !clientIdCol || !hlcCol || !deltaIdCol) {
			return Err(new FlushError("Parquet data is missing required system columns"));
		}

		// Identify user columns (everything that is not a system column)
		const userColumnNames: string[] = [];
		for (const field of arrowTable.schema.fields) {
			if (!SYSTEM_COLUMNS.has(field.name)) {
				userColumnNames.push(field.name);
			}
		}

		// Check metadata for boolean columns that were stored as Int8
		const boolColumnsRaw = arrowTable.schema.metadata.get(BOOL_COLUMNS_METADATA_KEY);
		const boolColumnSet = new Set<string>(
			boolColumnsRaw ? (JSON.parse(boolColumnsRaw) as string[]) : [],
		);

		// Reconstruct RowDelta objects from each row
		for (let i = 0; i < numRows; i++) {
			const op = opCol.get(i) as DeltaOp;
			const tableName = tableCol.get(i) as string;
			const rowId = rowIdCol.get(i) as string;
			const clientId = clientIdCol.get(i) as string;
			const hlc = hlcCol.get(i) as bigint as HLCTimestamp;
			const deltaId = deltaIdCol.get(i) as string;

			// Build column deltas from user columns
			const columns: ColumnDelta[] = [];
			for (const colName of userColumnNames) {
				const col = arrowTable.getChild(colName);
				if (!col) continue;

				const rawValue: unknown = col.get(i);

				// Skip null values — they represent missing columns for this delta
				if (rawValue === null || rawValue === undefined) {
					continue;
				}

				let value: unknown = rawValue;

				// Convert Int8 back to boolean if this column was originally boolean
				if (boolColumnSet.has(colName) && typeof rawValue === "number") {
					value = rawValue !== 0;
				}
				// Attempt to parse JSON-serialised strings back to objects/arrays.
				// JSON columns are stored as Utf8 strings via JSON.stringify during write.
				// We detect these by checking if the string starts with { or [.
				else if (typeof rawValue === "string") {
					const trimmed = rawValue.trim();
					if (
						(trimmed.startsWith("{") && trimmed.endsWith("}")) ||
						(trimmed.startsWith("[") && trimmed.endsWith("]"))
					) {
						try {
							value = JSON.parse(rawValue);
						} catch {
							// Not valid JSON — keep as plain string
						}
					}
				}

				columns.push({ column: colName, value });
			}

			deltas.push({
				op,
				table: tableName,
				rowId,
				clientId,
				columns,
				hlc,
				deltaId,
			});
		}

		return Ok(deltas);
	} catch (err) {
		const cause = err instanceof Error ? err : new Error(String(err));
		return Err(new FlushError(`Failed to read deltas from Parquet: ${cause.message}`, cause));
	}
}
