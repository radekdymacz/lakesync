import {
	Err,
	FlushError,
	HLC,
	type HLCTimestamp,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";
import { readParquetToDeltas, writeDeltasToParquet } from "@lakesync/parquet";

/**
 * Minimal schema used for equality delete files.
 *
 * Equality delete files only need the row-identity columns (table + rowId),
 * which are already present as system columns in every RowDelta. Using an
 * empty user-column list keeps the Parquet file as small as possible.
 */
const EQUALITY_DELETE_SCHEMA: TableSchema = {
	table: "_equality_delete",
	columns: [],
};

/**
 * Sentinel HLC value used for synthetic equality-delete deltas.
 * The actual timestamp is irrelevant for equality deletes — only
 * `table` and `rowId` matter for row identification.
 */
const SENTINEL_HLC: HLCTimestamp = HLC.encode(0, 0);

/**
 * Write an Iceberg equality delete file.
 *
 * Contains only the equality columns (table + rowId) needed to identify
 * deleted rows. The file is encoded as a Parquet file using synthetic
 * DELETE RowDeltas with no user columns.
 *
 * @param deletedRows - Array of row identifiers (table + rowId) for deleted rows
 * @param _schema - The table schema (reserved for future use with custom equality columns)
 * @returns A Result containing the Parquet bytes, or a FlushError on failure
 */
export async function writeEqualityDeletes(
	deletedRows: Array<{ table: string; rowId: string }>,
	_schema: TableSchema,
): Promise<Result<Uint8Array, FlushError>> {
	if (deletedRows.length === 0) {
		return Ok(new Uint8Array(0));
	}

	try {
		// Build synthetic DELETE RowDeltas with only row-identity fields.
		// All other fields use sentinel values since only table + rowId
		// are meaningful for equality deletes.
		const syntheticDeltas: RowDelta[] = deletedRows.map((row, index) => ({
			op: "DELETE" as const,
			table: row.table,
			rowId: row.rowId,
			clientId: "_compactor",
			columns: [],
			hlc: SENTINEL_HLC,
			deltaId: `eq-delete-${index}`,
		}));

		return await writeDeltasToParquet(syntheticDeltas, EQUALITY_DELETE_SCHEMA);
	} catch (err) {
		const cause = err instanceof Error ? err : new Error(String(err));
		return Err(new FlushError(`Failed to write equality deletes: ${cause.message}`, cause));
	}
}

/**
 * Read an equality delete file back into row identifiers.
 *
 * Deserialises a Parquet equality delete file and extracts the
 * table + rowId pairs that identify deleted rows.
 *
 * @param data - The Parquet file bytes to read
 * @returns A Result containing the row identifiers, or a FlushError on failure
 */
export async function readEqualityDeletes(
	data: Uint8Array,
): Promise<Result<Array<{ table: string; rowId: string }>, FlushError>> {
	if (data.byteLength === 0) {
		return Ok([]);
	}

	const readResult = await readParquetToDeltas(data);
	if (!readResult.ok) {
		return Err(
			new FlushError(
				`Failed to read equality deletes: ${readResult.error.message}`,
				readResult.error,
			),
		);
	}

	const rows = readResult.value.map((delta) => ({
		table: delta.table,
		rowId: delta.rowId,
	}));

	return Ok(rows);
}
