import type { LakeAdapter } from "@lakesync/adapter";
import {
	type ColumnDelta,
	Err,
	HLC,
	LakeSyncError,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
	applyDelta,
	rowKey,
} from "@lakesync/core";
import { readParquetToDeltas, writeDeltasToParquet } from "@lakesync/parquet";
import { writeEqualityDeletes } from "./equality-delete";
import type { CompactionConfig, CompactionResult } from "./types";

/**
 * Compacts delta files into consolidated base data files and equality delete files.
 *
 * Reads delta Parquet files from the lake adapter, resolves all deltas per row
 * using LWW (last-writer-wins based on HLC ordering), then writes the final
 * materialised state back as base files and delete files.
 */
export class Compactor {
	private readonly adapter: LakeAdapter;
	private readonly config: CompactionConfig;
	private readonly schema: TableSchema;

	/**
	 * Create a new Compactor instance.
	 *
	 * @param adapter - The lake adapter for reading/writing Parquet files
	 * @param config - Compaction configuration (thresholds and limits)
	 * @param schema - The table schema describing user-defined columns
	 */
	constructor(adapter: LakeAdapter, config: CompactionConfig, schema: TableSchema) {
		this.adapter = adapter;
		this.config = config;
		this.schema = schema;
	}

	/**
	 * Compact delta files into base data files.
	 *
	 * Reads delta files from storage, resolves all deltas per row using LWW,
	 * and writes consolidated base files + equality delete files.
	 *
	 * @param deltaFileKeys - Storage keys of the delta Parquet files to compact
	 * @param outputPrefix - Prefix for the output base/delete file keys
	 * @returns A Result containing the CompactionResult, or a LakeSyncError on failure
	 */
	async compact(
		deltaFileKeys: string[],
		outputPrefix: string,
	): Promise<Result<CompactionResult, LakeSyncError>> {
		// Skip if below minimum threshold
		if (deltaFileKeys.length < this.config.minDeltaFiles) {
			return Ok({
				baseFilesWritten: 0,
				deleteFilesWritten: 0,
				deltaFilesCompacted: 0,
				bytesRead: 0,
				bytesWritten: 0,
			});
		}

		// Limit to maxDeltaFiles
		const keysToCompact = deltaFileKeys.slice(0, this.config.maxDeltaFiles);

		// Step 1: Read all delta files and collect all deltas
		const allDeltas: RowDelta[] = [];
		let bytesRead = 0;

		for (const key of keysToCompact) {
			const getResult = await this.adapter.getObject(key);
			if (!getResult.ok) {
				return Err(
					new LakeSyncError(
						`Failed to read delta file: ${key}`,
						"COMPACTION_READ_ERROR",
						getResult.error,
					),
				);
			}

			const data = getResult.value;
			bytesRead += data.byteLength;

			const parseResult = await readParquetToDeltas(data);
			if (!parseResult.ok) {
				return Err(
					new LakeSyncError(
						`Failed to parse delta file: ${key}`,
						"COMPACTION_PARSE_ERROR",
						parseResult.error,
					),
				);
			}

			allDeltas.push(...parseResult.value);
		}

		// Step 2: Group deltas by row key
		const rowGroups = new Map<string, RowDelta[]>();
		for (const delta of allDeltas) {
			const key = rowKey(delta.table, delta.rowId);
			const group = rowGroups.get(key);
			if (group) {
				group.push(delta);
			} else {
				rowGroups.set(key, [delta]);
			}
		}

		// Step 3: For each row group, sort by HLC and apply deltas in order
		const liveRows: RowDelta[] = [];
		const deletedRows: Array<{ table: string; rowId: string }> = [];

		for (const [, deltas] of rowGroups) {
			// Sort deltas by HLC ascending for deterministic resolution
			deltas.sort((a, b) => HLC.compare(a.hlc, b.hlc));

			// Apply deltas sequentially to resolve final state
			let currentState: Record<string, unknown> | null = null;
			let latestDelta: RowDelta | undefined;

			for (const delta of deltas) {
				currentState = applyDelta(currentState, delta);
				latestDelta = delta;
			}

			if (!latestDelta) continue;

			if (currentState !== null) {
				// Live row: create an INSERT delta with the final materialised state
				const columns: ColumnDelta[] = [];
				for (const col of this.schema.columns) {
					if (col.name in currentState) {
						columns.push({ column: col.name, value: currentState[col.name] as unknown });
					}
				}

				liveRows.push({
					op: "INSERT",
					table: latestDelta.table,
					rowId: latestDelta.rowId,
					clientId: latestDelta.clientId,
					columns,
					hlc: latestDelta.hlc,
					deltaId: latestDelta.deltaId,
				});
			} else {
				// Deleted row: record table + rowId for the equality delete file
				deletedRows.push({
					table: latestDelta.table,
					rowId: latestDelta.rowId,
				});
			}
		}

		// Step 4: Write base file(s) for live rows
		let bytesWritten = 0;
		let baseFilesWritten = 0;
		let deleteFilesWritten = 0;

		if (liveRows.length > 0) {
			const writeResult = await writeDeltasToParquet(liveRows, this.schema);
			if (!writeResult.ok) {
				return Err(
					new LakeSyncError(
						"Failed to write base file",
						"COMPACTION_WRITE_ERROR",
						writeResult.error,
					),
				);
			}

			const baseData = writeResult.value;
			const timestamp = this.generateTimestamp();
			const basePath = `${outputPrefix}/base-${timestamp}.parquet`;

			const putResult = await this.adapter.putObject(
				basePath,
				baseData,
				"application/octet-stream",
			);
			if (!putResult.ok) {
				return Err(
					new LakeSyncError(
						`Failed to store base file: ${basePath}`,
						"COMPACTION_STORE_ERROR",
						putResult.error,
					),
				);
			}

			bytesWritten += baseData.byteLength;
			baseFilesWritten = 1;
		}

		// Step 5: Write equality delete file for deleted rows
		if (deletedRows.length > 0) {
			const writeResult = await writeEqualityDeletes(deletedRows, this.schema);
			if (!writeResult.ok) {
				return Err(
					new LakeSyncError(
						"Failed to write equality delete file",
						"COMPACTION_WRITE_ERROR",
						writeResult.error,
					),
				);
			}

			const deleteData = writeResult.value;
			const timestamp = this.generateTimestamp();
			const deletePath = `${outputPrefix}/delete-${timestamp}.parquet`;

			const putResult = await this.adapter.putObject(
				deletePath,
				deleteData,
				"application/octet-stream",
			);
			if (!putResult.ok) {
				return Err(
					new LakeSyncError(
						`Failed to store delete file: ${deletePath}`,
						"COMPACTION_STORE_ERROR",
						putResult.error,
					),
				);
			}

			bytesWritten += deleteData.byteLength;
			deleteFilesWritten = 1;
		}

		return Ok({
			baseFilesWritten,
			deleteFilesWritten,
			deltaFilesCompacted: keysToCompact.length,
			bytesRead,
			bytesWritten,
		});
	}

	/**
	 * Generate a timestamp string for output file naming.
	 * Uses the current wall clock time with a random suffix for uniqueness.
	 */
	private generateTimestamp(): string {
		const now = Date.now();
		const suffix = Math.random().toString(36).slice(2, 8);
		return `${now}-${suffix}`;
	}
}
