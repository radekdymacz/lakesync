import type { LakeAdapter } from "@lakesync/adapter";
import {
	applyDelta,
	type ColumnDelta,
	Err,
	HLC,
	LakeSyncError,
	Ok,
	type Result,
	type RowDelta,
	rowKey,
	type TableSchema,
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
		if (deltaFileKeys.length < this.config.minDeltaFiles) {
			return Ok({
				baseFilesWritten: 0,
				deleteFilesWritten: 0,
				deltaFilesCompacted: 0,
				bytesRead: 0,
				bytesWritten: 0,
			});
		}

		const keysToCompact = deltaFileKeys.slice(0, this.config.maxDeltaFiles);

		const readResult = await this.readAndGroupDeltas(keysToCompact);
		if (!readResult.ok) return readResult;

		const { liveRows, deletedRows } = this.resolveRowGroups(readResult.value.rowGroups);

		const writeResult = await this.writeOutputFiles(liveRows, deletedRows, outputPrefix);
		if (!writeResult.ok) return writeResult;

		return Ok({
			...writeResult.value,
			deltaFilesCompacted: keysToCompact.length,
			bytesRead: readResult.value.bytesRead,
		});
	}

	/** Read delta files and group the parsed deltas by row key. */
	private async readAndGroupDeltas(
		keysToCompact: string[],
	): Promise<Result<{ rowGroups: Map<string, RowDelta[]>; bytesRead: number }, LakeSyncError>> {
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

		const rowGroups = new Map<string, RowDelta[]>();
		for (const delta of allDeltas) {
			const k = rowKey(delta.table, delta.rowId);
			const group = rowGroups.get(k);
			if (group) {
				group.push(delta);
			} else {
				rowGroups.set(k, [delta]);
			}
		}

		return Ok({ rowGroups, bytesRead });
	}

	/** Resolve each row group into live rows (final state) or deleted rows. */
	private resolveRowGroups(rowGroups: Map<string, RowDelta[]>): {
		liveRows: RowDelta[];
		deletedRows: Array<{ table: string; rowId: string }>;
	} {
		const liveRows: RowDelta[] = [];
		const deletedRows: Array<{ table: string; rowId: string }> = [];

		for (const [, deltas] of rowGroups) {
			deltas.sort((a, b) => HLC.compare(a.hlc, b.hlc));

			let currentState: Record<string, unknown> | null = null;
			let latestDelta: RowDelta | undefined;

			for (const delta of deltas) {
				currentState = applyDelta(currentState, delta);
				latestDelta = delta;
			}

			if (!latestDelta) continue;

			if (currentState !== null) {
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
				deletedRows.push({
					table: latestDelta.table,
					rowId: latestDelta.rowId,
				});
			}
		}

		return { liveRows, deletedRows };
	}

	/** Write base Parquet file(s) for live rows and equality delete file(s) for deleted rows. */
	private async writeOutputFiles(
		liveRows: RowDelta[],
		deletedRows: Array<{ table: string; rowId: string }>,
		outputPrefix: string,
	): Promise<
		Result<
			{ baseFilesWritten: number; deleteFilesWritten: number; bytesWritten: number },
			LakeSyncError
		>
	> {
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

		return Ok({ baseFilesWritten, deleteFilesWritten, bytesWritten });
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
