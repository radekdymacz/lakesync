import type { LakeAdapter } from "@lakesync/adapter";
import {
	type ColumnDelta,
	Err,
	HLC,
	type HLCTimestamp,
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

/** Per-column LWW state for incremental resolution. */
interface ColumnState {
	value: unknown;
	hlc: HLCTimestamp;
}

/** Per-row state tracking column-level LWW resolution. */
interface RowState {
	table: string;
	rowId: string;
	clientId: string;
	columns: Map<string, ColumnState>;
	latestHlc: HLCTimestamp;
	latestDeltaId: string;
	/** HLC of the most recent DELETE operation, or 0n if never deleted. */
	deleteHlc: HLCTimestamp;
}

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

		const resolveResult = await this.readAndResolveIncrementally(keysToCompact);
		if (!resolveResult.ok) return resolveResult;

		const { liveRows, deletedRows, bytesRead } = resolveResult.value;

		const writeResult = await this.writeOutputFiles(liveRows, deletedRows, outputPrefix);
		if (!writeResult.ok) return writeResult;

		return Ok({
			...writeResult.value,
			deltaFilesCompacted: keysToCompact.length,
			bytesRead,
		});
	}

	/**
	 * Read delta files one at a time and incrementally resolve to final row state.
	 *
	 * Memory usage is O(unique rows x columns) rather than O(total deltas),
	 * since each file's deltas are processed and discarded before reading the next.
	 */
	private async readAndResolveIncrementally(keysToCompact: string[]): Promise<
		Result<
			{
				liveRows: RowDelta[];
				deletedRows: Array<{ table: string; rowId: string }>;
				bytesRead: number;
			},
			LakeSyncError
		>
	> {
		const rowStates = new Map<string, RowState>();
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

			// Process each delta incrementally — no accumulation
			for (const delta of parseResult.value) {
				const k = rowKey(delta.table, delta.rowId);
				let state = rowStates.get(k);

				if (!state) {
					state = {
						table: delta.table,
						rowId: delta.rowId,
						clientId: delta.clientId,
						columns: new Map(),
						latestHlc: 0n as HLCTimestamp,
						latestDeltaId: delta.deltaId,
						deleteHlc: 0n as HLCTimestamp,
					};
					rowStates.set(k, state);
				}

				// Track overall latest HLC for metadata
				if (HLC.compare(delta.hlc, state.latestHlc) > 0) {
					state.latestHlc = delta.hlc;
					state.latestDeltaId = delta.deltaId;
					state.clientId = delta.clientId;
				}

				if (delta.op === "DELETE") {
					// Track the latest DELETE HLC
					if (HLC.compare(delta.hlc, state.deleteHlc) > 0) {
						state.deleteHlc = delta.hlc;
					}
				} else {
					// INSERT or UPDATE — apply column-level LWW
					for (const col of delta.columns) {
						const existing = state.columns.get(col.column);
						if (!existing || HLC.compare(delta.hlc, existing.hlc) > 0) {
							state.columns.set(col.column, {
								value: col.value,
								hlc: delta.hlc,
							});
						}
					}
				}
			}
			// parseResult.value is now eligible for GC — no reference kept
		}

		// Convert resolved states to output format
		const liveRows: RowDelta[] = [];
		const deletedRows: Array<{ table: string; rowId: string }> = [];

		for (const [, state] of rowStates) {
			// A row is deleted if the DELETE HLC is >= all column HLCs
			// (i.e. no column was written after the delete)
			const isDeleted =
				state.deleteHlc > 0n &&
				[...state.columns.values()].every((col) => HLC.compare(state.deleteHlc, col.hlc) >= 0);

			if (isDeleted || state.columns.size === 0) {
				deletedRows.push({ table: state.table, rowId: state.rowId });
			} else {
				// Filter out columns that were set before the delete
				const columns: ColumnDelta[] = [];
				for (const col of this.schema.columns) {
					const colState = state.columns.get(col.name);
					if (
						colState &&
						(state.deleteHlc === 0n || HLC.compare(colState.hlc, state.deleteHlc) > 0)
					) {
						columns.push({ column: col.name, value: colState.value });
					}
				}

				liveRows.push({
					op: "INSERT",
					table: state.table,
					rowId: state.rowId,
					clientId: state.clientId,
					columns,
					hlc: state.latestHlc,
					deltaId: state.latestDeltaId,
				});
			}
		}

		return Ok({ liveRows, deletedRows, bytesRead });
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
