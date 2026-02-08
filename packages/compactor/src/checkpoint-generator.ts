import type { LakeAdapter } from "@lakesync/adapter";
import {
	Err,
	type HLCTimestamp,
	LakeSyncError,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";
import { readParquetToDeltas } from "@lakesync/parquet";
import { encodeSyncResponse } from "@lakesync/proto";

/** Configuration for checkpoint generation */
export interface CheckpointConfig {
	/** Max raw proto bytes per chunk. Tune to serving runtime memory budget. */
	chunkBytes: number;
}

/** Default checkpoint configuration (16 MB chunks for 128 MB DO) */
export const DEFAULT_CHECKPOINT_CONFIG: CheckpointConfig = {
	chunkBytes: 16 * 1024 * 1024,
};

/** Result of a checkpoint generation operation */
export interface CheckpointResult {
	/** Number of chunk files written */
	chunksWritten: number;
	/** Total bytes written across all chunks */
	bytesWritten: number;
	/** Snapshot HLC timestamp */
	snapshotHlc: HLCTimestamp;
}

/** Manifest stored alongside checkpoint chunks */
export interface CheckpointManifest {
	/** Snapshot HLC as decimal string (JSON-safe bigint) */
	snapshotHlc: string;
	/** ISO 8601 generation timestamp */
	generatedAt: string;
	/** Number of chunks */
	chunkCount: number;
	/** Total deltas across all chunks */
	totalDeltas: number;
	/** Ordered list of chunk file names */
	chunks: string[];
}

/** Estimated bytes per delta for chunk sizing (200 base + 50 per column) */
const ESTIMATED_BASE_BYTES = 200;
const ESTIMATED_BYTES_PER_COLUMN = 50;

/**
 * Generates checkpoint files from base Parquet files.
 *
 * Reads compacted base files, encodes ALL rows as proto SyncResponse chunks
 * sized to a configurable byte budget, and writes them to storage. Chunks
 * contain all rows (not per-user); filtering happens at serve time.
 */
export class CheckpointGenerator {
	private readonly adapter: LakeAdapter;
	private readonly gatewayId: string;
	private readonly config: CheckpointConfig;

	constructor(
		adapter: LakeAdapter,
		_schema: TableSchema,
		gatewayId: string,
		config?: CheckpointConfig,
	) {
		this.adapter = adapter;
		this.gatewayId = gatewayId;
		this.config = config ?? DEFAULT_CHECKPOINT_CONFIG;
	}

	/**
	 * Generate checkpoint chunks from base Parquet files.
	 *
	 * Reads each base file sequentially, accumulates deltas, and flushes
	 * chunks when the estimated byte size exceeds the configured threshold.
	 *
	 * @param baseFileKeys - Storage keys of the base Parquet files
	 * @param snapshotHlc - The HLC timestamp representing this snapshot point
	 * @returns A Result containing the CheckpointResult, or a LakeSyncError on failure
	 */
	async generate(
		baseFileKeys: string[],
		snapshotHlc: HLCTimestamp,
	): Promise<Result<CheckpointResult, LakeSyncError>> {
		if (baseFileKeys.length === 0) {
			return Ok({ chunksWritten: 0, bytesWritten: 0, snapshotHlc });
		}

		const prefix = `checkpoints/${this.gatewayId}`;
		const chunkNames: string[] = [];
		let totalBytesWritten = 0;
		let totalDeltas = 0;

		// Accumulator for current chunk
		let accumulator: RowDelta[] = [];
		let accumulatedBytes = 0;

		for (const key of baseFileKeys) {
			const getResult = await this.adapter.getObject(key);
			if (!getResult.ok) {
				return Err(
					new LakeSyncError(
						`Failed to read base file: ${key}`,
						"CHECKPOINT_READ_ERROR",
						getResult.error,
					),
				);
			}

			const parseResult = await readParquetToDeltas(getResult.value);
			if (!parseResult.ok) {
				return Err(
					new LakeSyncError(
						`Failed to parse base file: ${key}`,
						"CHECKPOINT_PARSE_ERROR",
						parseResult.error,
					),
				);
			}

			for (const delta of parseResult.value) {
				accumulator.push(delta);
				accumulatedBytes +=
					ESTIMATED_BASE_BYTES + delta.columns.length * ESTIMATED_BYTES_PER_COLUMN;

				if (accumulatedBytes >= this.config.chunkBytes) {
					const flushResult = await this.flushChunk(
						prefix,
						chunkNames.length,
						accumulator,
						snapshotHlc,
					);
					if (!flushResult.ok) return flushResult;

					totalBytesWritten += flushResult.value;
					totalDeltas += accumulator.length;
					chunkNames.push(this.chunkFileName(chunkNames.length));
					accumulator = [];
					accumulatedBytes = 0;
				}
			}
		}

		// Flush remaining accumulator
		if (accumulator.length > 0) {
			const flushResult = await this.flushChunk(
				prefix,
				chunkNames.length,
				accumulator,
				snapshotHlc,
			);
			if (!flushResult.ok) return flushResult;

			totalBytesWritten += flushResult.value;
			totalDeltas += accumulator.length;
			chunkNames.push(this.chunkFileName(chunkNames.length));
		}

		// Write manifest
		const manifest: CheckpointManifest = {
			snapshotHlc: snapshotHlc.toString(),
			generatedAt: new Date().toISOString(),
			chunkCount: chunkNames.length,
			totalDeltas,
			chunks: chunkNames,
		};

		const manifestBytes = new TextEncoder().encode(JSON.stringify(manifest));
		const manifestResult = await this.adapter.putObject(
			`${prefix}/manifest.json`,
			manifestBytes,
			"application/json",
		);

		if (!manifestResult.ok) {
			return Err(
				new LakeSyncError(
					"Failed to write checkpoint manifest",
					"CHECKPOINT_WRITE_ERROR",
					manifestResult.error,
				),
			);
		}

		totalBytesWritten += manifestBytes.byteLength;

		return Ok({
			chunksWritten: chunkNames.length,
			bytesWritten: totalBytesWritten,
			snapshotHlc,
		});
	}

	/**
	 * Get all storage keys produced by a checkpoint generation.
	 * Useful for adding to activeKeys in maintenance to prevent orphan removal.
	 */
	getCheckpointKeys(chunkCount: number): string[] {
		const prefix = `checkpoints/${this.gatewayId}`;
		const keys = [`${prefix}/manifest.json`];
		for (let i = 0; i < chunkCount; i++) {
			keys.push(`${prefix}/${this.chunkFileName(i)}`);
		}
		return keys;
	}

	private chunkFileName(index: number): string {
		return `chunk-${String(index).padStart(3, "0")}.bin`;
	}

	private async flushChunk(
		prefix: string,
		index: number,
		deltas: RowDelta[],
		snapshotHlc: HLCTimestamp,
	): Promise<Result<number, LakeSyncError>> {
		const encodeResult = encodeSyncResponse({
			deltas,
			serverHlc: snapshotHlc,
			hasMore: false,
		});

		if (!encodeResult.ok) {
			return Err(
				new LakeSyncError(
					`Failed to encode checkpoint chunk ${index}`,
					"CHECKPOINT_ENCODE_ERROR",
					encodeResult.error,
				),
			);
		}

		const data = encodeResult.value;
		const chunkKey = `${prefix}/${this.chunkFileName(index)}`;

		const putResult = await this.adapter.putObject(chunkKey, data, "application/octet-stream");
		if (!putResult.ok) {
			return Err(
				new LakeSyncError(
					`Failed to write checkpoint chunk: ${chunkKey}`,
					"CHECKPOINT_WRITE_ERROR",
					putResult.error,
				),
			);
		}

		return Ok(data.byteLength);
	}
}
