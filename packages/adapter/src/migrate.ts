import { type AdapterError, type HLCTimestamp, Ok } from "@lakesync/core";

import type { DatabaseAdapter } from "./db-types";

/** Options for migrating deltas between database adapters. */
export interface MigrateOptions {
	/** Source adapter to read from */
	from: DatabaseAdapter;
	/** Target adapter to write to */
	to: DatabaseAdapter;
	/** Optional: only migrate specific tables */
	tables?: string[];
	/** Batch size for writing (default: 1000) */
	batchSize?: number;
	/** Progress callback invoked after each batch write */
	onProgress?: (info: MigrateProgress) => void;
}

/** Progress information reported during migration. */
export interface MigrateProgress {
	/** Current batch number (1-based) */
	batch: number;
	/** Total deltas migrated so far */
	totalSoFar: number;
}

/** Result of a successful migration. */
export interface MigrateResult {
	/** Total number of deltas migrated */
	totalDeltas: number;
	/** Number of batches processed */
	batches: number;
}

/**
 * Migrate deltas from one database adapter to another.
 * Reads all matching deltas from the source, then writes them in batches to the target.
 * Idempotent via deltaId uniqueness in the target adapter.
 */
export async function migrateAdapter(
	opts: MigrateOptions,
): Promise<Result<MigrateResult, AdapterError>> {
	const batchSize = opts.batchSize ?? 1000;

	const readResult = await opts.from.queryDeltasSince(BigInt(0) as HLCTimestamp, opts.tables);
	if (!readResult.ok) {
		return readResult;
	}

	const deltas = readResult.value;

	if (deltas.length === 0) {
		return Ok({ totalDeltas: 0, batches: 0 });
	}

	let batchCount = 0;
	let totalSoFar = 0;

	for (let i = 0; i < deltas.length; i += batchSize) {
		const batch = deltas.slice(i, i + batchSize);
		const writeResult = await opts.to.insertDeltas(batch);
		if (!writeResult.ok) {
			return writeResult;
		}

		batchCount++;
		totalSoFar += batch.length;

		opts.onProgress?.({ batch: batchCount, totalSoFar });
	}

	return Ok({ totalDeltas: totalSoFar, batches: batchCount });
}
