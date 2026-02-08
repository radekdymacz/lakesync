import type { LakeAdapter, ObjectInfo } from "@lakesync/adapter";
import { Err, HLC, LakeSyncError, Ok, type Result } from "@lakesync/core";
import type { CheckpointGenerator, CheckpointResult } from "./checkpoint-generator";
import type { Compactor } from "./compactor";
import type { CompactionResult } from "./types";

/** Configuration for the maintenance cycle */
export interface MaintenanceConfig {
	/** Number of recent snapshots to retain */
	retainSnapshots: number;
	/** Minimum age (ms) before orphaned files can be deleted */
	orphanAgeMs: number;
}

/** Default maintenance configuration values */
export const DEFAULT_MAINTENANCE_CONFIG: MaintenanceConfig = {
	retainSnapshots: 5,
	orphanAgeMs: 60 * 60 * 1000, // 1 hour
};

/** Report produced by a full maintenance cycle */
export interface MaintenanceReport {
	/** Result of the compaction step */
	compaction: CompactionResult;
	/** Number of expired snapshots removed */
	snapshotsExpired: number;
	/** Number of orphaned files removed */
	orphansRemoved: number;
	/** Result of checkpoint generation (if a generator was configured) */
	checkpoint?: CheckpointResult;
}

/**
 * Runs a full maintenance cycle: compact, expire snapshots, and clean orphans.
 *
 * The runner orchestrates the three maintenance phases in order:
 * 1. **Compact** — merge delta files into consolidated base/delete files
 * 2. **Expire** — (reserved for future snapshot expiry logic)
 * 3. **Clean** — remove orphaned files that are no longer referenced
 */
export class MaintenanceRunner {
	private readonly compactor: Compactor;
	private readonly adapter: LakeAdapter;
	private readonly config: MaintenanceConfig;
	private readonly checkpointGenerator: CheckpointGenerator | null;

	/**
	 * Create a new MaintenanceRunner instance.
	 *
	 * @param compactor - The compactor instance for merging delta files
	 * @param adapter - The lake adapter for storage operations
	 * @param config - Maintenance configuration (retention and age thresholds)
	 * @param checkpointGenerator - Optional checkpoint generator; when provided,
	 *   checkpoints are generated after successful compaction
	 */
	constructor(
		compactor: Compactor,
		adapter: LakeAdapter,
		config: MaintenanceConfig,
		checkpointGenerator?: CheckpointGenerator,
	) {
		this.compactor = compactor;
		this.adapter = adapter;
		this.config = config;
		this.checkpointGenerator = checkpointGenerator ?? null;
	}

	/**
	 * Run the full maintenance cycle: compact, expire, and clean.
	 *
	 * Compacts delta files into base/delete files, then removes orphaned
	 * storage objects that are no longer referenced by any active data.
	 * Files younger than `orphanAgeMs` are never deleted to avoid races
	 * with in-progress flush operations.
	 *
	 * @param deltaFileKeys - Storage keys of the delta Parquet files to compact
	 * @param outputPrefix - Prefix for the output base/delete file keys
	 * @param storagePrefix - Prefix under which all related storage files live
	 * @returns A Result containing the MaintenanceReport, or a LakeSyncError on failure
	 */
	async run(
		deltaFileKeys: string[],
		outputPrefix: string,
		storagePrefix: string,
	): Promise<Result<MaintenanceReport, LakeSyncError>> {
		// Step 1: Compact delta files into base/delete files
		const compactionResult = await this.compactor.compact(deltaFileKeys, outputPrefix);
		if (!compactionResult.ok) {
			return Err(
				new LakeSyncError(
					`Maintenance compaction failed: ${compactionResult.error.message}`,
					"MAINTENANCE_COMPACTION_ERROR",
					compactionResult.error,
				),
			);
		}

		const compaction = compactionResult.value;

		// Build the set of active keys:
		// - Delta files that were NOT consumed by compaction (i.e. those beyond maxDeltaFiles)
		// - All newly written output files under the output prefix
		const activeKeys = new Set<string>();

		// Delta files that were not compacted remain active
		const compactedCount = compaction.deltaFilesCompacted;
		for (let i = compactedCount; i < deltaFileKeys.length; i++) {
			activeKeys.add(deltaFileKeys[i]!);
		}

		// Discover newly written output files (base + delete files)
		const listOutputResult = await this.adapter.listObjects(outputPrefix);
		if (!listOutputResult.ok) {
			return Err(
				new LakeSyncError(
					`Failed to list output files: ${listOutputResult.error.message}`,
					"MAINTENANCE_LIST_ERROR",
					listOutputResult.error,
				),
			);
		}

		for (const obj of listOutputResult.value) {
			activeKeys.add(obj.key);
		}

		// Step 2: Generate checkpoints (if configured)
		let checkpoint: CheckpointResult | undefined;
		if (this.checkpointGenerator && compaction.baseFilesWritten > 0) {
			// Collect base file keys from the output
			const baseFileKeys = listOutputResult.value
				.filter((obj) => obj.key.endsWith(".parquet") && obj.key.includes("/base-"))
				.map((obj) => obj.key);

			if (baseFileKeys.length > 0) {
				// Use the latest HLC from the compacted data as the snapshot HLC.
				// Read the first base file's max HLC as an approximation.
				const snapshotHlc = HLC.encode(Date.now(), 0);

				const checkpointResult = await this.checkpointGenerator.generate(baseFileKeys, snapshotHlc);

				if (checkpointResult.ok) {
					checkpoint = checkpointResult.value;
					// Add checkpoint keys to active set to prevent orphan removal
					const checkpointKeys = this.checkpointGenerator.getCheckpointKeys(
						checkpoint.chunksWritten,
					);
					for (const key of checkpointKeys) {
						activeKeys.add(key);
					}
				}
				// Checkpoint failure is non-fatal — compaction still succeeded
			}
		}

		// Step 3: Remove orphaned files
		const orphanResult = await this.removeOrphans(storagePrefix, activeKeys);
		if (!orphanResult.ok) {
			return Err(
				new LakeSyncError(
					`Maintenance orphan removal failed: ${orphanResult.error.message}`,
					"MAINTENANCE_ORPHAN_ERROR",
					orphanResult.error,
				),
			);
		}

		return Ok({
			compaction,
			snapshotsExpired: 0,
			orphansRemoved: orphanResult.value,
			checkpoint,
		});
	}

	/**
	 * Delete orphaned files not referenced by any active data.
	 *
	 * Lists all files under the given storage prefix, compares each
	 * against the set of active keys, and deletes files that are both
	 * unreferenced and older than `orphanAgeMs`. This age guard
	 * prevents deletion of files created by in-progress flush operations.
	 *
	 * @param storagePrefix - The storage prefix to scan for orphaned files
	 * @param activeKeys - Set of storage keys that must be retained
	 * @returns A Result containing the count of deleted files, or a LakeSyncError on failure
	 */
	async removeOrphans(
		storagePrefix: string,
		activeKeys: Set<string>,
	): Promise<Result<number, LakeSyncError>> {
		const listResult = await this.adapter.listObjects(storagePrefix);
		if (!listResult.ok) {
			return Err(
				new LakeSyncError(
					`Failed to list objects for orphan removal: ${listResult.error.message}`,
					"MAINTENANCE_LIST_ERROR",
					listResult.error,
				),
			);
		}

		const now = Date.now();
		const orphanKeys = this.findOrphans(listResult.value, activeKeys, now);

		if (orphanKeys.length === 0) {
			return Ok(0);
		}

		const deleteResult = await this.adapter.deleteObjects(orphanKeys);
		if (!deleteResult.ok) {
			return Err(
				new LakeSyncError(
					`Failed to delete orphaned files: ${deleteResult.error.message}`,
					"MAINTENANCE_DELETE_ERROR",
					deleteResult.error,
				),
			);
		}

		return Ok(orphanKeys.length);
	}

	/**
	 * Identify orphaned file keys from a list of storage objects.
	 *
	 * A file is considered an orphan if it is not in the active keys set
	 * and its last modification time is older than the configured orphan age.
	 */
	private findOrphans(objects: ObjectInfo[], activeKeys: Set<string>, now: number): string[] {
		const orphans: string[] = [];
		for (const obj of objects) {
			if (activeKeys.has(obj.key)) {
				continue;
			}
			const age = now - obj.lastModified.getTime();
			if (age >= this.config.orphanAgeMs) {
				orphans.push(obj.key);
			}
		}
		return orphans;
	}
}
