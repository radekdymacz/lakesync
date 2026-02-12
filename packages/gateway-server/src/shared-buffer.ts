import type { DatabaseAdapter } from "@lakesync/adapter";
import type { HLCTimestamp, RowDelta, SyncResponse } from "@lakesync/core";
import { Err, Ok, type Result } from "@lakesync/core";

/** Consistency mode for shared buffer writes. */
export type ConsistencyMode = "eventual" | "strong";

/** Configuration for SharedBuffer. */
export interface SharedBufferConfig {
	/**
	 * Controls how shared adapter write failures are handled.
	 *
	 * - `"eventual"` (default): Shared writes are best-effort. Failures are
	 *   logged but do not fail the push. Local buffer remains authoritative.
	 * - `"strong"`: Shared writes must succeed. Failures are returned as errors,
	 *   allowing the caller to decide whether to fail the push.
	 */
	consistencyMode?: ConsistencyMode;
}

/** Error returned by SharedBuffer in strong consistency mode. */
export interface SharedBufferError {
	code: "SHARED_WRITE_FAILED";
	message: string;
}

/**
 * Write-through buffer that pushes to both the in-memory gateway buffer
 * and a shared database adapter for cross-instance visibility.
 *
 * Pull merges in-memory buffer results with adapter query results,
 * deduplicating by deltaId.
 */
export class SharedBuffer {
	private readonly consistencyMode: ConsistencyMode;

	constructor(
		private readonly sharedAdapter: DatabaseAdapter,
		config?: SharedBufferConfig,
	) {
		this.consistencyMode = config?.consistencyMode ?? "eventual";
	}

	/**
	 * Write-through push: write to shared adapter for cross-instance visibility.
	 *
	 * In "eventual" mode (default), failures are logged but do not fail the push.
	 * In "strong" mode, failures are returned as errors.
	 */
	async writeThroughPush(deltas: RowDelta[]): Promise<Result<void, SharedBufferError>> {
		try {
			const result = await this.sharedAdapter.insertDeltas(deltas);
			if (!result.ok) {
				if (this.consistencyMode === "strong") {
					return Err({ code: "SHARED_WRITE_FAILED" as const, message: result.error.message });
				}
				console.warn(
					`[lakesync] Shared buffer write failed (eventual mode): ${result.error.message}`,
				);
			}
			return Ok(undefined);
		} catch (error: unknown) {
			if (this.consistencyMode === "strong") {
				return Err({
					code: "SHARED_WRITE_FAILED" as const,
					message: error instanceof Error ? error.message : String(error),
				});
			}
			console.warn(
				`[lakesync] Shared buffer write error (eventual mode): ${error instanceof Error ? error.message : String(error)}`,
			);
			return Ok(undefined);
		}
	}

	/**
	 * Merge pull: combine local buffer results with shared adapter results.
	 *
	 * Deduplicates by deltaId to avoid returning the same delta twice.
	 */
	async mergePull(localResult: SyncResponse, sinceHlc: HLCTimestamp): Promise<SyncResponse> {
		try {
			const adapterResult = await this.sharedAdapter.queryDeltasSince(sinceHlc);
			if (!adapterResult.ok) {
				return localResult; // Fallback to local-only
			}

			// Deduplicate by deltaId
			const seenIds = new Set(localResult.deltas.map((d) => d.deltaId));
			const additional = adapterResult.value.filter((d) => !seenIds.has(d.deltaId));

			if (additional.length === 0) {
				return localResult;
			}

			const merged = [...localResult.deltas, ...additional];
			merged.sort((a, b) => (a.hlc < b.hlc ? -1 : a.hlc > b.hlc ? 1 : 0));

			return {
				deltas: merged,
				serverHlc: localResult.serverHlc,
				hasMore: true,
			};
		} catch {
			return localResult; // Fallback to local-only on error
		}
	}
}
