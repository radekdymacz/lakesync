import type { DatabaseAdapter } from "@lakesync/adapter";
import type { HLCTimestamp, RowDelta, SyncResponse } from "@lakesync/core";

/**
 * Write-through buffer that pushes to both the in-memory gateway buffer
 * and a shared database adapter for cross-instance visibility.
 *
 * Pull merges in-memory buffer results with adapter query results,
 * deduplicating by deltaId.
 */
export class SharedBuffer {
	constructor(private readonly sharedAdapter: DatabaseAdapter) {}

	/**
	 * Write-through push: write to shared adapter for cross-instance visibility.
	 *
	 * Gateway buffer handles fast reads; shared adapter handles
	 * cross-instance visibility and durability.
	 */
	async writeThroughPush(deltas: RowDelta[]): Promise<void> {
		// Best-effort write to shared adapter (don't fail the push if this fails)
		try {
			await this.sharedAdapter.insertDeltas(deltas);
		} catch {
			// Shared adapter write failed — local buffer is still authoritative
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
