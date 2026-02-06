import type { RowDelta, RowKey, HLCTimestamp } from '@lakesync/core';
import { rowKey, HLC } from '@lakesync/core';

/**
 * Dual-structure delta buffer.
 *
 * Maintains an append-only log for event streaming (pull) and flush,
 * plus a row-level index for O(1) conflict resolution lookups.
 */
export class DeltaBuffer {
	private log: RowDelta[] = [];
	private index: Map<RowKey, RowDelta> = new Map();
	private estimatedBytes = 0;
	private createdAt: number = Date.now();

	/** Append a delta to the log and upsert the index (post-conflict-resolution). */
	append(delta: RowDelta): void {
		this.log.push(delta);
		const key = rowKey(delta.table, delta.rowId);
		this.index.set(key, delta);
		// Rough byte estimate: JSON length (BigInt-safe serialisation)
		this.estimatedBytes += JSON.stringify(delta, (_key, value) =>
			typeof value === 'bigint' ? value.toString() : (value as unknown),
		).length;
	}

	/** Get the current merged state for a row (for conflict resolution). */
	getRow(key: RowKey): RowDelta | undefined {
		return this.index.get(key);
	}

	/** Check if a delta with this ID already exists in the log (for idempotency). */
	hasDelta(deltaId: string): boolean {
		return this.log.some((d) => d.deltaId === deltaId);
	}

	/** Return change events from the log since a given HLC. */
	getEventsSince(
		hlc: HLCTimestamp,
		limit: number,
	): { deltas: RowDelta[]; hasMore: boolean } {
		const filtered = this.log.filter((d) => HLC.compare(d.hlc, hlc) > 0);
		const hasMore = filtered.length > limit;
		return {
			deltas: filtered.slice(0, limit),
			hasMore,
		};
	}

	/** Check if the buffer should be flushed. */
	shouldFlush(config: { maxBytes: number; maxAgeMs: number }): boolean {
		if (this.log.length === 0) return false;
		if (this.estimatedBytes >= config.maxBytes) return true;
		if (Date.now() - this.createdAt >= config.maxAgeMs) return true;
		return false;
	}

	/** Drain the log for flush. Returns log entries and clears both structures. */
	drain(): RowDelta[] {
		const entries = [...this.log];
		this.log = [];
		this.index.clear();
		this.estimatedBytes = 0;
		this.createdAt = Date.now();
		return entries;
	}

	/** Number of log entries */
	get logSize(): number {
		return this.log.length;
	}

	/** Number of unique rows in the index */
	get indexSize(): number {
		return this.index.size;
	}

	/** Estimated byte size of the buffer */
	get byteSize(): number {
		return this.estimatedBytes;
	}
}
