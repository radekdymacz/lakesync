import type { HLCTimestamp, RowDelta, RowKey } from "@lakesync/core";
import { HLC, rowKey } from "@lakesync/core";

/** Estimated bytes per column in a delta (for flush threshold estimation). */
const ESTIMATED_BYTES_PER_COLUMN = 50;
/** Estimated base bytes per delta entry (for flush threshold estimation). */
const ESTIMATED_BASE_BYTES_PER_DELTA = 200;

/**
 * Dual-structure delta buffer.
 *
 * Maintains an append-only log for event streaming (pull) and flush,
 * plus a row-level index for O(1) conflict resolution lookups.
 */
export class DeltaBuffer {
	private log: RowDelta[] = [];
	private index: Map<RowKey, RowDelta> = new Map();
	private deltaIds = new Set<string>();
	private estimatedBytes = 0;
	private createdAt: number = Date.now();

	/** Append a delta to the log and upsert the index (post-conflict-resolution). */
	append(delta: RowDelta): void {
		this.log.push(delta);
		const key = rowKey(delta.table, delta.rowId);
		this.index.set(key, delta);
		this.deltaIds.add(delta.deltaId);
		this.estimatedBytes += delta.columns.length * ESTIMATED_BYTES_PER_COLUMN + ESTIMATED_BASE_BYTES_PER_DELTA;
	}

	/** Get the current merged state for a row (for conflict resolution). */
	getRow(key: RowKey): RowDelta | undefined {
		return this.index.get(key);
	}

	/** Check if a delta with this ID already exists in the log (for idempotency). */
	hasDelta(deltaId: string): boolean {
		return this.deltaIds.has(deltaId);
	}

	/** Return change events from the log since a given HLC. */
	getEventsSince(hlc: HLCTimestamp, limit: number): { deltas: RowDelta[]; hasMore: boolean } {
		let lo = 0;
		let hi = this.log.length;
		while (lo < hi) {
			const mid = (lo + hi) >>> 1;
			if (HLC.compare(this.log[mid]!.hlc, hlc) <= 0) {
				lo = mid + 1;
			} else {
				hi = mid;
			}
		}
		const hasMore = this.log.length - lo > limit;
		return { deltas: this.log.slice(lo, lo + limit), hasMore };
	}

	/** Check if the buffer should be flushed based on size or age thresholds. */
	shouldFlush(config: { maxBytes: number; maxAgeMs: number }): boolean {
		if (this.log.length === 0) return false;
		return this.estimatedBytes >= config.maxBytes || Date.now() - this.createdAt >= config.maxAgeMs;
	}

	/** Drain the log for flush. Returns log entries and clears both structures. */
	drain(): RowDelta[] {
		const entries = [...this.log];
		this.log = [];
		this.index.clear();
		this.deltaIds.clear();
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
