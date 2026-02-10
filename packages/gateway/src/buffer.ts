import type { HLCTimestamp, RowDelta, RowKey } from "@lakesync/core";
import { HLC, rowKey } from "@lakesync/core";

/** Estimated base overhead per delta entry (metadata fields: deltaId, table, rowId, clientId, op + HLC bigint). */
const BASE_DELTA_OVERHEAD = 8 + 8 + 8 + 8 + 1;

/**
 * Estimate the byte size of a single column value.
 * Uses type-aware heuristics as a proxy for in-memory size.
 */
function estimateValueBytes(value: unknown): number {
	if (value === null || value === undefined) return 4;
	switch (typeof value) {
		case "boolean":
			return 4;
		case "number":
			return 8;
		case "bigint":
			return 8;
		case "string":
			return (value as string).length * 2; // UTF-16
		default:
			// Objects, arrays — use JSON.stringify as proxy
			try {
				return JSON.stringify(value).length;
			} catch {
				return 100; // fallback for circular refs etc.
			}
	}
}

/** Estimate the byte size of a RowDelta. */
function estimateDeltaBytes(delta: RowDelta): number {
	let bytes = BASE_DELTA_OVERHEAD;
	bytes += delta.deltaId.length;
	bytes += delta.table.length * 2;
	bytes += delta.rowId.length * 2;
	bytes += delta.clientId.length * 2;
	for (const col of delta.columns) {
		bytes += col.column.length * 2; // column name
		bytes += estimateValueBytes(col.value); // column value
	}
	return bytes;
}

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
	private tableBytes = new Map<string, number>();
	private tableLog = new Map<string, RowDelta[]>();

	/** Append a delta to the log and upsert the index (post-conflict-resolution). */
	append(delta: RowDelta): void {
		this.log.push(delta);
		const key = rowKey(delta.table, delta.rowId);
		this.index.set(key, delta);
		this.deltaIds.add(delta.deltaId);
		const bytes = estimateDeltaBytes(delta);
		this.estimatedBytes += bytes;
		// Per-table tracking
		this.tableBytes.set(delta.table, (this.tableBytes.get(delta.table) ?? 0) + bytes);
		const tableEntries = this.tableLog.get(delta.table);
		if (tableEntries) {
			tableEntries.push(delta);
		} else {
			this.tableLog.set(delta.table, [delta]);
		}
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

	/** Per-table buffer statistics. */
	tableStats(): Array<{ table: string; byteSize: number; deltaCount: number }> {
		const stats: Array<{ table: string; byteSize: number; deltaCount: number }> = [];
		for (const [table, bytes] of this.tableBytes) {
			stats.push({
				table,
				byteSize: bytes,
				deltaCount: this.tableLog.get(table)?.length ?? 0,
			});
		}
		return stats;
	}

	/** Drain only the specified table's deltas, leaving other tables intact. */
	drainTable(table: string): RowDelta[] {
		const tableDeltas = this.tableLog.get(table) ?? [];
		if (tableDeltas.length === 0) return [];

		// Remove from main log
		this.log = this.log.filter((d) => d.table !== table);

		// Remove from index and deltaIds
		for (const delta of tableDeltas) {
			this.index.delete(rowKey(delta.table, delta.rowId));
			this.deltaIds.delete(delta.deltaId);
		}

		// Adjust byte tracking
		const tableByteSize = this.tableBytes.get(table) ?? 0;
		this.estimatedBytes -= tableByteSize;
		this.tableBytes.delete(table);
		this.tableLog.delete(table);

		return tableDeltas;
	}

	/** Drain the log for flush. Returns log entries and clears both structures. */
	drain(): RowDelta[] {
		const entries = [...this.log];
		this.log = [];
		this.index.clear();
		this.deltaIds.clear();
		this.estimatedBytes = 0;
		this.createdAt = Date.now();
		this.tableBytes.clear();
		this.tableLog.clear();
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

	/** Average byte size per delta in the buffer (0 if empty). */
	get averageDeltaBytes(): number {
		return this.log.length === 0 ? 0 : this.estimatedBytes / this.log.length;
	}
}
