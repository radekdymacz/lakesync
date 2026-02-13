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

/** Immutable snapshot of buffer state — swapped atomically on each mutation. */
interface BufferSnapshot {
	readonly log: ReadonlyArray<RowDelta>;
	readonly index: ReadonlyMap<RowKey, RowDelta>;
	readonly deltaIds: ReadonlySet<string>;
	readonly estimatedBytes: number;
	readonly createdAt: number;
	readonly tableBytes: ReadonlyMap<string, number>;
	readonly tableLog: ReadonlyMap<string, ReadonlyArray<RowDelta>>;
}

/** Create an empty buffer snapshot. */
function emptySnapshot(): BufferSnapshot {
	return {
		log: [],
		index: new Map(),
		deltaIds: new Set(),
		estimatedBytes: 0,
		createdAt: Date.now(),
		tableBytes: new Map(),
		tableLog: new Map(),
	};
}

/**
 * Dual-structure delta buffer.
 *
 * Maintains an append-only log for event streaming (pull) and flush,
 * plus a row-level index for O(1) conflict resolution lookups.
 *
 * All mutable state is held in a single {@link BufferSnapshot} that is
 * swapped atomically on each mutation — no intermediate inconsistent
 * state is possible.
 */
export class DeltaBuffer {
	private state: BufferSnapshot = emptySnapshot();

	/** Append a delta to the log and upsert the index (post-conflict-resolution). */
	append(delta: RowDelta): void {
		const prev = this.state;
		const key = rowKey(delta.table, delta.rowId);
		const bytes = estimateDeltaBytes(delta);

		const newLog = [...prev.log, delta];
		const newIndex = new Map(prev.index);
		newIndex.set(key, delta);
		const newDeltaIds = new Set(prev.deltaIds);
		newDeltaIds.add(delta.deltaId);
		const newTableBytes = new Map(prev.tableBytes);
		newTableBytes.set(delta.table, (newTableBytes.get(delta.table) ?? 0) + bytes);
		const newTableLog = new Map(prev.tableLog);
		const existingTableEntries = newTableLog.get(delta.table);
		newTableLog.set(delta.table, existingTableEntries ? [...existingTableEntries, delta] : [delta]);

		this.state = {
			log: newLog,
			index: newIndex,
			deltaIds: newDeltaIds,
			estimatedBytes: prev.estimatedBytes + bytes,
			createdAt: prev.createdAt,
			tableBytes: newTableBytes,
			tableLog: newTableLog,
		};
	}

	/** Get the current merged state for a row (for conflict resolution). */
	getRow(key: RowKey): RowDelta | undefined {
		return this.state.index.get(key);
	}

	/** Check if a delta with this ID already exists in the log (for idempotency). */
	hasDelta(deltaId: string): boolean {
		return this.state.deltaIds.has(deltaId);
	}

	/** Return change events from the log since a given HLC. */
	getEventsSince(hlc: HLCTimestamp, limit: number): { deltas: RowDelta[]; hasMore: boolean } {
		const log = this.state.log;
		let lo = 0;
		let hi = log.length;
		while (lo < hi) {
			const mid = (lo + hi) >>> 1;
			if (HLC.compare(log[mid]!.hlc, hlc) <= 0) {
				lo = mid + 1;
			} else {
				hi = mid;
			}
		}
		const hasMore = log.length - lo > limit;
		return { deltas: log.slice(lo, lo + limit) as RowDelta[], hasMore };
	}

	/** Check if the buffer should be flushed based on size or age thresholds. */
	shouldFlush(config: { maxBytes: number; maxAgeMs: number }): boolean {
		const { log, estimatedBytes, createdAt } = this.state;
		if (log.length === 0) return false;
		return estimatedBytes >= config.maxBytes || Date.now() - createdAt >= config.maxAgeMs;
	}

	/** Per-table buffer statistics. */
	tableStats(): Array<{ table: string; byteSize: number; deltaCount: number }> {
		const { tableBytes, tableLog } = this.state;
		const stats: Array<{ table: string; byteSize: number; deltaCount: number }> = [];
		for (const [table, bytes] of tableBytes) {
			stats.push({
				table,
				byteSize: bytes,
				deltaCount: tableLog.get(table)?.length ?? 0,
			});
		}
		return stats;
	}

	/** Drain only the specified table's deltas, leaving other tables intact. */
	drainTable(table: string): RowDelta[] {
		const prev = this.state;
		const tableDeltas = prev.tableLog.get(table);
		if (!tableDeltas || tableDeltas.length === 0) return [];

		// Build new snapshot without the drained table
		const drainedDeltaIds = new Set(tableDeltas.map((d) => d.deltaId));
		const drainedRowKeys = new Set(tableDeltas.map((d) => rowKey(d.table, d.rowId)));

		const newLog = prev.log.filter((d) => d.table !== table);
		const newIndex = new Map(prev.index);
		for (const key of drainedRowKeys) {
			newIndex.delete(key);
		}
		const newDeltaIds = new Set(prev.deltaIds);
		for (const id of drainedDeltaIds) {
			newDeltaIds.delete(id);
		}
		const tableByteSize = prev.tableBytes.get(table) ?? 0;
		const newTableBytes = new Map(prev.tableBytes);
		newTableBytes.delete(table);
		const newTableLog = new Map(prev.tableLog);
		newTableLog.delete(table);

		this.state = {
			log: newLog,
			index: newIndex,
			deltaIds: newDeltaIds,
			estimatedBytes: prev.estimatedBytes - tableByteSize,
			createdAt: prev.createdAt,
			tableBytes: newTableBytes,
			tableLog: newTableLog,
		};

		return [...tableDeltas];
	}

	/**
	 * Snapshot the current buffer state without clearing it.
	 *
	 * Useful for inspecting the buffer contents without draining.
	 * Use {@link clear} separately after a successful flush for
	 * transactional semantics.
	 */
	snapshot(): { entries: RowDelta[]; byteSize: number } {
		const { log, estimatedBytes } = this.state;
		return { entries: [...log], byteSize: estimatedBytes };
	}

	/** Clear all buffer state. */
	clear(): void {
		this.state = emptySnapshot();
	}

	/** Drain the log for flush. Returns log entries and clears both structures. */
	drain(): RowDelta[] {
		const { entries } = this.snapshot();
		this.clear();
		return entries;
	}

	/** Number of log entries */
	get logSize(): number {
		return this.state.log.length;
	}

	/** Number of unique rows in the index */
	get indexSize(): number {
		return this.state.index.size;
	}

	/** Estimated byte size of the buffer */
	get byteSize(): number {
		return this.state.estimatedBytes;
	}

	/** Average byte size per delta in the buffer (0 if empty). */
	get averageDeltaBytes(): number {
		const { log, estimatedBytes } = this.state;
		return log.length === 0 ? 0 : estimatedBytes / log.length;
	}
}
