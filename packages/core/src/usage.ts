// ---------------------------------------------------------------------------
// Usage Recording — fire-and-forget metering for billable events
// ---------------------------------------------------------------------------

/** Types of billable usage events. */
export type UsageEventType =
	| "push_deltas"
	| "pull_deltas"
	| "flush_bytes"
	| "flush_deltas"
	| "storage_bytes"
	| "api_call"
	| "ws_connection"
	| "action_executed";

/** A single usage event to record. */
export interface UsageEvent {
	/** Gateway that produced the event. */
	gatewayId: string;
	/** Organisation ID (resolved from gateway mapping, optional). */
	orgId?: string;
	/** Type of billable event. */
	eventType: UsageEventType;
	/** Count (deltas, bytes, connections, etc.). */
	count: number;
	/** When the event occurred. */
	timestamp: Date;
}

/** Aggregated usage counters for a time window. */
export interface UsageAggregate {
	/** Gateway that produced the events. */
	gatewayId: string;
	/** Organisation ID (if known). */
	orgId?: string;
	/** Type of billable event. */
	eventType: UsageEventType;
	/** Summed count for the window. */
	count: number;
	/** Start of the aggregation window. */
	windowStart: Date;
}

/**
 * Fire-and-forget usage recorder.
 *
 * `record()` never blocks the request path — it buffers events in memory.
 * `flush()` is called periodically to write aggregated counters to a
 * persistent store.
 */
export interface UsageRecorder {
	/** Record a usage event (fire-and-forget, never throws). */
	record(event: UsageEvent): void;
	/** Flush buffered aggregates to persistent storage. */
	flush(): Promise<void>;
}

/**
 * Persistent store for aggregated usage data.
 *
 * Implemented by the control-plane package (e.g. Postgres).
 */
export interface UsageStore {
	/** Write aggregated counters to persistent storage. */
	recordAggregates(aggregates: UsageAggregate[]): Promise<void>;
}

// ---------------------------------------------------------------------------
// Aggregation key for in-memory bucketing
// ---------------------------------------------------------------------------

function minuteKey(date: Date): number {
	return Math.floor(date.getTime() / 60_000);
}

function aggregateKey(gatewayId: string, eventType: UsageEventType, minute: number): string {
	return `${gatewayId}:${eventType}:${minute}`;
}

/** Entry in the in-memory aggregation buffer. */
interface AggregateEntry {
	gatewayId: string;
	orgId?: string;
	eventType: UsageEventType;
	count: number;
	minute: number;
}

// ---------------------------------------------------------------------------
// MemoryUsageRecorder — default in-process implementation
// ---------------------------------------------------------------------------

/**
 * In-memory usage recorder that aggregates events per minute.
 *
 * Events are bucketed by `gatewayId + eventType + minute`, then flushed
 * as aggregates to the configured {@link UsageStore}. When no store is
 * provided, `flush()` simply clears the buffer (useful for testing or
 * when metering is disabled).
 */
export class MemoryUsageRecorder implements UsageRecorder {
	private buffer = new Map<string, AggregateEntry>();
	private readonly store: UsageStore | undefined;

	constructor(store?: UsageStore) {
		this.store = store;
	}

	/** Record a usage event. Never throws. */
	record(event: UsageEvent): void {
		const minute = minuteKey(event.timestamp);
		const key = aggregateKey(event.gatewayId, event.eventType, minute);
		const existing = this.buffer.get(key);
		if (existing) {
			existing.count += event.count;
			// Prefer the most specific orgId
			if (event.orgId && !existing.orgId) {
				existing.orgId = event.orgId;
			}
		} else {
			this.buffer.set(key, {
				gatewayId: event.gatewayId,
				orgId: event.orgId,
				eventType: event.eventType,
				count: event.count,
				minute,
			});
		}
	}

	/**
	 * Flush aggregated counters to the store.
	 *
	 * Drains the buffer atomically — concurrent `record()` calls during
	 * flush are captured in a fresh buffer.
	 */
	async flush(): Promise<void> {
		const snapshot = this.buffer;
		this.buffer = new Map();

		if (snapshot.size === 0) return;

		const aggregates: UsageAggregate[] = [];
		for (const entry of snapshot.values()) {
			aggregates.push({
				gatewayId: entry.gatewayId,
				orgId: entry.orgId,
				eventType: entry.eventType,
				count: entry.count,
				windowStart: new Date(entry.minute * 60_000),
			});
		}

		if (this.store) {
			await this.store.recordAggregates(aggregates);
		}
	}

	/** Current number of distinct aggregate buckets (for testing). */
	get size(): number {
		return this.buffer.size;
	}

	/** Get all current aggregate entries (for testing). */
	get entries(): ReadonlyArray<AggregateEntry> {
		return Array.from(this.buffer.values());
	}
}
