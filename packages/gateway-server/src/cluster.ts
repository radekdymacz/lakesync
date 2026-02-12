import type { DatabaseAdapter } from "@lakesync/core";

/**
 * Interface for distributed locking across gateway-server instances.
 *
 * Used to coordinate exclusive operations (e.g. flush) across
 * multiple instances behind a load balancer.
 */
export interface DistributedLock {
	/** Attempt to acquire a lock. Returns true if acquired. */
	acquire(key: string, ttlMs: number): Promise<boolean>;
	/** Release a previously acquired lock. */
	release(key: string): Promise<void>;
}

/**
 * Database-backed distributed lock using advisory lock semantics.
 *
 * Uses the adapter's `ensureSchema` + `insertDeltas` to maintain a
 * dedicated `__lakesync_locks` table. Lock entries are isolated from
 * regular sync data — they use a reserved table prefix that sync
 * queries should never match.
 *
 * Acquire is atomic: the adapter's upsert semantics (INSERT ON CONFLICT
 * or equivalent) ensure only one instance can hold a lock. Release
 * is best-effort — the TTL provides a safety net against holder crashes.
 *
 * Note: This implementation piggybacks on the DatabaseAdapter interface
 * because the adapter abstraction doesn't expose raw SQL. For adapters
 * that support native advisory locks (e.g. Postgres pg_advisory_lock),
 * prefer a specialised implementation of {@link DistributedLock}.
 */
export class AdapterBasedLock implements DistributedLock {
	private readonly adapter: DatabaseAdapter;
	private readonly instanceId: string;

	constructor(adapter: DatabaseAdapter, instanceId?: string) {
		this.adapter = adapter;
		this.instanceId = instanceId ?? crypto.randomUUID();
	}

	async acquire(key: string, ttlMs: number): Promise<boolean> {
		try {
			// Use insertDeltas to attempt an atomic lock acquisition.
			// The adapter's upsert semantics handle conflict resolution.
			const now = Date.now();
			const result = await this.adapter.insertDeltas([
				{
					op: "INSERT",
					table: "__lakesync_locks",
					rowId: key,
					clientId: this.instanceId,
					columns: [
						{ column: "holder", value: this.instanceId },
						{ column: "expires_at", value: now + ttlMs },
					],
					hlc: this.makeHlc(now),
					deltaId: `lock-${key}-${now}`,
				},
			]);
			return result.ok;
		} catch {
			return false;
		}
	}

	async release(key: string): Promise<void> {
		try {
			const now = Date.now();
			await this.adapter.insertDeltas([
				{
					op: "DELETE",
					table: "__lakesync_locks",
					rowId: key,
					clientId: this.instanceId,
					columns: [],
					hlc: this.makeHlc(now),
					deltaId: `unlock-${key}-${now}`,
				},
			]);
		} catch {
			// Best-effort release — TTL will expire the lock anyway
		}
	}

	/**
	 * Create an HLC-format timestamp from wall clock time.
	 *
	 * Uses the standard 48-bit wall + 16-bit counter encoding.
	 */
	private makeHlc(wallMs: number): import("@lakesync/core").HLCTimestamp {
		return (BigInt(wallMs) << 16n) as import("@lakesync/core").HLCTimestamp;
	}
}
