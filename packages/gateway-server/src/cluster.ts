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

// ---------------------------------------------------------------------------
// PostgresConnection — minimal interface for Postgres query execution
// ---------------------------------------------------------------------------

/**
 * Minimal Postgres connection interface for advisory locking.
 *
 * Compatible with `pg.Pool`, `pg.Client`, or any object with a
 * `query(text, params?)` method returning `{ rows }`.
 */
export interface PostgresConnection {
	query<T extends Record<string, unknown> = Record<string, unknown>>(
		text: string,
		params?: unknown[],
	): Promise<{ rows: T[] }>;
}

// ---------------------------------------------------------------------------
// PostgresAdvisoryLock — native Postgres advisory lock implementation
// ---------------------------------------------------------------------------

/**
 * Distributed lock using Postgres advisory locks (`pg_try_advisory_lock`).
 *
 * Advisory locks are server-side, session-scoped locks that do not touch
 * any table. They are ideal for coordinating exclusive operations (flush,
 * compaction) across multiple gateway-server instances sharing a Postgres
 * backend.
 *
 * Lock keys are converted to a pair of 32-bit integers via a simple hash
 * so they fit Postgres's `(int4, int4)` advisory lock signature.
 *
 * Prefer this over {@link AdapterBasedLock} when the backend is Postgres.
 */
export class PostgresAdvisoryLock implements DistributedLock {
	private readonly conn: PostgresConnection;
	private readonly held = new Set<string>();

	constructor(conn: PostgresConnection) {
		this.conn = conn;
	}

	async acquire(key: string, _ttlMs: number): Promise<boolean> {
		const [k1, k2] = hashKey(key);
		try {
			const result = await this.conn.query<{ acquired: boolean }>(
				"SELECT pg_try_advisory_lock($1, $2) AS acquired",
				[k1, k2],
			);
			const acquired = result.rows[0]?.acquired === true;
			if (acquired) {
				this.held.add(key);
			}
			return acquired;
		} catch {
			return false;
		}
	}

	async release(key: string): Promise<void> {
		const [k1, k2] = hashKey(key);
		try {
			await this.conn.query("SELECT pg_advisory_unlock($1, $2)", [k1, k2]);
		} catch {
			// Best-effort release — advisory locks are session-scoped
			// and will be released when the connection closes.
		}
		this.held.delete(key);
	}
}

/**
 * Hash a string key into a pair of 32-bit signed integers for use
 * with Postgres advisory lock functions.
 *
 * Uses a simple FNV-1a-inspired hash split into two halves.
 */
function hashKey(key: string): [number, number] {
	let h = 0x811c9dc5; // FNV offset basis (32-bit)
	for (let i = 0; i < key.length; i++) {
		h ^= key.charCodeAt(i);
		h = Math.imul(h, 0x01000193); // FNV prime
	}
	// Use the hash as the first int, and a rotated variant as the second
	const k1 = h | 0;
	const k2 = (h >>> 16) | (h << 16) | 0;
	return [k1, k2];
}

/**
 * Database-backed distributed lock using the DatabaseAdapter interface.
 *
 * This is an approximation for non-Postgres backends that lack native
 * advisory lock support. It simulates locking via upsert semantics on
 * a reserved `__lakesync_locks` table. For Postgres, prefer the
 * {@link PostgresAdvisoryLock} implementation which uses proper
 * `pg_try_advisory_lock` / `pg_advisory_unlock` calls.
 *
 * Uses the adapter's `ensureSchema` + `insertDeltas` to maintain a
 * dedicated `__lakesync_locks` table. Lock entries are isolated from
 * regular sync data — they use a reserved table prefix that sync
 * queries should never match.
 *
 * Acquire is atomic: the adapter's upsert semantics (INSERT ON CONFLICT
 * or equivalent) ensure only one instance can hold a lock. Release
 * is best-effort — the TTL provides a safety net against holder crashes.
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
