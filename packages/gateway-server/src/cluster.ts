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
// LockStore — low-level key-value lock storage
// ---------------------------------------------------------------------------

/**
 * Low-level key-value lock storage interface.
 *
 * Provides direct lock operations decoupled from the sync delta path.
 * Implementations may use a dedicated database table, Redis, or any
 * other atomic compare-and-swap backend.
 *
 * The `holderId` parameter enables holder-scoped release — only the
 * holder that acquired a lock can release it.
 */
export interface LockStore {
	/**
	 * Attempt to acquire a lock for the given key.
	 *
	 * Must be atomic: if two callers race on the same key, exactly one
	 * must win. Returns `true` if the lock was acquired by this holder.
	 *
	 * @param key - Unique lock identifier (e.g. `"flush:gw-1"`)
	 * @param ttlMs - Time-to-live in milliseconds (advisory — implementations
	 *   are not required to enforce automatic expiry)
	 * @param holderId - Unique identifier for the lock holder instance
	 */
	tryAcquire(key: string, ttlMs: number, holderId: string): Promise<boolean>;

	/**
	 * Release a previously acquired lock.
	 *
	 * Must be idempotent — releasing a lock that is not held should be a no-op.
	 *
	 * @param key - Lock identifier to release
	 * @param holderId - Must match the holder that acquired the lock
	 */
	release(key: string, holderId: string): Promise<void>;
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
 * Distributed lock backed by a {@link LockStore}.
 *
 * Delegates lock acquisition and release to an explicit `LockStore`
 * implementation, keeping the lock mechanism completely decoupled from
 * the sync delta path (`insertDeltas`, `queryDeltasSince`, etc.).
 *
 * **Important caveats:**
 *
 * - This is an **approximation** for backends that lack native advisory
 *   lock support. For Postgres, strongly prefer {@link PostgresAdvisoryLock}
 *   which uses proper `pg_try_advisory_lock` / `pg_advisory_unlock` calls.
 *
 * - **Race conditions are possible** under concurrent access. The atomicity
 *   guarantee depends entirely on the `LockStore` implementation. A naive
 *   read-then-write store will exhibit TOCTOU races; only stores backed by
 *   atomic compare-and-swap or database-level serialisation are safe.
 *
 * - **No automatic TTL enforcement.** The `ttlMs` parameter is passed to
 *   the `LockStore` as advisory metadata. Unless the store implementation
 *   actively expires stale entries (e.g. via a background reaper or
 *   database TTL index), a crashed holder's lock will persist until
 *   manually released.
 *
 * - Release is **best-effort** — failures are silently caught. If the
 *   store is unreachable, the lock remains held until TTL expiry (if
 *   the store enforces it) or manual cleanup.
 */
export class AdapterBasedLock implements DistributedLock {
	private readonly store: LockStore;
	private readonly instanceId: string;

	constructor(store: LockStore, instanceId?: string) {
		this.store = store;
		this.instanceId = instanceId ?? crypto.randomUUID();
	}

	async acquire(key: string, ttlMs: number): Promise<boolean> {
		try {
			return await this.store.tryAcquire(key, ttlMs, this.instanceId);
		} catch {
			return false;
		}
	}

	async release(key: string): Promise<void> {
		try {
			await this.store.release(key, this.instanceId);
		} catch {
			// Best-effort release — see class-level caveats on TTL enforcement
		}
	}
}
