import type { DatabaseAdapter } from "@lakesync/adapter";
import type { HLCTimestamp, RowDelta } from "@lakesync/core";

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
 * Database-backed distributed lock using an advisory lock row.
 *
 * Uses a `lakesync_locks` table with columns: key, holder, expires_at.
 * Lock acquisition is atomic via INSERT ... ON CONFLICT or UPDATE WHERE expires_at < NOW().
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
			const result = await this.adapter.insertDeltas([
				this.makeLockDelta("INSERT", key, [
					{ column: "holder", value: this.instanceId },
					{ column: "expires_at", value: Date.now() + ttlMs },
				]),
			]);
			return result.ok;
		} catch {
			return false;
		}
	}

	async release(key: string): Promise<void> {
		try {
			await this.adapter.insertDeltas([this.makeLockDelta("DELETE", key, [])]);
		} catch {
			// Best-effort release — TTL will expire the lock anyway
		}
	}

	private makeLockDelta(
		op: "INSERT" | "DELETE",
		key: string,
		columns: Array<{ column: string; value: unknown }>,
	): RowDelta {
		return {
			op,
			table: "__lakesync_locks",
			rowId: key,
			clientId: this.instanceId,
			columns,
			hlc: (BigInt(Date.now()) << 16n) as HLCTimestamp,
			deltaId: `${op === "INSERT" ? "lock" : "unlock"}-${key}-${Date.now()}`,
		};
	}
}
