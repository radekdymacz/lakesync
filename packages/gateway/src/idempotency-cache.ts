import type { ActionResult } from "@lakesync/core";

/** Cached action result — either a successful result or an error descriptor. */
export type CachedActionResult =
	| ActionResult
	| { actionId: string; code: string; message: string; retryable: boolean };

/**
 * Interface for idempotency caching of action results.
 *
 * Implementations store action results keyed by actionId and optional
 * idempotencyKey, enabling deduplication of repeated action dispatches.
 */
export interface IdempotencyCache {
	/** Check whether an action ID has been executed. */
	has(actionId: string): boolean;
	/** Get a cached result by action ID or idempotency key. Returns `undefined` if not found or expired. */
	get(key: string): CachedActionResult | undefined;
	/** Cache an action result. Also stores by idempotencyKey if provided. */
	set(actionId: string, result: CachedActionResult, idempotencyKey?: string): void;
}

/** Default maximum number of cached action results. */
const DEFAULT_MAX_CACHE_SIZE = 10_000;

/** Default TTL for idempotency cache entries (5 minutes). */
const DEFAULT_CACHE_TTL_MS = 5 * 60 * 1000;

/** Configuration for {@link MemoryIdempotencyCache}. */
export interface IdempotencyCacheConfig {
	/** Maximum number of entries in the cache. */
	maxSize?: number;
	/** Time-to-live for cache entries in milliseconds. */
	ttlMs?: number;
}

/** Cached action result with a timestamp for TTL eviction. */
interface CachedEntry {
	value: CachedActionResult;
	cachedAt: number;
}

/**
 * In-memory idempotency cache with TTL expiration and bounded size.
 *
 * Stores executed action IDs in a Set for fast `has()` lookups, and
 * detailed results in a Map keyed by actionId or `idem:{idempotencyKey}`.
 * Stale entries are evicted on every `get()` call and periodically
 * trimmed to the configured max size on `set()`.
 */
export class MemoryIdempotencyCache implements IdempotencyCache {
	private readonly executedActions: Set<string> = new Set();
	private readonly entries: Map<string, CachedEntry> = new Map();
	private readonly maxSize: number;
	private readonly ttlMs: number;

	constructor(config?: IdempotencyCacheConfig) {
		this.maxSize = config?.maxSize ?? DEFAULT_MAX_CACHE_SIZE;
		this.ttlMs = config?.ttlMs ?? DEFAULT_CACHE_TTL_MS;
	}

	/** {@inheritDoc IdempotencyCache.has} */
	has(actionId: string): boolean {
		return this.executedActions.has(actionId);
	}

	/** {@inheritDoc IdempotencyCache.get} */
	get(key: string): CachedActionResult | undefined {
		const entry = this.entries.get(key);
		if (!entry) return undefined;
		if (Date.now() - entry.cachedAt > this.ttlMs) {
			this.entries.delete(key);
			return undefined;
		}
		return entry.value;
	}

	/** {@inheritDoc IdempotencyCache.set} */
	set(actionId: string, result: CachedActionResult, idempotencyKey?: string): void {
		this.evictStaleEntries();

		const entry: CachedEntry = { value: result, cachedAt: Date.now() };
		this.executedActions.add(actionId);
		this.entries.set(actionId, entry);
		if (idempotencyKey) {
			this.entries.set(`idem:${idempotencyKey}`, entry);
		}
	}

	/** Evict expired entries and trim to max size. */
	private evictStaleEntries(): void {
		const now = Date.now();

		// Evict expired entries
		for (const [key, entry] of this.entries) {
			if (now - entry.cachedAt > this.ttlMs) {
				this.entries.delete(key);
				// Also remove from executedActions if it's an actionId (not idem: prefixed)
				if (!key.startsWith("idem:")) {
					this.executedActions.delete(key);
				}
			}
		}

		// Trim to max size — remove oldest entries first
		if (this.executedActions.size > this.maxSize) {
			const excess = this.executedActions.size - this.maxSize;
			let removed = 0;
			for (const actionId of this.executedActions) {
				if (removed >= excess) break;
				this.executedActions.delete(actionId);
				this.entries.delete(actionId);
				removed++;
			}
		}
	}
}
