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
 * All state lives in a single Map keyed by actionId or `idem:{idempotencyKey}`.
 * Stale entries are evicted on every `set()` call and the cache is
 * trimmed to the configured max size (counting only non-idem entries).
 */
/** Immutable cache snapshot. */
type CacheSnapshot = ReadonlyMap<string, CachedEntry>;

export class MemoryIdempotencyCache implements IdempotencyCache {
	private entries: CacheSnapshot = new Map();
	private readonly maxSize: number;
	private readonly ttlMs: number;

	constructor(config?: IdempotencyCacheConfig) {
		this.maxSize = config?.maxSize ?? DEFAULT_MAX_CACHE_SIZE;
		this.ttlMs = config?.ttlMs ?? DEFAULT_CACHE_TTL_MS;
	}

	/** {@inheritDoc IdempotencyCache.has} */
	has(actionId: string): boolean {
		const entry = this.entries.get(actionId);
		if (!entry) return false;
		if (Date.now() - entry.cachedAt > this.ttlMs) {
			return false;
		}
		return true;
	}

	/** {@inheritDoc IdempotencyCache.get} */
	get(key: string): CachedActionResult | undefined {
		const entry = this.entries.get(key);
		if (!entry) return undefined;
		if (Date.now() - entry.cachedAt > this.ttlMs) {
			return undefined;
		}
		return entry.value;
	}

	/** {@inheritDoc IdempotencyCache.set} */
	set(actionId: string, result: CachedActionResult, idempotencyKey?: string): void {
		const evicted = this.buildEvictedSnapshot();

		const next = new Map(evicted);
		const entry: CachedEntry = { value: result, cachedAt: Date.now() };
		next.set(actionId, entry);
		if (idempotencyKey) {
			next.set(`idem:${idempotencyKey}`, entry);
		}

		this.entries = next;
	}

	/** Build a new snapshot with expired entries evicted and size trimmed. */
	private buildEvictedSnapshot(): CacheSnapshot {
		const now = Date.now();
		const next = new Map<string, CachedEntry>();

		// Copy non-expired entries
		for (const [key, entry] of this.entries) {
			if (now - entry.cachedAt <= this.ttlMs) {
				next.set(key, entry);
			}
		}

		// Trim to max size — count only non-idem entries
		const actionKeys = [...next.keys()].filter((k) => !k.startsWith("idem:"));
		if (actionKeys.length > this.maxSize) {
			const excess = actionKeys.length - this.maxSize;
			for (let i = 0; i < excess; i++) {
				next.delete(actionKeys[i]!);
			}
		}

		return next;
	}
}
