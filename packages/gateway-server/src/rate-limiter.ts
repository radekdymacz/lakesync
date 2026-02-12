// ---------------------------------------------------------------------------
// Per-Client Rate Limiter — fixed-window token bucket
// ---------------------------------------------------------------------------

/** Configuration for the per-client rate limiter. */
export interface RateLimiterConfig {
	/** Maximum requests per window (default: 100). */
	maxRequests?: number;
	/** Window size in milliseconds (default: 60_000). */
	windowMs?: number;
}

interface ClientWindow {
	count: number;
	windowStart: number;
}

const DEFAULT_MAX_REQUESTS = 100;
const DEFAULT_WINDOW_MS = 60_000;
const CLEANUP_INTERVAL_MS = 60_000;

/**
 * Fixed-window per-client rate limiter.
 *
 * Tracks request counts per client within a sliding window. Stale entries
 * are periodically cleaned up to prevent unbounded memory growth.
 */
export class RateLimiter {
	private readonly maxRequests: number;
	private readonly windowMs: number;
	private readonly clients = new Map<string, ClientWindow>();
	private cleanupTimer: ReturnType<typeof setInterval> | null = null;

	constructor(config: RateLimiterConfig = {}) {
		this.maxRequests = config.maxRequests ?? DEFAULT_MAX_REQUESTS;
		this.windowMs = config.windowMs ?? DEFAULT_WINDOW_MS;

		this.cleanupTimer = setInterval(() => this.cleanup(), CLEANUP_INTERVAL_MS);
		// Allow the process to exit without waiting for the cleanup timer
		if (this.cleanupTimer.unref) {
			this.cleanupTimer.unref();
		}
	}

	/**
	 * Attempt to consume one request token for the given client.
	 *
	 * @returns `true` if the request is allowed, `false` if rate-limited.
	 */
	tryConsume(clientId: string): boolean {
		const now = Date.now();
		const entry = this.clients.get(clientId);

		if (!entry || now - entry.windowStart >= this.windowMs) {
			// New window
			this.clients.set(clientId, { count: 1, windowStart: now });
			return true;
		}

		if (entry.count >= this.maxRequests) {
			return false;
		}

		entry.count++;
		return true;
	}

	/**
	 * Calculate the number of seconds until the current window resets
	 * for a given client. Used for the Retry-After header.
	 */
	retryAfterSeconds(clientId: string): number {
		const entry = this.clients.get(clientId);
		if (!entry) return 0;
		const elapsed = Date.now() - entry.windowStart;
		const remaining = Math.max(0, this.windowMs - elapsed);
		return Math.ceil(remaining / 1000);
	}

	/** Remove all tracked clients and reset state. */
	reset(): void {
		this.clients.clear();
	}

	/** Stop the periodic cleanup timer. */
	dispose(): void {
		if (this.cleanupTimer) {
			clearInterval(this.cleanupTimer);
			this.cleanupTimer = null;
		}
		this.clients.clear();
	}

	/** Remove stale entries whose window has expired. */
	private cleanup(): void {
		const now = Date.now();
		for (const [clientId, entry] of this.clients) {
			if (now - entry.windowStart >= this.windowMs) {
				this.clients.delete(clientId);
			}
		}
	}
}
