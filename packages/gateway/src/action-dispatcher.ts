import type {
	Action,
	ActionDescriptor,
	ActionDiscovery,
	ActionExecutionError,
	ActionHandler,
	ActionPush,
	ActionResponse,
	ActionResult,
	ActionValidationError,
	AuthContext,
	HLCTimestamp,
	Result,
} from "@lakesync/core";
import { Err, Ok, validateAction } from "@lakesync/core";

/** Default maximum number of cached action results. */
const DEFAULT_MAX_CACHE_SIZE = 10_000;

/** Default TTL for idempotency cache entries (5 minutes). */
const DEFAULT_CACHE_TTL_MS = 5 * 60 * 1000;

/** Configuration for ActionDispatcher caches. */
export interface ActionCacheConfig {
	/** Maximum number of entries in the executed actions set and idempotency map. */
	maxSize?: number;
	/** Time-to-live for idempotency entries in milliseconds. */
	ttlMs?: number;
}

/** Cached action result with a timestamp for TTL eviction. */
interface CachedResult {
	value: ActionResult | { actionId: string; code: string; message: string; retryable: boolean };
	cachedAt: number;
}

/**
 * Dispatches imperative actions to registered handlers.
 *
 * Manages idempotency via actionId deduplication and idempotencyKey mapping.
 * Caches are bounded by max size and TTL to prevent unbounded growth.
 * Completely decoupled from the HLC clock — takes a callback for timestamp generation.
 */
export class ActionDispatcher {
	private actionHandlers: Map<string, ActionHandler> = new Map();
	private executedActions: Set<string> = new Set();
	private idempotencyMap: Map<string, CachedResult> = new Map();
	private readonly maxCacheSize: number;
	private readonly cacheTtlMs: number;

	constructor(handlers?: Record<string, ActionHandler>, cacheConfig?: ActionCacheConfig) {
		if (handlers) {
			for (const [name, handler] of Object.entries(handlers)) {
				this.actionHandlers.set(name, handler);
			}
		}
		this.maxCacheSize = cacheConfig?.maxSize ?? DEFAULT_MAX_CACHE_SIZE;
		this.cacheTtlMs = cacheConfig?.ttlMs ?? DEFAULT_CACHE_TTL_MS;
	}

	/**
	 * Dispatch an action push to registered handlers.
	 *
	 * Iterates over actions, dispatches each to the registered ActionHandler
	 * by connector name. Supports idempotency via actionId deduplication and
	 * idempotencyKey mapping.
	 *
	 * @param msg - The action push containing one or more actions.
	 * @param hlcNow - Callback to get the current server HLC timestamp.
	 * @param context - Optional auth context for permission checks.
	 * @returns A `Result` containing results for each action.
	 */
	async dispatch(
		msg: ActionPush,
		hlcNow: () => HLCTimestamp,
		context?: AuthContext,
	): Promise<Result<ActionResponse, ActionValidationError>> {
		// Sweep expired and over-limit entries before processing
		this.evictStaleEntries();

		const results: Array<
			ActionResult | { actionId: string; code: string; message: string; retryable: boolean }
		> = [];

		for (const action of msg.actions) {
			// Structural validation
			const validation = validateAction(action);
			if (!validation.ok) {
				return Err(validation.error);
			}

			// Idempotency — check actionId
			if (this.executedActions.has(action.actionId)) {
				const cached = this.getCachedResult(action.actionId);
				if (cached) {
					results.push(cached);
					continue;
				}
				// Already executed but no cached result — skip
				continue;
			}

			// Idempotency — check idempotencyKey
			if (action.idempotencyKey) {
				const cached = this.getCachedResult(`idem:${action.idempotencyKey}`);
				if (cached) {
					results.push(cached);
					continue;
				}
			}

			// Resolve handler
			const handler = this.actionHandlers.get(action.connector);
			if (!handler) {
				const errorResult = {
					actionId: action.actionId,
					code: "ACTION_NOT_SUPPORTED",
					message: `No action handler registered for connector "${action.connector}"`,
					retryable: false,
				};
				results.push(errorResult);
				this.cacheActionResult(action, errorResult);
				continue;
			}

			// Check action type is supported
			const supported = handler.supportedActions.some((d) => d.actionType === action.actionType);
			if (!supported) {
				const errorResult = {
					actionId: action.actionId,
					code: "ACTION_NOT_SUPPORTED",
					message: `Action type "${action.actionType}" not supported by connector "${action.connector}"`,
					retryable: false,
				};
				results.push(errorResult);
				this.cacheActionResult(action, errorResult);
				continue;
			}

			// Execute
			const execResult = await handler.executeAction(action, context);
			if (execResult.ok) {
				results.push(execResult.value);
				this.cacheActionResult(action, execResult.value);
			} else {
				const err = execResult.error;
				const errorResult = {
					actionId: action.actionId,
					code: err.code,
					message: err.message,
					retryable: "retryable" in err ? (err as ActionExecutionError).retryable : false,
				};
				results.push(errorResult);
				// Only cache non-retryable errors — retryable errors should be retried
				if (!errorResult.retryable) {
					this.cacheActionResult(action, errorResult);
				}
			}
		}

		const serverHlc = hlcNow();
		return Ok({ results, serverHlc });
	}

	/**
	 * Register a named action handler.
	 *
	 * @param name - Connector name (matches `Action.connector`).
	 * @param handler - The action handler to register.
	 */
	registerHandler(name: string, handler: ActionHandler): void {
		this.actionHandlers.set(name, handler);
	}

	/**
	 * Unregister a named action handler.
	 *
	 * @param name - The connector name to remove.
	 */
	unregisterHandler(name: string): void {
		this.actionHandlers.delete(name);
	}

	/**
	 * List all registered action handler names.
	 *
	 * @returns Array of registered connector names.
	 */
	listHandlers(): string[] {
		return [...this.actionHandlers.keys()];
	}

	/**
	 * Describe all registered action handlers and their supported actions.
	 *
	 * Returns a map of connector name to its {@link ActionDescriptor} array,
	 * enabling frontend discovery of available actions.
	 *
	 * @returns An {@link ActionDiscovery} object listing connectors and their actions.
	 */
	describe(): ActionDiscovery {
		const connectors: Record<string, ActionDescriptor[]> = {};
		for (const [name, handler] of this.actionHandlers) {
			connectors[name] = handler.supportedActions;
		}
		return { connectors };
	}

	/** Cache an action result for idempotency deduplication. */
	private cacheActionResult(
		action: Action,
		result: ActionResult | { actionId: string; code: string; message: string; retryable: boolean },
	): void {
		const entry: CachedResult = { value: result, cachedAt: Date.now() };
		this.executedActions.add(action.actionId);
		this.idempotencyMap.set(action.actionId, entry);
		if (action.idempotencyKey) {
			this.idempotencyMap.set(`idem:${action.idempotencyKey}`, entry);
		}
	}

	/** Get a cached result if it exists and hasn't expired. */
	private getCachedResult(
		key: string,
	):
		| ActionResult
		| { actionId: string; code: string; message: string; retryable: boolean }
		| undefined {
		const entry = this.idempotencyMap.get(key);
		if (!entry) return undefined;
		if (Date.now() - entry.cachedAt > this.cacheTtlMs) {
			this.idempotencyMap.delete(key);
			return undefined;
		}
		return entry.value;
	}

	/** Evict expired entries and trim to max size. */
	private evictStaleEntries(): void {
		const now = Date.now();

		// Evict expired entries
		for (const [key, entry] of this.idempotencyMap) {
			if (now - entry.cachedAt > this.cacheTtlMs) {
				this.idempotencyMap.delete(key);
				// Also remove from executedActions if it's an actionId (not idem: prefixed)
				if (!key.startsWith("idem:")) {
					this.executedActions.delete(key);
				}
			}
		}

		// Trim to max size — remove oldest entries first
		if (this.executedActions.size > this.maxCacheSize) {
			const excess = this.executedActions.size - this.maxCacheSize;
			let removed = 0;
			for (const actionId of this.executedActions) {
				if (removed >= excess) break;
				this.executedActions.delete(actionId);
				this.idempotencyMap.delete(actionId);
				removed++;
			}
		}
	}
}
