import type {
	ActionDescriptor,
	ActionDiscovery,
	ActionExecutionError,
	ActionHandler,
	ActionPush,
	ActionResponse,
	ActionValidationError,
	AuthContext,
	HLCTimestamp,
	Result,
} from "@lakesync/core";
import { Err, Ok, validateAction } from "@lakesync/core";
import {
	type CachedActionResult,
	type IdempotencyCache,
	type IdempotencyCacheConfig,
	MemoryIdempotencyCache,
} from "./idempotency-cache";

/** @deprecated Use {@link IdempotencyCacheConfig} instead. */
export type ActionCacheConfig = IdempotencyCacheConfig;

/**
 * Dispatches imperative actions to registered handlers.
 *
 * Routing and handler management are separated from idempotency caching,
 * which is delegated to an {@link IdempotencyCache} instance.
 * Completely decoupled from the HLC clock — takes a callback for timestamp generation.
 */
export class ActionDispatcher {
	private handlers: ReadonlyMap<string, ActionHandler> = new Map();
	private readonly cache: IdempotencyCache;

	/**
	 * Create an ActionDispatcher.
	 *
	 * @param handlers - Optional map of connector name to action handler.
	 * @param cacheConfig - Optional cache configuration (used when no `cache` is provided).
	 * @param cache - Optional pre-built idempotency cache; defaults to a {@link MemoryIdempotencyCache}.
	 */
	constructor(
		handlers?: Record<string, ActionHandler>,
		cacheConfig?: ActionCacheConfig,
		cache?: IdempotencyCache,
	) {
		if (handlers) {
			const initial = new Map<string, ActionHandler>();
			for (const [name, handler] of Object.entries(handlers)) {
				initial.set(name, handler);
			}
			this.handlers = initial;
		}
		this.cache = cache ?? new MemoryIdempotencyCache(cacheConfig);
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
		const results: Array<CachedActionResult> = [];

		for (const action of msg.actions) {
			// Structural validation
			const validation = validateAction(action);
			if (!validation.ok) {
				return Err(validation.error);
			}

			// Idempotency — check actionId
			if (this.cache.has(action.actionId)) {
				const cached = this.cache.get(action.actionId);
				if (cached) {
					results.push(cached);
					continue;
				}
				// Already executed but no cached result — skip
				continue;
			}

			// Idempotency — check idempotencyKey
			if (action.idempotencyKey) {
				const cached = this.cache.get(`idem:${action.idempotencyKey}`);
				if (cached) {
					results.push(cached);
					continue;
				}
			}

			// Resolve handler
			const handler = this.handlers.get(action.connector);
			if (!handler) {
				const errorResult = {
					actionId: action.actionId,
					code: "ACTION_NOT_SUPPORTED",
					message: `No action handler registered for connector "${action.connector}"`,
					retryable: false,
				};
				results.push(errorResult);
				this.cache.set(action.actionId, errorResult, action.idempotencyKey);
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
				this.cache.set(action.actionId, errorResult, action.idempotencyKey);
				continue;
			}

			// Execute
			const execResult = await handler.executeAction(action, context);
			if (execResult.ok) {
				results.push(execResult.value);
				this.cache.set(action.actionId, execResult.value, action.idempotencyKey);
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
					this.cache.set(action.actionId, errorResult, action.idempotencyKey);
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
		const next = new Map(this.handlers);
		next.set(name, handler);
		this.handlers = next;
	}

	/**
	 * Unregister a named action handler.
	 *
	 * @param name - The connector name to remove.
	 */
	unregisterHandler(name: string): void {
		const next = new Map(this.handlers);
		next.delete(name);
		this.handlers = next;
	}

	/**
	 * List all registered action handler names.
	 *
	 * @returns Array of registered connector names.
	 */
	listHandlers(): string[] {
		return [...this.handlers.keys()];
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
		for (const [name, handler] of this.handlers) {
			connectors[name] = handler.supportedActions;
		}
		return { connectors };
	}
}
