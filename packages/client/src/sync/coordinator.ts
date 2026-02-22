import {
	type ActionDiscovery,
	type ActionErrorResult,
	type ActionResult,
	type ConnectorDescriptor,
	HLC,
	type LakeSyncError,
	type Result,
} from "@lakesync/core";
import type { LocalDB } from "../db/local-db";
import type { ActionQueue } from "../queue/action-types";
import { IDBQueue } from "../queue/idb-queue";
import type { SyncQueue } from "../queue/types";
import { ActionProcessor } from "./action-processor";
import { AutoSyncScheduler } from "./auto-sync";
import { SyncEngine } from "./engine";
import { OnlineManager } from "./online-manager";
import { PullFirstStrategy, type SyncStrategy } from "./strategy";
import { SyncTracker } from "./tracker";
import type { TransportWithCapabilities } from "./transport";

/** Controls which operations syncOnce() / startAutoSync() performs */
export type SyncMode = "full" | "pushOnly" | "pullOnly";

/** Events emitted by SyncCoordinator */
export interface SyncEvents {
	/** Fired after remote deltas are applied locally. Count is the number of deltas applied. Tables is the optional list of affected tables. */
	onChange: (count: number, tables?: string[]) => void;
	/** Fired when a sync cycle (push + pull) begins. */
	onSyncStart: () => void;
	/** Fired after a successful sync cycle (push + pull) completes. */
	onSyncComplete: () => void;
	/** Fired when a sync error occurs. */
	onError: (error: Error) => void;
	/** Fired when an action completes (success or non-retryable failure). */
	onActionComplete: (actionId: string, result: ActionResult | ActionErrorResult) => void;
}

/** Optional configuration for dependency injection (useful for testing) */
export interface SyncCoordinatorConfig {
	/** Sync queue implementation. Defaults to IDBQueue. */
	queue?: SyncQueue;
	/** HLC instance. Defaults to a new HLC(). */
	hlc?: HLC;
	/** Client identifier. Defaults to a random UUID. */
	clientId?: string;
	/** Maximum retries before dead-lettering an entry. Defaults to 10. */
	maxRetries?: number;
	/** Sync mode. Defaults to "full" (push + pull). */
	syncMode?: SyncMode;
	/** Auto-sync interval in milliseconds. Defaults to 10000 (10 seconds). */
	autoSyncIntervalMs?: number;
	/** Polling interval when realtime transport is active (heartbeat). Defaults to 60000 (60 seconds). */
	realtimeHeartbeatMs?: number;
	/** Action queue for imperative command execution. */
	actionQueue?: ActionQueue;
	/** Maximum retries for actions before dead-lettering. Defaults to 5. */
	maxActionRetries?: number;
	/** Sync strategy. Defaults to PullFirstStrategy. */
	strategy?: SyncStrategy;
}

/** Auto-sync interval in milliseconds (every 10 seconds) */
const AUTO_SYNC_INTERVAL_MS = 10_000;

/** Auto-sync heartbeat interval when realtime is active (every 60 seconds) */
const REALTIME_HEARTBEAT_MS = 60_000;

/**
 * Coordinates local mutations (via SyncTracker) with gateway push/pull.
 *
 * Uses a {@link TransportWithCapabilities} abstraction to communicate with the gateway,
 * allowing both in-process (LocalTransport) and remote (HttpTransport) usage.
 *
 * Composes {@link SyncEngine} for core sync operations,
 * {@link AutoSyncScheduler} for periodic sync, and
 * {@link ActionProcessor} for imperative action execution.
 */
export class SyncCoordinator {
	readonly tracker: SyncTracker;
	/** The underlying sync engine for advanced consumers. */
	readonly engine: SyncEngine;
	private readonly transport: TransportWithCapabilities;
	private readonly onlineManager: OnlineManager;
	private readonly autoSyncScheduler: AutoSyncScheduler;
	private readonly actionProcessor: ActionProcessor | null;
	private listeners: { [K in keyof SyncEvents]: Array<SyncEvents[K]> } = {
		onChange: [],
		onSyncStart: [],
		onSyncComplete: [],
		onError: [],
		onActionComplete: [],
	};

	constructor(db: LocalDB, transport: TransportWithCapabilities, config?: SyncCoordinatorConfig) {
		this.transport = transport;
		const hlc = config?.hlc ?? new HLC();
		const queue = config?.queue ?? new IDBQueue();
		const clientId = config?.clientId ?? `client-${crypto.randomUUID()}`;

		// Create the sync engine
		this.engine = new SyncEngine({
			db,
			transport,
			queue,
			hlc,
			clientId,
			maxRetries: config?.maxRetries ?? 10,
			syncMode: config?.syncMode ?? "full",
			strategy: config?.strategy ?? new PullFirstStrategy(),
		});

		this.tracker = new SyncTracker(db, queue, hlc, clientId);

		// Compute auto-sync interval
		const autoSyncIntervalMs = config?.autoSyncIntervalMs ?? AUTO_SYNC_INTERVAL_MS;
		const realtimeHeartbeatMs = config?.realtimeHeartbeatMs ?? REALTIME_HEARTBEAT_MS;
		const intervalMs = transport.supportsRealtime ? realtimeHeartbeatMs : autoSyncIntervalMs;

		// Initialise composed modules
		this.onlineManager = new OnlineManager();
		this.onlineManager.onOnline = () => {
			void this.syncOnce();
		};
		this.autoSyncScheduler = new AutoSyncScheduler(() => this.syncOnce(), intervalMs);

		if (config?.actionQueue) {
			this.actionProcessor = new ActionProcessor({
				actionQueue: config.actionQueue,
				transport,
				clientId,
				hlc,
				maxRetries: config?.maxActionRetries ?? 5,
			});
			this.actionProcessor.setOnComplete((actionId, result) => {
				this.emit("onActionComplete", actionId, result);
			});
		} else {
			this.actionProcessor = null;
		}

		// Register broadcast handler for realtime transports
		if (this.transport.onBroadcast) {
			this.transport.onBroadcast((deltas, serverHlc) => {
				void this.engine
					.handleBroadcast(deltas, serverHlc)
					.then((result) => {
						if (result.remoteDeltasApplied > 0) {
							this.emit("onChange", result.remoteDeltasApplied);
						}
					})
					.catch((err) => {
						this.emit("onError", err instanceof Error ? err : new Error(String(err)));
					});
			});
		}
	}

	/**
	 * Subscribe to multiple events at once.
	 *
	 * @param handlers - Partial map of event names to listener functions.
	 * @returns An unsubscribe function that removes all registered listeners.
	 */
	subscribe(handlers: Partial<SyncEvents>): () => void {
		const keys = Object.keys(handlers) as Array<keyof SyncEvents>;
		for (const key of keys) {
			const listener = handlers[key];
			if (listener) {
				this.on(key, listener as SyncEvents[typeof key]);
			}
		}
		return () => {
			for (const key of keys) {
				const listener = handlers[key];
				if (listener) {
					this.off(key, listener as SyncEvents[typeof key]);
				}
			}
		};
	}

	/** Register an event listener */
	on<K extends keyof SyncEvents>(event: K, listener: SyncEvents[K]): void {
		this.listeners[event].push(listener);
	}

	/** Remove an event listener */
	off<K extends keyof SyncEvents>(event: K, listener: SyncEvents[K]): void {
		const listeners = this.listeners as Record<
			keyof SyncEvents,
			Array<SyncEvents[keyof SyncEvents]>
		>;
		listeners[event] = this.listeners[event].filter((fn) => fn !== listener);
	}

	private emit<K extends keyof SyncEvents>(event: K, ...args: Parameters<SyncEvents[K]>): void {
		for (const fn of this.listeners[event]) {
			try {
				(fn as (...a: Parameters<SyncEvents[K]>) => void)(...args);
			} catch {
				// Swallow listener errors to avoid breaking sync
			}
		}
	}

	/** Whether the client believes it is online. */
	get isOnline(): boolean {
		return this.onlineManager.isOnline;
	}

	/** Push pending deltas to the gateway via the transport */
	async pushToGateway(): Promise<void> {
		const result = await this.engine.push();
		if (result.deadLettered > 0) {
			this.emit(
				"onError",
				new Error(`Dead-lettered ${result.deadLettered} entries after max retries`),
			);
		}
	}

	/**
	 * Pull deltas from a named adapter source.
	 *
	 * Convenience wrapper around {@link pullFromGateway} that passes the
	 * `source` field through to the gateway, triggering an adapter-sourced
	 * pull instead of a buffer pull.
	 */
	async pullFrom(source: string): Promise<number> {
		const result = await this.engine.pullFrom(source);
		if (result.remoteDeltasApplied > 0) {
			this.emit("onChange", result.remoteDeltasApplied);
		}
		return result.remoteDeltasApplied;
	}

	/** Pull remote deltas from the gateway and apply them */
	async pullFromGateway(source?: string): Promise<number> {
		const result = await this.engine.pull(source);
		if (result.remoteDeltasApplied > 0) {
			this.emit("onChange", result.remoteDeltasApplied);
		}
		return result.remoteDeltasApplied;
	}

	/** Get the queue depth */
	async queueDepth(): Promise<number> {
		return this.engine.queueDepth();
	}

	/**
	 * Start auto-sync: periodic interval + visibility change handler.
	 * Synchronises (push + pull) on tab focus and every N seconds.
	 * Registers online/offline listeners to skip sync when offline
	 * and trigger an immediate sync on reconnect.
	 */
	startAutoSync(): void {
		this.transport.connect?.();
		this.autoSyncScheduler.start();
		this.onlineManager.start();
	}

	/** Perform a single sync cycle (push + pull + actions, depending on syncMode). */
	async syncOnce(): Promise<void> {
		if (!this.onlineManager.isOnline) return;
		this.emit("onSyncStart");
		try {
			const ran = await this.engine.syncOnce(
				() => this.processActionQueue(),
				(pullResult) => {
					if (pullResult.remoteDeltasApplied > 0) {
						this.emit("onChange", pullResult.remoteDeltasApplied);
					}
				},
				(pushResult) => {
					if (pushResult.deadLettered > 0) {
						this.emit(
							"onError",
							new Error(`Dead-lettered ${pushResult.deadLettered} entries after max retries`),
						);
					}
				},
			);
			if (ran) {
				this.emit("onSyncComplete");
			}
		} catch (err) {
			this.emit("onError", err instanceof Error ? err : new Error(String(err)));
		}
	}

	/**
	 * Submit an action for execution.
	 *
	 * Pushes the action to the ActionQueue and triggers immediate processing.
	 * The action will be sent to the gateway on the next sync cycle or
	 * immediately if not currently syncing.
	 *
	 * @param params - Partial action (connector, actionType, params). ActionId and HLC are generated.
	 */
	async executeAction(params: {
		connector: string;
		actionType: string;
		params: Record<string, unknown>;
		idempotencyKey?: string;
	}): Promise<void> {
		if (!this.actionProcessor) {
			this.emit("onError", new Error("No action queue configured"));
			return;
		}
		await this.actionProcessor.enqueue(params);
	}

	/**
	 * Process pending actions from the action queue.
	 *
	 * Delegates to the ActionProcessor if one is configured.
	 */
	async processActionQueue(): Promise<void> {
		if (!this.actionProcessor) return;
		await this.actionProcessor.processQueue();
	}

	/**
	 * Discover available connectors and their supported action types.
	 *
	 * Delegates to the transport's `describeActions()` method. Returns
	 * empty connectors when the transport does not support discovery.
	 */
	async describeActions(): Promise<Result<ActionDiscovery, LakeSyncError>> {
		if (this.actionProcessor) {
			return this.actionProcessor.describeActions();
		}
		if (!this.transport.describeActions) {
			return { ok: true, value: { connectors: {} } };
		}
		return this.transport.describeActions();
	}

	/**
	 * List available connector types and their configuration schemas.
	 *
	 * Delegates to the transport's `listConnectorTypes()` method. Returns
	 * an empty array when the transport does not support it.
	 */
	async listConnectorTypes(): Promise<Result<ConnectorDescriptor[], LakeSyncError>> {
		if (this.actionProcessor) {
			return this.actionProcessor.listConnectorTypes();
		}
		if (!this.transport.listConnectorTypes) {
			return { ok: true, value: [] };
		}
		return this.transport.listConnectorTypes();
	}

	/** Stop auto-sync and clean up listeners */
	stopAutoSync(): void {
		this.autoSyncScheduler.stop();
		this.onlineManager.stop();
		// Disconnect persistent transport (e.g. WebSocket)
		this.transport.disconnect?.();
	}
}
