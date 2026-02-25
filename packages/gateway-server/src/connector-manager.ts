// ---------------------------------------------------------------------------
// Connector Manager — unified registry-based connector lifecycle management
// ---------------------------------------------------------------------------

import {
	type AdapterFactoryRegistry,
	createDatabaseAdapter,
	createQueryFn,
	defaultAdapterFactoryRegistry,
} from "@lakesync/adapter";
import {
	type ConnectorConfig,
	type ConnectorFactory,
	type ConnectorFactoryRegistry,
	type ConnectorLifecycle,
	createConnectorFactoryRegistry,
	createPoller,
	createPollerRegistry,
	isActionHandler,
	type PollerRegistry,
	type PushTarget,
} from "@lakesync/core";
import type { ConfigStore, SyncGateway } from "@lakesync/gateway";
import {
	type HandlerResult,
	handleListConnectors,
	handleRegisterConnector,
	handleUnregisterConnector,
} from "@lakesync/gateway";
import { SourcePoller } from "./ingest/poller";
import type { IngestSourceConfig } from "./ingest/types";
import type { DeltaPersistence } from "./persistence";

// ---------------------------------------------------------------------------
// Adapters: wrap legacy registries into ConnectorFactory functions
// ---------------------------------------------------------------------------

/**
 * Wrap a {@link PollerRegistry} entry into a {@link ConnectorFactory}.
 *
 * The returned lifecycle delegates start/stop to the underlying
 * {@link BaseSourcePoller} which already implements the same contract.
 */
function wrapPollerFactory(
	pollerRegistry: PollerRegistry,
	persistence: DeltaPersistence | null,
): ConnectorFactory {
	return async (config, target) => {
		const poller = createPoller(config, target, pollerRegistry);

		// Restore persisted cursor state
		if (persistence) {
			const saved = persistence.loadCursor(config.name);
			if (saved) {
				poller.setCursorState(JSON.parse(saved));
			}
			poller.onCursorUpdate = (state) => {
				persistence.saveCursor(config.name, JSON.stringify(state));
			};
		}

		return {
			async start() {
				poller.start();
			},
			async stop() {
				poller.stop();
			},
			get isRunning() {
				return poller.isRunning;
			},
		};
	};
}

/**
 * Wrap an {@link AdapterFactoryRegistry} entry into a {@link ConnectorFactory}.
 *
 * The returned lifecycle registers the adapter as a gateway source,
 * optionally starts an ingest {@link SourcePoller}, and cleans up on stop.
 */
function wrapAdapterFactory(
	adapterRegistry: AdapterFactoryRegistry,
	gateway: SyncGateway,
	persistence: DeltaPersistence | null,
): ConnectorFactory {
	return async (config, _target) => {
		const adapterResult = createDatabaseAdapter(config, adapterRegistry);
		if (!adapterResult.ok) {
			throw new Error(adapterResult.error.message);
		}

		const adapter = adapterResult.value;
		gateway.registerSource(config.name, adapter);

		// Auto-register as action handler if the adapter supports actions
		if (isActionHandler(adapter)) {
			gateway.registerActionHandler(config.name, adapter);
		}

		let ingestPoller: SourcePoller | null = null;

		// Start ingest poller if configured
		if (config.ingest) {
			const queryFn = await createQueryFn(config);
			if (queryFn) {
				const pollerConfig: IngestSourceConfig = {
					name: config.name,
					queryFn,
					tables: config.ingest.tables.map((t) => ({
						table: t.table,
						query: t.query,
						rowIdColumn: t.rowIdColumn,
						strategy: t.strategy,
					})),
					intervalMs: config.ingest.intervalMs,
				};
				ingestPoller = new SourcePoller(pollerConfig, gateway);

				// Restore persisted cursor state
				if (persistence) {
					const saved = persistence.loadCursor(config.name);
					if (saved) {
						ingestPoller.setCursorState(JSON.parse(saved));
					}
					ingestPoller.onCursorUpdate = (state) => {
						persistence.saveCursor(config.name, JSON.stringify(state));
					};
				}

				ingestPoller.start();
			}
		}

		return {
			async start() {
				ingestPoller?.start();
			},
			async stop() {
				ingestPoller?.stop();
				await adapter.close();
				gateway.unregisterSource(config.name);
				gateway.unregisterActionHandler(config.name);
			},
			get isRunning() {
				return ingestPoller?.isRunning ?? true;
			},
		};
	};
}

/**
 * Build a unified {@link ConnectorFactoryRegistry} from the legacy
 * separate registries. For each type in the poller registry, wraps
 * it as a connector factory. Falls back to adapter registry for
 * unknown types.
 *
 * @internal Backward-compatibility bridge.
 */
export function buildConnectorFactoryRegistry(options: {
	pollerRegistry: PollerRegistry;
	adapterRegistry: AdapterFactoryRegistry;
	gateway: SyncGateway;
	persistence: DeltaPersistence | null;
}): ConnectorFactoryRegistry {
	// We create a registry that tries the poller registry first,
	// then falls back to the adapter registry at lookup time.
	const pollerFactory = wrapPollerFactory(options.pollerRegistry, options.persistence);
	const adapterFactory = wrapAdapterFactory(
		options.adapterRegistry,
		options.gateway,
		options.persistence,
	);

	return {
		get(type: string): ConnectorFactory | undefined {
			// Poller registry takes precedence (API-based connectors)
			if (options.pollerRegistry.get(type)) {
				return pollerFactory;
			}
			// Adapter registry fallback (database connectors)
			if (options.adapterRegistry.get(type)) {
				return adapterFactory;
			}
			return undefined;
		},
		with(type: string, factory: ConnectorFactory): ConnectorFactoryRegistry {
			// Build a concrete map from this dynamic registry, then add the new entry
			const concrete = createConnectorFactoryRegistry().with(type, factory);
			const self = this;
			return {
				get(t: string): ConnectorFactory | undefined {
					return concrete.get(t) ?? self.get(t);
				},
				with(t: string, f: ConnectorFactory): ConnectorFactoryRegistry {
					return self.with(t, f);
				},
			};
		},
	};
}

// ---------------------------------------------------------------------------
// ConnectorManager
// ---------------------------------------------------------------------------

/**
 * Manages connector registration, lifecycle, and teardown.
 *
 * Uses a single {@link ConnectorFactoryRegistry} for dispatch —
 * one lookup path for all connector types. Legacy separate registries
 * are bridged via {@link buildConnectorFactoryRegistry}.
 */
export class ConnectorManager {
	private readonly connectors = new Map<string, ConnectorLifecycle>();
	private readonly registry: ConnectorFactoryRegistry;
	private readonly gateway: SyncGateway;
	private readonly persistence: DeltaPersistence | null;

	constructor(
		private readonly configStore: ConfigStore,
		gateway: SyncGateway,
		options?: {
			/** Unified connector factory registry. Takes precedence over legacy registries. */
			connectorRegistry?: ConnectorFactoryRegistry;
			/** @deprecated Use `connectorRegistry` instead. */
			pollerRegistry?: PollerRegistry;
			/** @deprecated Use `connectorRegistry` instead. */
			adapterRegistry?: AdapterFactoryRegistry;
			persistence?: DeltaPersistence;
		},
	) {
		this.gateway = gateway;
		this.persistence = options?.persistence ?? null;

		if (options?.connectorRegistry) {
			this.registry = options.connectorRegistry;
		} else {
			// Backward compatibility: build unified registry from separate registries
			this.registry = buildConnectorFactoryRegistry({
				pollerRegistry: options?.pollerRegistry ?? createPollerRegistry(),
				adapterRegistry: options?.adapterRegistry ?? defaultAdapterFactoryRegistry(),
				gateway,
				persistence: this.persistence,
			});
		}
	}

	/**
	 * Register a connector from raw JSON body.
	 *
	 * Validates via shared handler, then dispatches to the unified
	 * {@link ConnectorFactoryRegistry} for a single-lookup creation path.
	 */
	async register(raw: string): Promise<HandlerResult> {
		// Use shared handler for validation and ConfigStore registration
		const result = await handleRegisterConnector(raw, this.configStore);
		if (result.status !== 200) {
			return result;
		}

		// Extract the registered config from the ConfigStore
		const registeredConnectors = await this.configStore.getConnectors();
		const registeredName = (result.body as { name: string }).name;
		const config = registeredConnectors[registeredName];
		if (!config) {
			return result;
		}

		// Single-lookup dispatch
		const factory = this.registry.get(config.type);
		if (!factory) {
			await this.rollbackRegistration(registeredConnectors, registeredName);
			return {
				status: 500,
				body: { error: `No connector factory registered for type "${config.type}"` },
			};
		}

		try {
			const lifecycle = await factory(config, this.gateway as unknown as PushTarget);
			await lifecycle.start();
			this.connectors.set(config.name, lifecycle);
			return result;
		} catch (err) {
			await this.rollbackRegistration(registeredConnectors, registeredName);
			const message = err instanceof Error ? err.message : String(err);
			return {
				status: 500,
				body: { error: `Failed to create connector for "${config.type}": ${message}` },
			};
		}
	}

	/**
	 * Unregister a connector by name.
	 *
	 * Stops the connector lifecycle and removes from the config store.
	 */
	async unregister(name: string): Promise<HandlerResult> {
		const result = await handleUnregisterConnector(name, this.configStore);
		if (result.status !== 200) {
			return result;
		}

		const lifecycle = this.connectors.get(name);
		if (lifecycle) {
			await lifecycle.stop();
			this.connectors.delete(name);
		}

		return result;
	}

	/**
	 * List registered connectors with live polling status.
	 */
	async list(): Promise<HandlerResult> {
		const result = await handleListConnectors(this.configStore);
		if (result.status !== 200) {
			return result;
		}

		const list = result.body as Array<{ name: string; type: string; hasIngest: boolean }>;
		const augmented = list.map((c) => ({
			...c,
			isPolling: this.connectors.get(c.name)?.isRunning ?? false,
		}));

		return { status: 200, body: augmented };
	}

	/** Stop all connectors and release resources. */
	async stopAll(): Promise<void> {
		const stops: Promise<void>[] = [];
		for (const [, lifecycle] of this.connectors) {
			stops.push(lifecycle.stop());
		}
		await Promise.all(stops);
		this.connectors.clear();
	}

	// -----------------------------------------------------------------------
	// Internal helpers
	// -----------------------------------------------------------------------

	private async rollbackRegistration(
		connectors: Record<string, ConnectorConfig>,
		name: string,
	): Promise<void> {
		delete connectors[name];
		await this.configStore.setConnectors(connectors);
	}
}
