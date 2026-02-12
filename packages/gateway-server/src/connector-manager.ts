// ---------------------------------------------------------------------------
// Connector Manager — registry-based connector registration and lifecycle
// ---------------------------------------------------------------------------

import {
	type AdapterFactoryRegistry,
	createDatabaseAdapter,
	createQueryFn,
	defaultAdapterFactoryRegistry,
} from "@lakesync/adapter";
import {
	type ConnectorConfig,
	createPoller,
	createPollerRegistry,
	type DatabaseAdapter,
	isActionHandler,
	type PollerRegistry,
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

/** Common lifecycle interface for source pollers (SQL or API-based). */
interface Poller {
	start(): void;
	stop(): void;
	readonly isRunning: boolean;
}

/**
 * Manages connector registration, adapter creation, and poller lifecycle.
 *
 * Uses {@link PollerRegistry} and {@link AdapterFactoryRegistry} for
 * dispatch — no if/else chains for specific connector types.
 */
export class ConnectorManager {
	private readonly adapters = new Map<string, DatabaseAdapter>();
	private readonly pollers = new Map<string, Poller>();
	private readonly pollerRegistry: PollerRegistry;
	private readonly adapterRegistry: AdapterFactoryRegistry;

	constructor(
		private readonly configStore: ConfigStore,
		private readonly gateway: SyncGateway,
		options?: {
			pollerRegistry?: PollerRegistry;
			adapterRegistry?: AdapterFactoryRegistry;
		},
	) {
		this.pollerRegistry = options?.pollerRegistry ?? createPollerRegistry();
		this.adapterRegistry = options?.adapterRegistry ?? defaultAdapterFactoryRegistry();
	}

	/**
	 * Register a connector from raw JSON body.
	 *
	 * Validates via shared handler, then dispatches to the appropriate
	 * registry: poller registry for API-based connectors, adapter registry
	 * for database-based connectors.
	 */
	async register(raw: string): Promise<HandlerResult> {
		// Use shared handler for validation and ConfigStore registration
		const result = await handleRegisterConnector(raw, this.configStore);
		if (result.status !== 200) {
			return result;
		}

		// Extract the registered config from the ConfigStore
		const connectors = await this.configStore.getConnectors();
		const registeredName = (result.body as { name: string }).name;
		const config = connectors[registeredName];
		if (!config) {
			return result;
		}

		// Path 1: PollerRegistry has a factory — create an API-based poller
		const pollerFactory = this.pollerRegistry.get(config.type);
		if (pollerFactory) {
			try {
				const poller = createPoller(config, this.gateway, this.pollerRegistry);
				poller.start();
				this.pollers.set(config.name, poller);
				return result;
			} catch (err) {
				await this.rollbackRegistration(connectors, registeredName);
				const message = err instanceof Error ? err.message : String(err);
				return {
					status: 500,
					body: { error: `Failed to create poller for "${config.type}": ${message}` },
				};
			}
		}

		// Path 2: AdapterFactoryRegistry has a factory — database-based connector
		const adapterResult = createDatabaseAdapter(config, this.adapterRegistry);
		if (!adapterResult.ok) {
			await this.rollbackRegistration(connectors, registeredName);
			return { status: 500, body: { error: adapterResult.error.message } };
		}

		const adapter = adapterResult.value;
		this.gateway.registerSource(config.name, adapter);
		this.adapters.set(config.name, adapter);

		// Auto-register as action handler if the adapter supports actions
		if (isActionHandler(adapter)) {
			this.gateway.registerActionHandler(config.name, adapter);
		}

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
				const poller = new SourcePoller(pollerConfig, this.gateway);
				poller.start();
				this.pollers.set(config.name, poller);
			}
		}

		return result;
	}

	/**
	 * Unregister a connector by name.
	 *
	 * Stops poller, closes adapter, and unregisters from gateway.
	 */
	async unregister(name: string): Promise<HandlerResult> {
		const result = await handleUnregisterConnector(name, this.configStore);
		if (result.status !== 200) {
			return result;
		}

		// Stop poller if running
		const poller = this.pollers.get(name);
		if (poller) {
			poller.stop();
			this.pollers.delete(name);
		}

		// Close adapter
		const adapter = this.adapters.get(name);
		if (adapter) {
			await adapter.close();
			this.adapters.delete(name);
		}

		// Unregister from gateway
		this.gateway.unregisterSource(name);
		this.gateway.unregisterActionHandler(name);

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
			isPolling: this.pollers.get(c.name)?.isRunning ?? false,
		}));

		return { status: 200, body: augmented };
	}

	/** Stop all pollers and close all adapters. */
	async stopAll(): Promise<void> {
		for (const [, poller] of this.pollers) {
			poller.stop();
		}
		this.pollers.clear();

		for (const [, adapter] of this.adapters) {
			await adapter.close();
		}
		this.adapters.clear();
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
