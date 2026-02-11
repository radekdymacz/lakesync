// ---------------------------------------------------------------------------
// Connector Manager — registration, poller lifecycle, adapter creation
// ---------------------------------------------------------------------------

import { createDatabaseAdapter, createQueryFn, type DatabaseAdapter } from "@lakesync/adapter";
import { isActionHandler } from "@lakesync/core";
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
 * Encapsulates the if/else chains for different connector types and
 * handles clean-up on unregistration.
 */
export class ConnectorManager {
	private readonly adapters = new Map<string, DatabaseAdapter>();
	private readonly pollers = new Map<string, Poller>();

	constructor(
		private readonly configStore: ConfigStore,
		private readonly gateway: SyncGateway,
	) {}

	/**
	 * Register a connector from raw JSON body.
	 *
	 * Validates via shared handler, creates adapter/poller as appropriate,
	 * registers with the gateway.
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

		// Jira connectors use their own API-based poller
		if (config.type === "jira") {
			try {
				const { JiraSourcePoller } = await import("@lakesync/connector-jira");
				const ingestConfig = config.ingest ? { intervalMs: config.ingest.intervalMs } : undefined;
				const poller = new JiraSourcePoller(config.jira, ingestConfig, config.name, this.gateway);
				poller.start();
				this.pollers.set(config.name, poller);
				return result;
			} catch (err) {
				await this.rollbackRegistration(connectors, registeredName);
				const message = err instanceof Error ? err.message : String(err);
				return { status: 500, body: { error: `Failed to load Jira connector: ${message}` } };
			}
		}

		// Salesforce connectors use their own API-based poller
		if (config.type === "salesforce") {
			try {
				const { SalesforceSourcePoller } = await import("@lakesync/connector-salesforce");
				const ingestConfig = config.ingest ? { intervalMs: config.ingest.intervalMs } : undefined;
				const poller = new SalesforceSourcePoller(
					config.salesforce,
					ingestConfig,
					config.name,
					this.gateway,
				);
				poller.start();
				this.pollers.set(config.name, poller);
				return result;
			} catch (err) {
				await this.rollbackRegistration(connectors, registeredName);
				const message = err instanceof Error ? err.message : String(err);
				return {
					status: 500,
					body: { error: `Failed to load Salesforce connector: ${message}` },
				};
			}
		}

		// Database-based connectors (postgres, mysql, bigquery)
		const adapterResult = createDatabaseAdapter(config);
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
		connectors: Record<string, import("@lakesync/core").ConnectorConfig>,
		name: string,
	): Promise<void> {
		delete connectors[name];
		await this.configStore.setConnectors(connectors);
	}
}
