import type { ConnectorConfig, SyncRulesConfig, TableSchema } from "@lakesync/core";

/**
 * Platform-agnostic configuration storage interface.
 *
 * Implemented by MemoryConfigStore (tests, gateway-server) and
 * DurableStorageConfigStore (gateway-worker).
 */
export interface ConfigStore {
	getSchema(gatewayId: string): Promise<TableSchema | undefined>;
	setSchema(gatewayId: string, schema: TableSchema): Promise<void>;
	getSyncRules(gatewayId: string): Promise<SyncRulesConfig | undefined>;
	setSyncRules(gatewayId: string, rules: SyncRulesConfig): Promise<void>;
	getConnectors(): Promise<Record<string, ConnectorConfig>>;
	setConnectors(connectors: Record<string, ConnectorConfig>): Promise<void>;
}

/**
 * In-memory implementation of ConfigStore.
 * Used by tests and gateway-server.
 */
export class MemoryConfigStore implements ConfigStore {
	private schemas = new Map<string, TableSchema>();
	private syncRules = new Map<string, SyncRulesConfig>();
	private connectors: Record<string, ConnectorConfig> = {};

	async getSchema(gatewayId: string): Promise<TableSchema | undefined> {
		return this.schemas.get(gatewayId);
	}

	async setSchema(gatewayId: string, schema: TableSchema): Promise<void> {
		this.schemas.set(gatewayId, schema);
	}

	async getSyncRules(gatewayId: string): Promise<SyncRulesConfig | undefined> {
		return this.syncRules.get(gatewayId);
	}

	async setSyncRules(gatewayId: string, rules: SyncRulesConfig): Promise<void> {
		this.syncRules.set(gatewayId, rules);
	}

	async getConnectors(): Promise<Record<string, ConnectorConfig>> {
		return { ...this.connectors };
	}

	async setConnectors(connectors: Record<string, ConnectorConfig>): Promise<void> {
		this.connectors = { ...connectors };
	}
}
