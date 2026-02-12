import {
	type BaseSourcePoller,
	type ConnectorConfig,
	type PushTarget,
	registerOutputSchemas,
	type SalesforceConnectorConfigFull,
} from "@lakesync/core";
import { SalesforceSourcePoller } from "./poller";
import { SALESFORCE_TABLE_SCHEMAS } from "./schemas";
import type { SalesforceIngestConfig } from "./types";

export { SalesforceClient } from "./client";
export { SalesforceApiError, SalesforceAuthError } from "./errors";
export { mapAccount, mapContact, mapLead, mapOpportunity } from "./mapping";
export { SalesforceSourcePoller } from "./poller";
export { SALESFORCE_TABLE_SCHEMAS } from "./schemas";
export { testConnection } from "./test-connection";
export type {
	SalesforceAuthResponse,
	SalesforceConnectorConfig,
	SalesforceIngestConfig,
	SalesforceQueryResponse,
	SfAccount,
	SfContact,
	SfLead,
	SfOpportunity,
} from "./types";

/**
 * Poller factory for Salesforce connectors.
 *
 * Register with a {@link import("@lakesync/core").PollerRegistry} via `.with("salesforce", salesforcePollerFactory)`.
 */
export function salesforcePollerFactory(
	config: ConnectorConfig,
	gateway: PushTarget,
): BaseSourcePoller {
	if (config.type !== "salesforce") {
		throw new Error(`Expected connector type "salesforce", got "${config.type}"`);
	}
	const sfConfig = config as SalesforceConnectorConfigFull;
	const ingest: SalesforceIngestConfig | undefined = sfConfig.ingest
		? {
				intervalMs: sfConfig.ingest.intervalMs,
				chunkSize: sfConfig.ingest.chunkSize,
				memoryBudgetBytes: sfConfig.ingest.memoryBudgetBytes,
			}
		: undefined;
	return new SalesforceSourcePoller(sfConfig.salesforce, ingest, sfConfig.name, gateway);
}

/** @deprecated Use {@link salesforcePollerFactory} instead. */
export const createSalesforcePoller = salesforcePollerFactory;

// Auto-register output schemas so listConnectorDescriptors() includes table info.
registerOutputSchemas("salesforce", SALESFORCE_TABLE_SCHEMAS);
