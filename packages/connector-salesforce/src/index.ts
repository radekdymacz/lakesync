import {
	type BaseSourcePoller,
	type ConnectorConfig,
	type PushTarget,
	registerOutputSchemas,
	registerPollerFactory,
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
 * Use with an explicit {@link import("@lakesync/core").PollerRegistry} instead
 * of the deprecated global {@link registerPollerFactory}.
 */
export function createSalesforcePoller(
	config: ConnectorConfig,
	gateway: PushTarget,
): BaseSourcePoller {
	if (config.type !== "salesforce") {
		throw new Error(`Expected connector type "salesforce", got "${config.type}"`);
	}
	const ingest: SalesforceIngestConfig | undefined = config.ingest
		? {
				intervalMs: config.ingest.intervalMs,
				chunkSize: config.ingest.chunkSize,
				memoryBudgetBytes: config.ingest.memoryBudgetBytes,
			}
		: undefined;
	return new SalesforceSourcePoller(config.salesforce, ingest, config.name, gateway);
}

// Auto-register output schemas so listConnectorDescriptors() includes table info.
registerOutputSchemas("salesforce", SALESFORCE_TABLE_SCHEMAS);

/** @deprecated Global auto-registration — prefer explicit {@link createSalesforcePoller} with a PollerRegistry. */
registerPollerFactory("salesforce", createSalesforcePoller);
