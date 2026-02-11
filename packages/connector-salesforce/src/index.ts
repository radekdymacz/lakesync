import { registerOutputSchemas, registerPollerFactory } from "@lakesync/core";
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

// Auto-register output schemas so listConnectorDescriptors() includes table info.
registerOutputSchemas("salesforce", SALESFORCE_TABLE_SCHEMAS);

// Auto-register poller factory so createPoller("salesforce", ...) works.
registerPollerFactory("salesforce", (config, gateway) => {
	const ingest: SalesforceIngestConfig | undefined = config.ingest
		? {
				intervalMs: config.ingest.intervalMs,
				chunkSize: config.ingest.chunkSize,
				memoryBudgetBytes: config.ingest.memoryBudgetBytes,
			}
		: undefined;
	return new SalesforceSourcePoller(config.salesforce!, ingest, config.name, gateway);
});
