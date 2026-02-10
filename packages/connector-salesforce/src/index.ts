import { registerPollerFactory } from "@lakesync/core";
import { SalesforceSourcePoller } from "./poller";

export { SalesforceClient } from "./client";
export { SalesforceApiError, SalesforceAuthError } from "./errors";
export { mapAccount, mapContact, mapLead, mapOpportunity } from "./mapping";
export { SalesforceSourcePoller } from "./poller";
export { SALESFORCE_TABLE_SCHEMAS } from "./schemas";
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

// Auto-register poller factory so createPoller("salesforce", ...) works.
registerPollerFactory("salesforce", (config, gateway) => {
	return new SalesforceSourcePoller(config.salesforce!, undefined, config.name, gateway);
});
