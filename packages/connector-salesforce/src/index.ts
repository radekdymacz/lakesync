export { SalesforceClient } from "./client";
export { SalesforceApiError, SalesforceAuthError } from "./errors";
export { mapAccount, mapContact, mapLead, mapOpportunity } from "./mapping";
export { SalesforceSourcePoller } from "./poller";
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
