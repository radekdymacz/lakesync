import type { Result } from "@lakesync/core";
import { SalesforceClient } from "./client";
import type { SalesforceAuthError } from "./errors";
import type { SalesforceConnectorConfig } from "./types";

/**
 * Test a Salesforce connection by attempting OAuth authentication.
 *
 * Creates a `SalesforceClient` internally and calls `authenticate()` —
 * if the OAuth flow succeeds, the connection is valid.
 */
export async function testConnection(
	config: SalesforceConnectorConfig,
): Promise<Result<void, SalesforceAuthError>> {
	const client = new SalesforceClient(config);
	return client.authenticate();
}
