import { Ok, type Result } from "@lakesync/core";
import { JiraClient } from "./client";
import type { JiraApiError, JiraRateLimitError } from "./errors";
import type { JiraConnectorConfig } from "./types";

/**
 * Test a Jira Cloud connection by authenticating and fetching the current user.
 *
 * Creates a `JiraClient` internally and calls `GET /rest/api/3/myself` —
 * the cheapest endpoint that validates credentials.
 */
export async function testConnection(
	config: JiraConnectorConfig,
): Promise<Result<void, JiraApiError | JiraRateLimitError>> {
	const client = new JiraClient(config);
	const result = await client.getCurrentUser();
	if (!result.ok) return result;
	return Ok(undefined);
}
