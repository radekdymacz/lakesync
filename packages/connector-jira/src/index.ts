import { registerOutputSchemas, registerPollerFactory } from "@lakesync/core";
import { JiraSourcePoller } from "./poller";
import { JIRA_TABLE_SCHEMAS } from "./schemas";
import type { JiraIngestConfig } from "./types";

export { JiraClient } from "./client";
export { JiraApiError, JiraRateLimitError } from "./errors";
export { mapComment, mapIssue, mapProject } from "./mapping";
export { JiraSourcePoller } from "./poller";
export { JIRA_TABLE_SCHEMAS } from "./schemas";
export { testConnection } from "./test-connection";
export type {
	JiraComment,
	JiraCommentPage,
	JiraConnectorConfig,
	JiraIngestConfig,
	JiraIssue,
	JiraProject,
	JiraProjectPage,
	JiraSearchResponse,
} from "./types";

// Auto-register output schemas so listConnectorDescriptors() includes table info.
registerOutputSchemas("jira", JIRA_TABLE_SCHEMAS);

// Auto-register poller factory so createPoller("jira", ...) works.
registerPollerFactory("jira", (config, gateway) => {
	if (config.type !== "jira") {
		throw new Error(`Expected connector type "jira", got "${config.type}"`);
	}
	const ingest: JiraIngestConfig | undefined = config.ingest
		? {
				intervalMs: config.ingest.intervalMs,
				chunkSize: config.ingest.chunkSize,
				memoryBudgetBytes: config.ingest.memoryBudgetBytes,
			}
		: undefined;
	return new JiraSourcePoller(config.jira, ingest, config.name, gateway);
});
