import {
	type BaseSourcePoller,
	type ConnectorConfig,
	type JiraConnectorConfigFull,
	type PushTarget,
	registerOutputSchemas,
} from "@lakesync/core";
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

/**
 * Poller factory for Jira connectors.
 *
 * Register with a {@link import("@lakesync/core").PollerRegistry} via `.with("jira", jiraPollerFactory)`.
 */
export function jiraPollerFactory(config: ConnectorConfig, gateway: PushTarget): BaseSourcePoller {
	if (config.type !== "jira") {
		throw new Error(`Expected connector type "jira", got "${config.type}"`);
	}
	const typed = config as JiraConnectorConfigFull;
	const ingest: JiraIngestConfig | undefined = typed.ingest
		? {
				intervalMs: typed.ingest.intervalMs,
				chunkSize: typed.ingest.chunkSize,
				memoryBudgetBytes: typed.ingest.memoryBudgetBytes,
			}
		: undefined;
	return new JiraSourcePoller(typed.jira, ingest, typed.name, gateway);
}

/** @deprecated Use {@link jiraPollerFactory} instead. */
export const createJiraPoller = jiraPollerFactory;

// Auto-register output schemas so listConnectorDescriptors() includes table info.
registerOutputSchemas("jira", JIRA_TABLE_SCHEMAS);
