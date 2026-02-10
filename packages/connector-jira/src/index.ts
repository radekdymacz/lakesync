import { registerPollerFactory } from "@lakesync/core";
import { JiraSourcePoller } from "./poller";

export { JiraClient } from "./client";
export { JiraApiError, JiraRateLimitError } from "./errors";
export { mapComment, mapIssue, mapProject } from "./mapping";
export { JiraSourcePoller } from "./poller";
export { JIRA_TABLE_SCHEMAS } from "./schemas";
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

// Auto-register poller factory so createPoller("jira", ...) works.
registerPollerFactory("jira", (config, gateway) => {
	return new JiraSourcePoller(config.jira!, undefined, config.name, gateway);
});
