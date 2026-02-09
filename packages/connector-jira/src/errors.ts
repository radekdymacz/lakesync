import { LakeSyncError } from "@lakesync/core";

/** HTTP error from the Jira REST API. */
export class JiraApiError extends LakeSyncError {
	/** HTTP status code returned by Jira. */
	readonly statusCode: number;
	/** Raw response body from Jira. */
	readonly responseBody: string;

	constructor(statusCode: number, responseBody: string, cause?: Error) {
		super(`Jira API error (${statusCode}): ${responseBody}`, "JIRA_API_ERROR", cause);
		this.statusCode = statusCode;
		this.responseBody = responseBody;
	}
}

/** Rate limit error (HTTP 429) from the Jira REST API. */
export class JiraRateLimitError extends LakeSyncError {
	/** Milliseconds to wait before retrying. */
	readonly retryAfterMs: number;

	constructor(retryAfterMs: number, cause?: Error) {
		super(`Jira rate limited — retry after ${retryAfterMs}ms`, "JIRA_RATE_LIMITED", cause);
		this.retryAfterMs = retryAfterMs;
	}
}
