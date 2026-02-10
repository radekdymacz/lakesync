// ---------------------------------------------------------------------------
// JiraClient — HTTP wrapper for Jira Cloud REST API v3
// ---------------------------------------------------------------------------

import { Err, Ok, type Result } from "@lakesync/core";
import { JiraApiError, JiraRateLimitError } from "./errors";
import type {
	JiraComment,
	JiraCommentPage,
	JiraConnectorConfig,
	JiraIssue,
	JiraProject,
	JiraProjectPage,
	JiraSearchResponse,
} from "./types";

const MAX_RESULTS = 100;
const MAX_RATE_LIMIT_RETRIES = 3;
const DEFAULT_RETRY_AFTER_MS = 10_000;

/** Fields requested for issue search. */
const ISSUE_FIELDS = [
	"summary",
	"description",
	"status",
	"priority",
	"assignee",
	"reporter",
	"labels",
	"created",
	"updated",
	"project",
	"issuetype",
];

/**
 * HTTP client for the Jira Cloud REST API v3.
 *
 * Uses Basic authentication (email + API token) and global `fetch`.
 * All public methods return `Result<T, JiraApiError>`.
 */
export class JiraClient {
	private readonly baseUrl: string;
	private readonly authHeader: string;

	constructor(config: JiraConnectorConfig) {
		this.baseUrl = `https://${config.domain}.atlassian.net`;
		this.authHeader = `Basic ${btoa(`${config.email}:${config.apiToken}`)}`;
	}

	/**
	 * Search for issues via JQL with auto-pagination.
	 *
	 * Uses the `/rest/api/3/search/jql` endpoint with token-based pagination.
	 * When `updatedSince` is provided, appends `AND updated >= "date"` to the JQL.
	 * Empty JQL is replaced with `project is not EMPTY` (the new endpoint
	 * rejects unbounded queries).
	 *
	 * @param limit — optional cap on the total number of issues returned.
	 */
	async searchIssues(
		jql: string,
		updatedSince?: string,
		limit?: number,
	): Promise<Result<JiraIssue[], JiraApiError | JiraRateLimitError>> {
		let effectiveJql = jql || "";

		if (updatedSince) {
			const clause = `updated >= "${updatedSince}"`;
			effectiveJql = effectiveJql.length > 0 ? `(${effectiveJql}) AND ${clause}` : clause;
		}

		// The /search/jql endpoint rejects unbounded queries
		if (effectiveJql.length === 0) {
			effectiveJql = "project is not EMPTY";
		}

		const allIssues: JiraIssue[] = [];
		let nextPageToken: string | undefined;
		const pageSize = limit !== undefined ? Math.min(limit, MAX_RESULTS) : MAX_RESULTS;

		while (true) {
			const body: Record<string, unknown> = {
				jql: effectiveJql,
				maxResults: pageSize,
				fields: ISSUE_FIELDS,
			};
			if (nextPageToken) {
				body.nextPageToken = nextPageToken;
			}

			const result = await this.request<JiraSearchResponse>("/rest/api/3/search/jql", "POST", body);

			if (!result.ok) return result;

			const page = result.value;
			for (const issue of page.issues) {
				allIssues.push(issue);
			}

			if (limit !== undefined && allIssues.length >= limit) break;
			if (page.isLast || !page.nextPageToken) break;
			nextPageToken = page.nextPageToken;
		}

		return Ok(limit !== undefined ? allIssues.slice(0, limit) : allIssues);
	}

	/**
	 * Fetch all comments for a given issue key with auto-pagination.
	 */
	async getComments(
		issueKey: string,
	): Promise<Result<JiraComment[], JiraApiError | JiraRateLimitError>> {
		const allComments: JiraComment[] = [];
		let startAt = 0;

		while (true) {
			const result = await this.request<JiraCommentPage>(
				`/rest/api/3/issue/${encodeURIComponent(issueKey)}/comment?startAt=${startAt}&maxResults=${MAX_RESULTS}`,
				"GET",
			);

			if (!result.ok) return result;

			const page = result.value;
			for (const comment of page.comments) {
				allComments.push(comment);
			}

			startAt += page.maxResults;
			if (startAt >= page.total) break;
		}

		return Ok(allComments);
	}

	/**
	 * Fetch all projects with auto-pagination.
	 */
	async getProjects(): Promise<Result<JiraProject[], JiraApiError | JiraRateLimitError>> {
		const allProjects: JiraProject[] = [];
		let startAt = 0;

		while (true) {
			const result = await this.request<JiraProjectPage>(
				`/rest/api/3/project/search?startAt=${startAt}&maxResults=${MAX_RESULTS}`,
				"GET",
			);

			if (!result.ok) return result;

			const page = result.value;
			for (const project of page.values) {
				allProjects.push(project);
			}

			startAt += page.maxResults;
			if (startAt >= page.total) break;
		}

		return Ok(allProjects);
	}

	// -----------------------------------------------------------------------
	// Internal HTTP helpers
	// -----------------------------------------------------------------------

	/** Make an HTTP request with rate-limit retry logic. */
	private async request<T>(
		path: string,
		method: "GET" | "POST",
		body?: unknown,
	): Promise<Result<T, JiraApiError | JiraRateLimitError>> {
		let lastError: JiraApiError | undefined;

		for (let attempt = 0; attempt <= MAX_RATE_LIMIT_RETRIES; attempt++) {
			const headers: Record<string, string> = {
				Authorization: this.authHeader,
				Accept: "application/json",
			};

			const init: RequestInit = { method, headers };

			if (body !== undefined) {
				headers["Content-Type"] = "application/json";
				init.body = JSON.stringify(body);
			}

			const response = await fetch(`${this.baseUrl}${path}`, init);

			if (response.ok) {
				const data = (await response.json()) as T;
				return Ok(data);
			}

			if (response.status === 429) {
				const retryAfter = response.headers.get("Retry-After");
				const waitMs = retryAfter ? Number.parseInt(retryAfter, 10) * 1000 : DEFAULT_RETRY_AFTER_MS;

				if (attempt < MAX_RATE_LIMIT_RETRIES) {
					await sleep(waitMs);
					lastError = new JiraApiError(429, `Rate limited, retried after ${waitMs}ms`);
					continue;
				}

				return Err(new JiraRateLimitError(waitMs));
			}

			const responseBody = await response.text();
			return Err(new JiraApiError(response.status, responseBody));
		}

		return Err(lastError ?? new JiraApiError(0, "Unknown error after retries"));
	}
}

/** Sleep for the given number of milliseconds. */
function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}
