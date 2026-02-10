// ---------------------------------------------------------------------------
// Jira Connector — Type Definitions
// ---------------------------------------------------------------------------

/** Connection configuration for a Jira Cloud source. */
export interface JiraConnectorConfig {
	/** Jira Cloud domain (e.g. "mycompany" for mycompany.atlassian.net). */
	domain: string;
	/** Email address for Basic auth. */
	email: string;
	/** API token paired with the email. */
	apiToken: string;
	/** Optional JQL filter to scope issue polling. */
	jql?: string;
	/** Whether to include comments (default true). */
	includeComments?: boolean;
	/** Whether to include projects (default true). */
	includeProjects?: boolean;
}

/** Ingest configuration for the Jira poller. */
export interface JiraIngestConfig {
	/** Poll interval in milliseconds (default 30 000). */
	intervalMs?: number;
	/** Number of deltas per push chunk (default 500). */
	chunkSize?: number;
	/** Approximate memory budget in bytes — triggers flush at 70%. */
	memoryBudgetBytes?: number;
}

// ---------------------------------------------------------------------------
// Jira REST API v3 — Minimal Response Types
// ---------------------------------------------------------------------------

/** Issue search response from POST /rest/api/3/search/jql. */
export interface JiraSearchResponse {
	issues: JiraIssue[];
	/** Opaque token for fetching the next page (absent on last page). */
	nextPageToken?: string;
	/** True when this is the final page. */
	isLast?: boolean;
}

/** A single Jira issue. */
export interface JiraIssue {
	id: string;
	key: string;
	fields: {
		summary: string | null;
		description: unknown | null;
		status: { name: string } | null;
		priority: { name: string } | null;
		issuetype: { name: string } | null;
		assignee: { displayName: string; emailAddress: string } | null;
		reporter: { displayName: string; emailAddress: string } | null;
		labels: string[] | null;
		project: { key: string; name: string } | null;
		created: string | null;
		updated: string | null;
	};
}

/** Paginated comment response from GET /rest/api/3/issue/{key}/comment. */
export interface JiraCommentPage {
	startAt: number;
	maxResults: number;
	total: number;
	comments: JiraComment[];
}

/** A single Jira comment. */
export interface JiraComment {
	id: string;
	body: unknown | null;
	author: { displayName: string; emailAddress: string } | null;
	created: string | null;
	updated: string | null;
}

/** Paginated project response from GET /rest/api/3/project/search. */
export interface JiraProjectPage {
	startAt: number;
	maxResults: number;
	total: number;
	values: JiraProject[];
}

/** A single Jira project. */
export interface JiraProject {
	id: string;
	key: string;
	name: string;
	projectTypeKey: string | null;
	lead: { displayName: string } | null;
}
