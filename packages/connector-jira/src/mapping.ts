// ---------------------------------------------------------------------------
// Jira Entity → Flat LakeSync Row Mapping
// ---------------------------------------------------------------------------

import type { JiraComment, JiraIssue, JiraProject } from "./types";

/**
 * Map a Jira issue to a flat row for the `jira_issues` table.
 *
 * The row ID is the issue key (e.g. "PROJ-123").
 */
export function mapIssue(issue: JiraIssue): { rowId: string; row: Record<string, unknown> } {
	const f = issue.fields;
	return {
		rowId: issue.key,
		row: {
			jira_id: issue.id,
			key: issue.key,
			summary: f.summary ?? null,
			description: f.description != null ? JSON.stringify(f.description) : null,
			status: f.status?.name ?? null,
			priority: f.priority?.name ?? null,
			issue_type: f.issuetype?.name ?? null,
			assignee_name: f.assignee?.displayName ?? null,
			assignee_email: f.assignee?.emailAddress ?? null,
			reporter_name: f.reporter?.displayName ?? null,
			reporter_email: f.reporter?.emailAddress ?? null,
			labels: f.labels != null ? JSON.stringify(f.labels) : null,
			project_key: f.project?.key ?? null,
			project_name: f.project?.name ?? null,
			created: f.created ?? null,
			updated: f.updated ?? null,
		},
	};
}

/**
 * Map a Jira comment to a flat row for the `jira_comments` table.
 *
 * The row ID is "{issueKey}:{commentId}".
 */
export function mapComment(
	issueKey: string,
	comment: JiraComment,
): { rowId: string; row: Record<string, unknown> } {
	return {
		rowId: `${issueKey}:${comment.id}`,
		row: {
			jira_id: comment.id,
			issue_key: issueKey,
			body: comment.body != null ? JSON.stringify(comment.body) : null,
			author_name: comment.author?.displayName ?? null,
			author_email: comment.author?.emailAddress ?? null,
			created: comment.created ?? null,
			updated: comment.updated ?? null,
		},
	};
}

/**
 * Map a Jira project to a flat row for the `jira_projects` table.
 *
 * The row ID is the project key (e.g. "PROJ").
 */
export function mapProject(project: JiraProject): { rowId: string; row: Record<string, unknown> } {
	return {
		rowId: project.key,
		row: {
			jira_id: project.id,
			key: project.key,
			name: project.name,
			project_type: project.projectTypeKey ?? null,
			lead_name: project.lead?.displayName ?? null,
		},
	};
}
