// ---------------------------------------------------------------------------
// Jira Table Schemas — declares the shape of each table produced by the poller
// ---------------------------------------------------------------------------

import type { TableSchema } from "@lakesync/core";

/** Schema for the `jira_issues` table (columns from {@link mapIssue}). */
const JIRA_ISSUES_SCHEMA: TableSchema = {
	table: "jira_issues",
	columns: [
		{ name: "jira_id", type: "string" },
		{ name: "key", type: "string" },
		{ name: "summary", type: "string" },
		{ name: "description", type: "string" },
		{ name: "status", type: "string" },
		{ name: "priority", type: "string" },
		{ name: "issue_type", type: "string" },
		{ name: "assignee_name", type: "string" },
		{ name: "assignee_email", type: "string" },
		{ name: "reporter_name", type: "string" },
		{ name: "reporter_email", type: "string" },
		{ name: "labels", type: "string" },
		{ name: "project_key", type: "string" },
		{ name: "project_name", type: "string" },
		{ name: "created", type: "string" },
		{ name: "updated", type: "string" },
	],
};

/** Schema for the `jira_comments` table (columns from {@link mapComment}). */
const JIRA_COMMENTS_SCHEMA: TableSchema = {
	table: "jira_comments",
	columns: [
		{ name: "jira_id", type: "string" },
		{ name: "issue_key", type: "string" },
		{ name: "body", type: "string" },
		{ name: "author_name", type: "string" },
		{ name: "author_email", type: "string" },
		{ name: "created", type: "string" },
		{ name: "updated", type: "string" },
	],
};

/** Schema for the `jira_projects` table (columns from {@link mapProject}). */
const JIRA_PROJECTS_SCHEMA: TableSchema = {
	table: "jira_projects",
	columns: [
		{ name: "jira_id", type: "string" },
		{ name: "key", type: "string" },
		{ name: "name", type: "string" },
		{ name: "project_type", type: "string" },
		{ name: "lead_name", type: "string" },
	],
};

/** All table schemas produced by the Jira connector. */
export const JIRA_TABLE_SCHEMAS: ReadonlyArray<TableSchema> = [
	JIRA_ISSUES_SCHEMA,
	JIRA_COMMENTS_SCHEMA,
	JIRA_PROJECTS_SCHEMA,
];
