import { Ok } from "@lakesync/core";
import { SyncGateway } from "@lakesync/gateway";
import { describe, expect, it, vi } from "vitest";
import type { JiraClient } from "../client";
import { mapComment, mapIssue, mapProject } from "../mapping";
import { JiraSourcePoller } from "../poller";
import { JIRA_TABLE_SCHEMAS } from "../schemas";
import type { JiraComment, JiraIssue, JiraProject } from "../types";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

function makeIssue(key: string, updated: string): JiraIssue {
	return {
		id: key.replace("-", ""),
		key,
		fields: {
			summary: `Issue ${key}`,
			description: null,
			status: { name: "Open" },
			priority: { name: "Medium" },
			issuetype: { name: "Task" },
			assignee: null,
			reporter: null,
			labels: [],
			project: { key: key.split("-")[0]!, name: "Test" },
			created: "2025-01-01T00:00:00.000+0000",
			updated,
		},
	};
}

function makeComment(id: string): JiraComment {
	return {
		id,
		body: { type: "doc", content: [] },
		author: { displayName: "Alice", emailAddress: "alice@co.com" },
		created: "2025-01-15T00:00:00.000+0000",
		updated: "2025-01-15T00:00:00.000+0000",
	};
}

function makeProject(key: string, name: string): JiraProject {
	return {
		id: `proj-${key}`,
		key,
		name,
		projectTypeKey: "software",
		lead: { displayName: "Charlie" },
	};
}

function createMockClient(overrides: {
	searchIssues?: JiraIssue[];
	getComments?: Map<string, JiraComment[]>;
	getProjects?: JiraProject[];
}): JiraClient {
	const client = {
		searchIssues: vi.fn().mockResolvedValue(Ok(overrides.searchIssues ?? [])),
		getComments: vi.fn().mockImplementation((issueKey: string) => {
			const comments = overrides.getComments?.get(issueKey) ?? [];
			return Promise.resolve(Ok(comments));
		}),
		getProjects: vi.fn().mockResolvedValue(Ok(overrides.getProjects ?? [])),
	};
	return client as unknown as JiraClient;
}

// ---------------------------------------------------------------------------
// Cursor state serialisation
// ---------------------------------------------------------------------------

describe("JiraSourcePoller cursor state", () => {
	it("round-trips cursor state through getCursorState/setCursorState", async () => {
		const comments = new Map<string, JiraComment[]>();
		comments.set("ENG-1", [makeComment("c1"), makeComment("c2")]);

		const client = createMockClient({
			searchIssues: [makeIssue("ENG-1", "2025-06-01T12:00:00.000+0000")],
			getComments: comments,
			getProjects: [makeProject("ENG", "Engineering")],
		});

		const gateway = new SyncGateway({
			gatewayId: "test-gw",
			maxBufferBytes: 4 * 1024 * 1024,
			maxBufferAgeMs: 30_000,
		});

		const poller = new JiraSourcePoller(
			{ domain: "co", email: "a@b.com", apiToken: "tok" },
			undefined,
			"jira-test",
			gateway,
			client,
		);

		// Poll to populate internal state
		await poller.poll();

		// Export state
		const exported = poller.getCursorState();

		// Verify shape
		expect(exported.lastUpdated).toBe("2025-06-01T12:00:00.000+0000");
		expect(Array.isArray(exported.commentSnapshot)).toBe(true);
		expect(Array.isArray(exported.projectSnapshot)).toBe(true);

		// Create a fresh poller and restore state
		const poller2 = new JiraSourcePoller(
			{ domain: "co", email: "a@b.com", apiToken: "tok" },
			undefined,
			"jira-test-2",
			gateway,
			client,
		);

		poller2.setCursorState(exported);

		// Export from restored poller — should match original
		const reExported = poller2.getCursorState();
		expect(reExported).toEqual(exported);
	});

	it("handles empty cursor state", () => {
		const gateway = new SyncGateway({
			gatewayId: "test-gw",
			maxBufferBytes: 4 * 1024 * 1024,
			maxBufferAgeMs: 30_000,
		});

		const client = createMockClient({});
		const poller = new JiraSourcePoller(
			{ domain: "co", email: "a@b.com", apiToken: "tok" },
			undefined,
			"jira-test",
			gateway,
			client,
		);

		const state = poller.getCursorState();
		expect(state.lastUpdated).toBeUndefined();
		expect(state.commentSnapshot).toEqual([]);
		expect(state.projectSnapshot).toEqual([]);
	});

	it("cursor state is JSON-serialisable", async () => {
		const comments = new Map<string, JiraComment[]>();
		comments.set("ENG-1", [makeComment("c1")]);

		const client = createMockClient({
			searchIssues: [makeIssue("ENG-1", "2025-06-01T12:00:00.000+0000")],
			getComments: comments,
			getProjects: [makeProject("ENG", "Engineering")],
		});

		const gateway = new SyncGateway({
			gatewayId: "test-gw",
			maxBufferBytes: 4 * 1024 * 1024,
			maxBufferAgeMs: 30_000,
		});

		const poller = new JiraSourcePoller(
			{ domain: "co", email: "a@b.com", apiToken: "tok" },
			undefined,
			"jira-test",
			gateway,
			client,
		);

		await poller.poll();

		const state = poller.getCursorState();
		const json = JSON.stringify(state);
		const restored = JSON.parse(json) as Record<string, unknown>;

		// Restore from deserialised JSON and re-export
		const poller2 = new JiraSourcePoller(
			{ domain: "co", email: "a@b.com", apiToken: "tok" },
			undefined,
			"jira-test-2",
			gateway,
			client,
		);
		poller2.setCursorState(restored);
		expect(poller2.getCursorState()).toEqual(state);
	});
});

// ---------------------------------------------------------------------------
// Table schema completeness
// ---------------------------------------------------------------------------

describe("JIRA_TABLE_SCHEMAS", () => {
	it("contains schemas for all three entity types", () => {
		const tableNames = JIRA_TABLE_SCHEMAS.map((s) => s.table);
		expect(tableNames).toContain("jira_issues");
		expect(tableNames).toContain("jira_comments");
		expect(tableNames).toContain("jira_projects");
		expect(JIRA_TABLE_SCHEMAS).toHaveLength(3);
	});

	it("jira_issues schema covers all columns from mapIssue", () => {
		const schema = JIRA_TABLE_SCHEMAS.find((s) => s.table === "jira_issues")!;
		const schemaColumns = new Set(schema.columns.map((c) => c.name));

		const issue = makeIssue("ENG-1", "2025-01-01T00:00:00.000+0000");
		const { row } = mapIssue(issue);
		const mappingColumns = Object.keys(row);

		for (const col of mappingColumns) {
			expect(schemaColumns.has(col)).toBe(true);
		}
		expect(schema.columns).toHaveLength(mappingColumns.length);
	});

	it("jira_comments schema covers all columns from mapComment", () => {
		const schema = JIRA_TABLE_SCHEMAS.find((s) => s.table === "jira_comments")!;
		const schemaColumns = new Set(schema.columns.map((c) => c.name));

		const comment = makeComment("c1");
		const { row } = mapComment("ENG-1", comment);
		const mappingColumns = Object.keys(row);

		for (const col of mappingColumns) {
			expect(schemaColumns.has(col)).toBe(true);
		}
		expect(schema.columns).toHaveLength(mappingColumns.length);
	});

	it("jira_projects schema covers all columns from mapProject", () => {
		const schema = JIRA_TABLE_SCHEMAS.find((s) => s.table === "jira_projects")!;
		const schemaColumns = new Set(schema.columns.map((c) => c.name));

		const project = makeProject("ENG", "Engineering");
		const { row } = mapProject(project);
		const mappingColumns = Object.keys(row);

		for (const col of mappingColumns) {
			expect(schemaColumns.has(col)).toBe(true);
		}
		expect(schema.columns).toHaveLength(mappingColumns.length);
	});
});
