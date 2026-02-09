import { describe, expect, it } from "vitest";
import { mapComment, mapIssue, mapProject } from "../mapping";
import type { JiraComment, JiraIssue, JiraProject } from "../types";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const fullIssue: JiraIssue = {
	id: "10001",
	key: "ENG-42",
	fields: {
		summary: "Fix login bug",
		description: { type: "doc", content: [{ type: "paragraph" }] },
		status: { name: "In Progress" },
		priority: { name: "High" },
		issuetype: { name: "Bug" },
		assignee: { displayName: "Alice", emailAddress: "alice@co.com" },
		reporter: { displayName: "Bob", emailAddress: "bob@co.com" },
		labels: ["backend", "urgent"],
		project: { key: "ENG", name: "Engineering" },
		created: "2025-01-15T10:00:00.000+0000",
		updated: "2025-01-16T12:00:00.000+0000",
	},
};

const nullFieldsIssue: JiraIssue = {
	id: "10002",
	key: "ENG-43",
	fields: {
		summary: null,
		description: null,
		status: null,
		priority: null,
		issuetype: null,
		assignee: null,
		reporter: null,
		labels: null,
		project: null,
		created: null,
		updated: null,
	},
};

const fullComment: JiraComment = {
	id: "20001",
	body: { type: "doc", content: [{ type: "paragraph" }] },
	author: { displayName: "Alice", emailAddress: "alice@co.com" },
	created: "2025-01-15T11:00:00.000+0000",
	updated: "2025-01-15T11:30:00.000+0000",
};

const nullComment: JiraComment = {
	id: "20002",
	body: null,
	author: null,
	created: null,
	updated: null,
};

const fullProject: JiraProject = {
	id: "30001",
	key: "ENG",
	name: "Engineering",
	projectTypeKey: "software",
	lead: { displayName: "Charlie" },
};

const nullProject: JiraProject = {
	id: "30002",
	key: "OPS",
	name: "Operations",
	projectTypeKey: null,
	lead: null,
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("mapIssue", () => {
	it("maps a fully populated issue", () => {
		const { rowId, row } = mapIssue(fullIssue);

		expect(rowId).toBe("ENG-42");
		expect(row.jira_id).toBe("10001");
		expect(row.key).toBe("ENG-42");
		expect(row.summary).toBe("Fix login bug");
		expect(row.description).toBe(JSON.stringify(fullIssue.fields.description));
		expect(row.status).toBe("In Progress");
		expect(row.priority).toBe("High");
		expect(row.issue_type).toBe("Bug");
		expect(row.assignee_name).toBe("Alice");
		expect(row.assignee_email).toBe("alice@co.com");
		expect(row.reporter_name).toBe("Bob");
		expect(row.reporter_email).toBe("bob@co.com");
		expect(row.labels).toBe(JSON.stringify(["backend", "urgent"]));
		expect(row.project_key).toBe("ENG");
		expect(row.project_name).toBe("Engineering");
		expect(row.created).toBe("2025-01-15T10:00:00.000+0000");
		expect(row.updated).toBe("2025-01-16T12:00:00.000+0000");
	});

	it("maps null fields gracefully", () => {
		const { rowId, row } = mapIssue(nullFieldsIssue);

		expect(rowId).toBe("ENG-43");
		expect(row.summary).toBeNull();
		expect(row.description).toBeNull();
		expect(row.status).toBeNull();
		expect(row.priority).toBeNull();
		expect(row.issue_type).toBeNull();
		expect(row.assignee_name).toBeNull();
		expect(row.assignee_email).toBeNull();
		expect(row.reporter_name).toBeNull();
		expect(row.reporter_email).toBeNull();
		expect(row.labels).toBeNull();
		expect(row.project_key).toBeNull();
		expect(row.project_name).toBeNull();
		expect(row.created).toBeNull();
		expect(row.updated).toBeNull();
	});
});

describe("mapComment", () => {
	it("maps a fully populated comment", () => {
		const { rowId, row } = mapComment("ENG-42", fullComment);

		expect(rowId).toBe("ENG-42:20001");
		expect(row.jira_id).toBe("20001");
		expect(row.issue_key).toBe("ENG-42");
		expect(row.body).toBe(JSON.stringify(fullComment.body));
		expect(row.author_name).toBe("Alice");
		expect(row.author_email).toBe("alice@co.com");
		expect(row.created).toBe("2025-01-15T11:00:00.000+0000");
		expect(row.updated).toBe("2025-01-15T11:30:00.000+0000");
	});

	it("maps null fields gracefully", () => {
		const { rowId, row } = mapComment("ENG-42", nullComment);

		expect(rowId).toBe("ENG-42:20002");
		expect(row.body).toBeNull();
		expect(row.author_name).toBeNull();
		expect(row.author_email).toBeNull();
		expect(row.created).toBeNull();
		expect(row.updated).toBeNull();
	});
});

describe("mapProject", () => {
	it("maps a fully populated project", () => {
		const { rowId, row } = mapProject(fullProject);

		expect(rowId).toBe("ENG");
		expect(row.jira_id).toBe("30001");
		expect(row.key).toBe("ENG");
		expect(row.name).toBe("Engineering");
		expect(row.project_type).toBe("software");
		expect(row.lead_name).toBe("Charlie");
	});

	it("maps null fields gracefully", () => {
		const { rowId, row } = mapProject(nullProject);

		expect(rowId).toBe("OPS");
		expect(row.project_type).toBeNull();
		expect(row.lead_name).toBeNull();
	});
});
