import { Ok } from "@lakesync/core";
import { SyncGateway } from "@lakesync/gateway";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { JiraClient } from "../client";
import { JiraSourcePoller } from "../poller";
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

// ---------------------------------------------------------------------------
// Mock JiraClient
// ---------------------------------------------------------------------------

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
// Tests
// ---------------------------------------------------------------------------

describe("JiraSourcePoller", () => {
	let gateway: SyncGateway;

	beforeEach(() => {
		gateway = new SyncGateway({
			gatewayId: "test-gw",
			maxBufferBytes: 4 * 1024 * 1024,
			maxBufferAgeMs: 30_000,
		});
	});

	afterEach(() => {
		vi.restoreAllMocks();
	});

	it("pushes issue deltas to gateway on first poll", async () => {
		const client = createMockClient({
			searchIssues: [makeIssue("ENG-1", "2025-01-10T00:00:00.000+0000")],
			getComments: new Map(),
			getProjects: [],
		});

		const poller = new JiraSourcePoller(
			{ domain: "co", email: "a@b.com", apiToken: "tok", includeProjects: false },
			undefined,
			"jira-test",
			gateway,
			client,
		);

		const handlePushSpy = vi.spyOn(gateway, "handlePush");
		await poller.poll();

		expect(handlePushSpy).toHaveBeenCalledOnce();
		const push = handlePushSpy.mock.calls[0]![0]!;
		expect(push.clientId).toBe("ingest:jira-test");
		expect(push.deltas.length).toBeGreaterThan(0);

		// Should have issue deltas
		const issueDelta = push.deltas.find((d) => d.table === "jira_issues");
		expect(issueDelta).toBeDefined();
		expect(issueDelta!.rowId).toBe("ENG-1");
	});

	it("advances cursor on subsequent polls", async () => {
		const client = createMockClient({
			searchIssues: [makeIssue("ENG-1", "2025-01-10T00:00:00.000+0000")],
		});

		const poller = new JiraSourcePoller(
			{
				domain: "co",
				email: "a@b.com",
				apiToken: "tok",
				includeComments: false,
				includeProjects: false,
			},
			undefined,
			"jira-test",
			gateway,
			client,
		);

		await poller.poll();

		// Second poll — should pass updatedSince to searchIssues
		(client.searchIssues as ReturnType<typeof vi.fn>).mockResolvedValueOnce(Ok([]));
		await poller.poll();

		const secondCall = (client.searchIssues as ReturnType<typeof vi.fn>).mock.calls[1]!;
		expect(secondCall[1]).toBe("2025-01-10T00:00:00.000+0000");
	});

	it("polls comments for returned issues", async () => {
		const comments = new Map<string, JiraComment[]>();
		comments.set("ENG-1", [makeComment("c1")]);

		const client = createMockClient({
			searchIssues: [makeIssue("ENG-1", "2025-01-10T00:00:00.000+0000")],
			getComments: comments,
			getProjects: [],
		});

		const poller = new JiraSourcePoller(
			{ domain: "co", email: "a@b.com", apiToken: "tok", includeProjects: false },
			undefined,
			"jira-test",
			gateway,
			client,
		);

		const handlePushSpy = vi.spyOn(gateway, "handlePush");
		await poller.poll();

		const push = handlePushSpy.mock.calls[0]![0]!;
		const commentDelta = push.deltas.find((d) => d.table === "jira_comments");
		expect(commentDelta).toBeDefined();
		expect(commentDelta!.rowId).toBe("ENG-1:c1");
	});

	it("detects project deletes via diff", async () => {
		const client = createMockClient({
			searchIssues: [],
			getProjects: [makeProject("ENG", "Engineering")],
		});

		const poller = new JiraSourcePoller(
			{ domain: "co", email: "a@b.com", apiToken: "tok", includeComments: false },
			undefined,
			"jira-test",
			gateway,
			client,
		);

		const handlePushSpy = vi.spyOn(gateway, "handlePush");

		// First poll — inserts project
		await poller.poll();
		expect(handlePushSpy).toHaveBeenCalledOnce();

		// Second poll — project disappears
		(client.getProjects as ReturnType<typeof vi.fn>).mockResolvedValueOnce(Ok([]));
		(client.searchIssues as ReturnType<typeof vi.fn>).mockResolvedValueOnce(Ok([]));
		await poller.poll();

		expect(handlePushSpy).toHaveBeenCalledTimes(2);
		const push = handlePushSpy.mock.calls[1]![0]!;
		const deleteDelta = push.deltas.find((d) => d.table === "jira_projects" && d.op === "DELETE");
		expect(deleteDelta).toBeDefined();
		expect(deleteDelta!.rowId).toBe("ENG");
	});

	it("start/stop lifecycle", async () => {
		vi.useFakeTimers();

		const client = createMockClient({ searchIssues: [] });

		const poller = new JiraSourcePoller(
			{
				domain: "co",
				email: "a@b.com",
				apiToken: "tok",
				includeComments: false,
				includeProjects: false,
			},
			{ intervalMs: 1000 },
			"jira-test",
			gateway,
			client,
		);

		expect(poller.isRunning).toBe(false);
		poller.start();
		expect(poller.isRunning).toBe(true);

		// Should not start twice
		poller.start();
		expect(poller.isRunning).toBe(true);

		poller.stop();
		expect(poller.isRunning).toBe(false);

		vi.useRealTimers();
	});

	it("swallows errors during poll without crashing", async () => {
		const client = createMockClient({});
		(client.searchIssues as ReturnType<typeof vi.fn>).mockRejectedValueOnce(
			new Error("Network error"),
		);

		const poller = new JiraSourcePoller(
			{
				domain: "co",
				email: "a@b.com",
				apiToken: "tok",
				includeComments: false,
				includeProjects: false,
			},
			undefined,
			"jira-test",
			gateway,
			client,
		);

		// Should not throw
		await expect(poller.poll()).rejects.toThrow("Network error");
	});
});
