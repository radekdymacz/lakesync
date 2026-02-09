import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { JiraClient } from "../client";
import type { JiraCommentPage, JiraProjectPage, JiraSearchResponse } from "../types";

// ---------------------------------------------------------------------------
// Mock fetch
// ---------------------------------------------------------------------------

const mockFetch = vi.fn<(...args: Parameters<typeof fetch>) => Promise<Response>>();

beforeEach(() => {
	vi.stubGlobal("fetch", mockFetch);
});

afterEach(() => {
	vi.restoreAllMocks();
});

const config = {
	domain: "mycompany",
	email: "bot@co.com",
	apiToken: "tok-123",
};

function jsonResponse(body: unknown, status = 200, headers?: Record<string, string>): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: { "Content-Type": "application/json", ...headers },
	});
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("JiraClient", () => {
	describe("auth header", () => {
		it("sends correct Basic auth header", async () => {
			const page: JiraSearchResponse = {
				startAt: 0,
				maxResults: 100,
				total: 0,
				issues: [],
			};
			mockFetch.mockResolvedValueOnce(jsonResponse(page));

			const client = new JiraClient(config);
			await client.searchIssues("");

			expect(mockFetch).toHaveBeenCalledOnce();
			const [, init] = mockFetch.mock.calls[0]!;
			const headers = (init as RequestInit).headers as Record<string, string>;
			const expected = `Basic ${btoa("bot@co.com:tok-123")}`;
			expect(headers.Authorization).toBe(expected);
		});
	});

	describe("searchIssues", () => {
		it("returns issues from a single page", async () => {
			const page: JiraSearchResponse = {
				startAt: 0,
				maxResults: 100,
				total: 1,
				issues: [
					{
						id: "1",
						key: "ENG-1",
						fields: {
							summary: "Test",
							description: null,
							status: { name: "Open" },
							priority: { name: "Medium" },
							issuetype: { name: "Task" },
							assignee: null,
							reporter: null,
							labels: [],
							project: { key: "ENG", name: "Engineering" },
							created: "2025-01-01",
							updated: "2025-01-02",
						},
					},
				],
			};
			mockFetch.mockResolvedValueOnce(jsonResponse(page));

			const client = new JiraClient(config);
			const result = await client.searchIssues("project = ENG");

			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(1);
				expect(result.value[0]!.key).toBe("ENG-1");
			}
		});

		it("paginates across multiple pages", async () => {
			const page1: JiraSearchResponse = {
				startAt: 0,
				maxResults: 1,
				total: 2,
				issues: [
					{
						id: "1",
						key: "ENG-1",
						fields: {
							summary: "First",
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
					},
				],
			};
			const page2: JiraSearchResponse = {
				startAt: 1,
				maxResults: 1,
				total: 2,
				issues: [
					{
						id: "2",
						key: "ENG-2",
						fields: {
							summary: "Second",
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
					},
				],
			};
			mockFetch.mockResolvedValueOnce(jsonResponse(page1));
			mockFetch.mockResolvedValueOnce(jsonResponse(page2));

			const client = new JiraClient(config);
			const result = await client.searchIssues("");

			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(2);
				expect(result.value[0]!.key).toBe("ENG-1");
				expect(result.value[1]!.key).toBe("ENG-2");
			}
		});

		it("appends updatedSince to JQL", async () => {
			const page: JiraSearchResponse = {
				startAt: 0,
				maxResults: 100,
				total: 0,
				issues: [],
			};
			mockFetch.mockResolvedValueOnce(jsonResponse(page));

			const client = new JiraClient(config);
			await client.searchIssues("project = ENG", "2025-01-15T00:00:00.000+0000");

			const [, init] = mockFetch.mock.calls[0]!;
			const body = JSON.parse((init as RequestInit).body as string);
			expect(body.jql).toContain("updated >=");
			expect(body.jql).toContain("project = ENG");
		});

		it("returns error on HTTP failure", async () => {
			mockFetch.mockResolvedValueOnce(new Response("Forbidden", { status: 403 }));

			const client = new JiraClient(config);
			const result = await client.searchIssues("");

			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("JIRA_API_ERROR");
			}
		});
	});

	describe("rate limiting", () => {
		it("retries on 429 and succeeds", async () => {
			const rateLimited = new Response("", {
				status: 429,
				headers: { "Retry-After": "0" },
			});
			const page: JiraSearchResponse = {
				startAt: 0,
				maxResults: 100,
				total: 0,
				issues: [],
			};

			mockFetch.mockResolvedValueOnce(rateLimited);
			mockFetch.mockResolvedValueOnce(jsonResponse(page));

			const client = new JiraClient(config);
			const result = await client.searchIssues("");

			expect(result.ok).toBe(true);
			expect(mockFetch).toHaveBeenCalledTimes(2);
		});

		it("returns rate limit error after max retries", async () => {
			const rateLimited = new Response("", {
				status: 429,
				headers: { "Retry-After": "0" },
			});

			mockFetch.mockResolvedValue(rateLimited);

			const client = new JiraClient(config);
			const result = await client.searchIssues("");

			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("JIRA_RATE_LIMITED");
			}
		});
	});

	describe("getComments", () => {
		it("returns comments for an issue", async () => {
			const page: JiraCommentPage = {
				startAt: 0,
				maxResults: 100,
				total: 1,
				comments: [
					{
						id: "100",
						body: { type: "doc", content: [] },
						author: { displayName: "Alice", emailAddress: "alice@co.com" },
						created: "2025-01-15",
						updated: "2025-01-15",
					},
				],
			};
			mockFetch.mockResolvedValueOnce(jsonResponse(page));

			const client = new JiraClient(config);
			const result = await client.getComments("ENG-1");

			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(1);
				expect(result.value[0]!.id).toBe("100");
			}
		});
	});

	describe("getProjects", () => {
		it("returns projects", async () => {
			const page: JiraProjectPage = {
				startAt: 0,
				maxResults: 100,
				total: 1,
				values: [
					{
						id: "200",
						key: "ENG",
						name: "Engineering",
						projectTypeKey: "software",
						lead: { displayName: "Charlie" },
					},
				],
			};
			mockFetch.mockResolvedValueOnce(jsonResponse(page));

			const client = new JiraClient(config);
			const result = await client.getProjects();

			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(1);
				expect(result.value[0]!.key).toBe("ENG");
			}
		});
	});
});
