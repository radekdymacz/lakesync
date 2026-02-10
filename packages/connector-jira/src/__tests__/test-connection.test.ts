import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { testConnection } from "../test-connection";

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

function jsonResponse(body: unknown, status = 200): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: { "Content-Type": "application/json" },
	});
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("testConnection (Jira)", () => {
	it("returns Ok when /myself succeeds", async () => {
		mockFetch.mockResolvedValueOnce(
			jsonResponse({ displayName: "Bot User", emailAddress: "bot@co.com" }),
		);

		const result = await testConnection(config);

		expect(result.ok).toBe(true);
		expect(mockFetch).toHaveBeenCalledOnce();

		const [url, init] = mockFetch.mock.calls[0]!;
		expect(url).toBe("https://mycompany.atlassian.net/rest/api/3/myself");
		expect((init as RequestInit).method).toBe("GET");

		const headers = (init as RequestInit).headers as Record<string, string>;
		const expected = `Basic ${btoa("bot@co.com:tok-123")}`;
		expect(headers.Authorization).toBe(expected);
	});

	it("returns Err on bad credentials", async () => {
		mockFetch.mockResolvedValueOnce(new Response("Unauthorized", { status: 401 }));

		const result = await testConnection(config);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("JIRA_API_ERROR");
		}
	});

	it("returns Err on rate limit", async () => {
		const rateLimited = new Response("", {
			status: 429,
			headers: { "Retry-After": "0" },
		});
		// Return 429 for all retries
		mockFetch.mockResolvedValue(rateLimited);

		const result = await testConnection(config);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("JIRA_RATE_LIMITED");
		}
	});
});
