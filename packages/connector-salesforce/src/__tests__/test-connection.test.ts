import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { testConnection } from "../test-connection";
import type { SalesforceAuthResponse } from "../types";

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
	instanceUrl: "https://mycompany.salesforce.com",
	clientId: "consumer-key",
	clientSecret: "consumer-secret",
	username: "user@company.com",
	password: "pass123token",
};

function jsonResponse(body: unknown, status = 200): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: { "Content-Type": "application/json" },
	});
}

const authResponse: SalesforceAuthResponse = {
	access_token: "test-token-123",
	instance_url: "https://mycompany.salesforce.com",
	token_type: "Bearer",
	issued_at: "1234567890",
	signature: "sig",
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("testConnection (Salesforce)", () => {
	it("returns Ok when authentication succeeds", async () => {
		mockFetch.mockResolvedValueOnce(jsonResponse(authResponse));

		const result = await testConnection(config);

		expect(result.ok).toBe(true);
		expect(mockFetch).toHaveBeenCalledOnce();

		const [url, init] = mockFetch.mock.calls[0]!;
		expect(url).toBe("https://login.salesforce.com/services/oauth2/token");
		expect((init as RequestInit).method).toBe("POST");

		const body = (init as RequestInit).body as string;
		expect(body).toContain("grant_type=password");
		expect(body).toContain("client_id=consumer-key");
	});

	it("returns Err on authentication failure", async () => {
		mockFetch.mockResolvedValueOnce(new Response("invalid_grant", { status: 400 }));

		const result = await testConnection(config);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("SALESFORCE_AUTH_ERROR");
		}
	});

	it("uses sandbox login URL when configured", async () => {
		mockFetch.mockResolvedValueOnce(jsonResponse(authResponse));

		const result = await testConnection({ ...config, isSandbox: true });

		expect(result.ok).toBe(true);
		const [url] = mockFetch.mock.calls[0]!;
		expect(url).toBe("https://test.salesforce.com/services/oauth2/token");
	});
});
