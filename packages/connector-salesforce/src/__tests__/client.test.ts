import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SalesforceClient } from "../client";
import type { SalesforceAuthResponse, SalesforceQueryResponse, SfAccount } from "../types";

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

function jsonResponse(body: unknown, status = 200, headers?: Record<string, string>): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: { "Content-Type": "application/json", ...headers },
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

describe("SalesforceClient", () => {
	describe("authenticate", () => {
		it("sends correct OAuth password grant", async () => {
			mockFetch.mockResolvedValueOnce(jsonResponse(authResponse));

			const client = new SalesforceClient(config);
			const result = await client.authenticate();

			expect(result.ok).toBe(true);
			expect(mockFetch).toHaveBeenCalledOnce();

			const [url, init] = mockFetch.mock.calls[0]!;
			expect(url).toBe("https://login.salesforce.com/services/oauth2/token");
			expect((init as RequestInit).method).toBe("POST");

			const body = (init as RequestInit).body as string;
			expect(body).toContain("grant_type=password");
			expect(body).toContain("client_id=consumer-key");
			expect(body).toContain("username=user%40company.com");
		});

		it("uses test.salesforce.com for sandbox", async () => {
			mockFetch.mockResolvedValueOnce(jsonResponse(authResponse));

			const client = new SalesforceClient({ ...config, isSandbox: true });
			const result = await client.authenticate();

			expect(result.ok).toBe(true);
			const [url] = mockFetch.mock.calls[0]!;
			expect(url).toBe("https://test.salesforce.com/services/oauth2/token");
		});

		it("returns error on auth failure", async () => {
			mockFetch.mockResolvedValueOnce(new Response("invalid_grant", { status: 400 }));

			const client = new SalesforceClient(config);
			const result = await client.authenticate();

			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("SALESFORCE_AUTH_ERROR");
			}
		});
	});

	describe("query", () => {
		it("auto-authenticates and returns records", async () => {
			const queryResponse: SalesforceQueryResponse<SfAccount> = {
				totalSize: 1,
				done: true,
				records: [
					{
						Id: "001ABC",
						Name: "Acme Corp",
						Type: "Customer",
						Industry: "Technology",
						Website: "https://acme.com",
						Phone: "555-0100",
						BillingCity: "San Francisco",
						BillingState: "CA",
						BillingCountry: "US",
						AnnualRevenue: 1_000_000,
						NumberOfEmployees: 50,
						Owner: { Name: "Alice" },
						CreatedDate: "2025-01-01T00:00:00.000+0000",
						LastModifiedDate: "2025-01-15T00:00:00.000+0000",
					},
				],
			};

			// First call = auth, second = query
			mockFetch.mockResolvedValueOnce(jsonResponse(authResponse));
			mockFetch.mockResolvedValueOnce(jsonResponse(queryResponse));

			const client = new SalesforceClient(config);
			const result = await client.query<SfAccount>("SELECT Id FROM Account");

			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(1);
				expect(result.value[0]!.Id).toBe("001ABC");
			}

			// Auth + query = 2 calls
			expect(mockFetch).toHaveBeenCalledTimes(2);
		});

		it("paginates across multiple pages", async () => {
			const page1: SalesforceQueryResponse<SfAccount> = {
				totalSize: 2,
				done: false,
				nextRecordsUrl: "/services/data/v62.0/query/01gD000-next",
				records: [
					{
						Id: "001A",
						Name: "First",
						Type: null,
						Industry: null,
						Website: null,
						Phone: null,
						BillingCity: null,
						BillingState: null,
						BillingCountry: null,
						AnnualRevenue: null,
						NumberOfEmployees: null,
						Owner: null,
						CreatedDate: null,
						LastModifiedDate: null,
					},
				],
			};

			const page2: SalesforceQueryResponse<SfAccount> = {
				totalSize: 2,
				done: true,
				records: [
					{
						Id: "001B",
						Name: "Second",
						Type: null,
						Industry: null,
						Website: null,
						Phone: null,
						BillingCity: null,
						BillingState: null,
						BillingCountry: null,
						AnnualRevenue: null,
						NumberOfEmployees: null,
						Owner: null,
						CreatedDate: null,
						LastModifiedDate: null,
					},
				],
			};

			mockFetch.mockResolvedValueOnce(jsonResponse(authResponse));
			mockFetch.mockResolvedValueOnce(jsonResponse(page1));
			mockFetch.mockResolvedValueOnce(jsonResponse(page2));

			const client = new SalesforceClient(config);
			const result = await client.query<SfAccount>("SELECT Id FROM Account");

			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(2);
				expect(result.value[0]!.Id).toBe("001A");
				expect(result.value[1]!.Id).toBe("001B");
			}
		});

		it("re-authenticates on 401 and retries", async () => {
			const queryResponse: SalesforceQueryResponse<SfAccount> = {
				totalSize: 0,
				done: true,
				records: [],
			};

			// auth → query (401) → re-auth → retry (200)
			mockFetch.mockResolvedValueOnce(jsonResponse(authResponse));
			mockFetch.mockResolvedValueOnce(new Response("Unauthorized", { status: 401 }));
			mockFetch.mockResolvedValueOnce(jsonResponse(authResponse));
			mockFetch.mockResolvedValueOnce(jsonResponse(queryResponse));

			const client = new SalesforceClient(config);
			const result = await client.query<SfAccount>("SELECT Id FROM Account");

			expect(result.ok).toBe(true);
			expect(mockFetch).toHaveBeenCalledTimes(4);
		});

		it("returns error on HTTP failure", async () => {
			mockFetch.mockResolvedValueOnce(jsonResponse(authResponse));
			mockFetch.mockResolvedValueOnce(new Response("Forbidden", { status: 403 }));

			const client = new SalesforceClient(config);
			const result = await client.query<SfAccount>("SELECT Id FROM Account");

			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("SALESFORCE_API_ERROR");
			}
		});

		it("sends Bearer token in query requests", async () => {
			const queryResponse: SalesforceQueryResponse<SfAccount> = {
				totalSize: 0,
				done: true,
				records: [],
			};

			mockFetch.mockResolvedValueOnce(jsonResponse(authResponse));
			mockFetch.mockResolvedValueOnce(jsonResponse(queryResponse));

			const client = new SalesforceClient(config);
			await client.query<SfAccount>("SELECT Id FROM Account");

			// Second call is the query
			const [, init] = mockFetch.mock.calls[1]!;
			const headers = (init as RequestInit).headers as Record<string, string>;
			expect(headers.Authorization).toBe("Bearer test-token-123");
		});
	});

	describe("rate limiting", () => {
		it("retries on 503 with Retry-After", async () => {
			const queryResponse: SalesforceQueryResponse<SfAccount> = {
				totalSize: 0,
				done: true,
				records: [],
			};

			mockFetch.mockResolvedValueOnce(jsonResponse(authResponse));
			mockFetch.mockResolvedValueOnce(
				new Response("", { status: 503, headers: { "Retry-After": "0" } }),
			);
			mockFetch.mockResolvedValueOnce(jsonResponse(queryResponse));

			const client = new SalesforceClient(config);
			const result = await client.query<SfAccount>("SELECT Id FROM Account");

			expect(result.ok).toBe(true);
			// auth + 503 + retry = 3
			expect(mockFetch).toHaveBeenCalledTimes(3);
		});

		it("returns error after max retries on 503", async () => {
			const serviceUnavailable = new Response("", {
				status: 503,
				headers: { "Retry-After": "0" },
			});

			mockFetch.mockResolvedValueOnce(jsonResponse(authResponse));
			mockFetch.mockResolvedValue(serviceUnavailable);

			const client = new SalesforceClient(config);
			const result = await client.query<SfAccount>("SELECT Id FROM Account");

			expect(result.ok).toBe(false);
			if (!result.ok) {
				expect(result.error.code).toBe("SALESFORCE_API_ERROR");
			}
		});
	});
});
