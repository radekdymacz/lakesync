// ---------------------------------------------------------------------------
// SalesforceClient — HTTP wrapper for Salesforce REST API
// ---------------------------------------------------------------------------

import { Err, Ok, type Result } from "@lakesync/core";
import { SalesforceApiError, SalesforceAuthError } from "./errors";
import type {
	SalesforceAuthResponse,
	SalesforceConnectorConfig,
	SalesforceQueryResponse,
} from "./types";

const DEFAULT_API_VERSION = "v62.0";
const MAX_RETRY_ATTEMPTS = 3;
const DEFAULT_RETRY_AFTER_MS = 10_000;

/**
 * HTTP client for the Salesforce REST API.
 *
 * Uses OAuth 2.0 Username-Password flow for authentication and global `fetch`.
 * All public methods return `Result<T, SalesforceApiError | SalesforceAuthError>`.
 */
export class SalesforceClient {
	private readonly config: SalesforceConnectorConfig;
	private readonly apiVersion: string;
	private readonly loginUrl: string;

	private accessToken: string | null = null;
	private instanceUrl: string;

	constructor(config: SalesforceConnectorConfig) {
		this.config = config;
		this.apiVersion = config.apiVersion ?? DEFAULT_API_VERSION;
		this.loginUrl = config.isSandbox
			? "https://test.salesforce.com"
			: "https://login.salesforce.com";
		this.instanceUrl = config.instanceUrl;
	}

	/**
	 * Authenticate via OAuth 2.0 Username-Password flow.
	 *
	 * Stores access token and updates instance URL from the response.
	 */
	async authenticate(): Promise<Result<void, SalesforceAuthError>> {
		const body = new URLSearchParams({
			grant_type: "password",
			client_id: this.config.clientId,
			client_secret: this.config.clientSecret,
			username: this.config.username,
			password: this.config.password,
		});

		let response: Response;
		try {
			response = await fetch(`${this.loginUrl}/services/oauth2/token`, {
				method: "POST",
				headers: { "Content-Type": "application/x-www-form-urlencoded" },
				body: body.toString(),
			});
		} catch (err) {
			return Err(
				new SalesforceAuthError(
					`Failed to connect to Salesforce auth endpoint: ${err instanceof Error ? err.message : String(err)}`,
					err instanceof Error ? err : undefined,
				),
			);
		}

		if (!response.ok) {
			const text = await response.text();
			return Err(
				new SalesforceAuthError(`Salesforce authentication failed (${response.status}): ${text}`),
			);
		}

		const data = (await response.json()) as SalesforceAuthResponse;
		this.accessToken = data.access_token;
		this.instanceUrl = data.instance_url;

		return Ok(undefined);
	}

	/**
	 * Execute a SOQL query with auto-pagination.
	 *
	 * Automatically authenticates on first call and re-authenticates on 401.
	 */
	async query<T>(soql: string): Promise<Result<T[], SalesforceApiError | SalesforceAuthError>> {
		// Ensure we have a token
		if (!this.accessToken) {
			const authResult = await this.authenticate();
			if (!authResult.ok) return authResult;
		}

		const allRecords: T[] = [];
		let url = `${this.instanceUrl}/services/data/${this.apiVersion}/query?q=${encodeURIComponent(soql)}`;

		while (true) {
			const result = await this.request<SalesforceQueryResponse<T>>(url);

			// Re-auth on 401 and retry once
			if (
				!result.ok &&
				result.error instanceof SalesforceApiError &&
				result.error.statusCode === 401
			) {
				const authResult = await this.authenticate();
				if (!authResult.ok) return authResult;

				const retryResult = await this.request<SalesforceQueryResponse<T>>(url);
				if (!retryResult.ok) return retryResult;

				for (const record of retryResult.value.records) {
					allRecords.push(record);
				}

				if (retryResult.value.done || !retryResult.value.nextRecordsUrl) break;
				url = `${this.instanceUrl}${retryResult.value.nextRecordsUrl}`;
				continue;
			}

			if (!result.ok) return result;

			for (const record of result.value.records) {
				allRecords.push(record);
			}

			if (result.value.done || !result.value.nextRecordsUrl) break;
			url = `${this.instanceUrl}${result.value.nextRecordsUrl}`;
		}

		return Ok(allRecords);
	}

	// -----------------------------------------------------------------------
	// Internal HTTP helpers
	// -----------------------------------------------------------------------

	/** Make an HTTP request with rate-limit retry logic. */
	private async request<T>(
		url: string,
	): Promise<Result<T, SalesforceApiError | SalesforceAuthError>> {
		for (let attempt = 0; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
			const headers: Record<string, string> = {
				Authorization: `Bearer ${this.accessToken}`,
				Accept: "application/json",
			};

			const response = await fetch(url, { method: "GET", headers });

			if (response.ok) {
				const data = (await response.json()) as T;
				return Ok(data);
			}

			// Rate limit: 503 with Retry-After
			if (response.status === 503) {
				const retryAfter = response.headers.get("Retry-After");
				const waitMs = retryAfter ? Number.parseInt(retryAfter, 10) * 1000 : DEFAULT_RETRY_AFTER_MS;

				if (attempt < MAX_RETRY_ATTEMPTS) {
					await sleep(waitMs);
					continue;
				}

				const responseBody = await response.text();
				return Err(new SalesforceApiError(503, responseBody));
			}

			const responseBody = await response.text();
			return Err(new SalesforceApiError(response.status, responseBody));
		}

		return Err(new SalesforceApiError(0, "Unknown error after retries"));
	}
}

/** Sleep for the given number of milliseconds. */
function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}
