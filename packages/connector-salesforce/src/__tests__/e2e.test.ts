// ---------------------------------------------------------------------------
// Salesforce Connector — E2E tests against a real Salesforce Developer org
//
// Requires a `.env` file in packages/connector-salesforce/ with:
//   SF_INSTANCE_URL, SF_CLIENT_ID, SF_CLIENT_SECRET, SF_USERNAME, SF_PASSWORD
//
// Skipped automatically when credentials are not present.
// ---------------------------------------------------------------------------

import { SyncGateway } from "@lakesync/gateway";
import { describe, expect, it } from "vitest";
import { SalesforceClient } from "../client";
import { SalesforceSourcePoller } from "../poller";
import type { SalesforceConnectorConfig, SfAccount, SfContact } from "../types";

// ---------------------------------------------------------------------------
// Read credentials from env (loaded via vitest dotenv)
// ---------------------------------------------------------------------------

const config: SalesforceConnectorConfig | null = (() => {
	const instanceUrl = process.env.SF_INSTANCE_URL;
	const clientId = process.env.SF_CLIENT_ID;
	const clientSecret = process.env.SF_CLIENT_SECRET;
	const username = process.env.SF_USERNAME;
	const password = process.env.SF_PASSWORD;

	if (!instanceUrl || !clientId || !clientSecret || !username || !password) {
		return null;
	}

	return {
		instanceUrl,
		clientId,
		clientSecret,
		username,
		password,
		isSandbox: process.env.SF_IS_SANDBOX === "true",
	};
})();

const hasCredentials = config !== null;

// ---------------------------------------------------------------------------
// Tests — skip entire suite when credentials are absent
// ---------------------------------------------------------------------------

describe.skipIf(!hasCredentials)("Salesforce E2E", () => {
	it("authenticates with the Salesforce org", async () => {
		const client = new SalesforceClient(config!);
		const result = await client.authenticate();

		expect(result.ok).toBe(true);
	});

	it("queries accounts", async () => {
		const client = new SalesforceClient(config!);
		const result = await client.query<SfAccount>(
			"SELECT Id, Name, Industry, LastModifiedDate FROM Account ORDER BY LastModifiedDate DESC LIMIT 5",
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			// Dev orgs ship with sample data — should have at least one account
			expect(result.value.length).toBeGreaterThan(0);
			expect(result.value[0]!.Id).toBeTruthy();
			expect(result.value[0]!.Name).toBeTruthy();
		}
	});

	it("queries contacts", async () => {
		const client = new SalesforceClient(config!);
		const result = await client.query<SfContact>(
			"SELECT Id, FirstName, LastName, Email, LastModifiedDate FROM Contact ORDER BY LastModifiedDate DESC LIMIT 5",
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.length).toBeGreaterThan(0);
			expect(result.value[0]!.Id).toBeTruthy();
		}
	});

	it("handles empty query result", async () => {
		const client = new SalesforceClient(config!);
		// Query for a non-existent ID
		const result = await client.query<SfAccount>(
			"SELECT Id, Name FROM Account WHERE Id = '001000000000000AAA'",
		);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toHaveLength(0);
		}
	});

	it("polls and pushes deltas to a gateway", async () => {
		const gateway = new SyncGateway({
			gatewayId: "sf-e2e",
			maxBufferBytes: 4 * 1024 * 1024,
			maxBufferAgeMs: 30_000,
		});

		const poller = new SalesforceSourcePoller(
			{
				...config!,
				// Only poll accounts to keep the test fast
				includeContacts: false,
				includeOpportunities: false,
				includeLeads: false,
			},
			undefined,
			"sf-e2e",
			gateway,
		);

		// Single poll cycle
		await poller.poll();

		// Pull deltas from gateway to verify they arrived
		const pullResult = gateway.pullFromBuffer({
			clientId: "e2e-test-client",
			sinceHlc: 0n as never,
			maxDeltas: 10_000,
		});

		expect(pullResult.ok).toBe(true);
		if (pullResult.ok) {
			expect(pullResult.value.deltas.length).toBeGreaterThan(0);

			const accountDelta = pullResult.value.deltas.find((d) => d.table === "sf_accounts");
			expect(accountDelta).toBeDefined();
		}
	});
});

// ---------------------------------------------------------------------------
// Performance benchmarks — CF Worker 128 MB budget
// ---------------------------------------------------------------------------

const E2E_TIMEOUT = 60_000;
const CF_WORKER_MEMORY_LIMIT = 128 * 1024 * 1024; // 128 MB

describe.skipIf(!hasCredentials)("Salesforce E2E — Performance", () => {
	it("measures query time per 100 records", { timeout: E2E_TIMEOUT }, async () => {
		const client = new SalesforceClient(config!);

		const start = performance.now();
		const result = await client.query<SfAccount>(
			"SELECT Id, Name, Industry, LastModifiedDate FROM Account LIMIT 100",
		);
		const elapsed = performance.now() - start;

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		const count = result.value.length;
		const msPerRecord = count > 0 ? elapsed / count : 0;

		console.log(`[perf] Fetched ${count} accounts in ${elapsed.toFixed(0)} ms`);
		console.log(`[perf] Avg ${msPerRecord.toFixed(1)} ms/record`);

		expect(elapsed).toBeLessThan(E2E_TIMEOUT);
	});

	it("measures memory usage for a full poll cycle", { timeout: E2E_TIMEOUT }, async () => {
		if (typeof globalThis.gc === "function") globalThis.gc();

		const heapBefore = process.memoryUsage().heapUsed;

		const gateway = new SyncGateway({
			gatewayId: "sf-perf",
			maxBufferBytes: 4 * 1024 * 1024,
			maxBufferAgeMs: 30_000,
		});

		const poller = new SalesforceSourcePoller(
			{
				...config!,
				includeContacts: false,
				includeOpportunities: false,
				includeLeads: false,
			},
			undefined,
			"sf-perf",
			gateway,
		);

		const start = performance.now();
		await poller.poll();
		const pollMs = performance.now() - start;

		const heapAfter = process.memoryUsage().heapUsed;
		const heapDeltaBytes = heapAfter - heapBefore;

		const pullResult = gateway.pullFromBuffer({
			clientId: "perf-client",
			sinceHlc: 0n as never,
			maxDeltas: 10_000,
		});

		expect(pullResult.ok).toBe(true);
		if (!pullResult.ok) return;

		const deltaCount = pullResult.value.deltas.length;
		const bytesPerDelta = deltaCount > 0 ? heapDeltaBytes / deltaCount : 0;

		console.log(`[perf] Poll cycle: ${pollMs.toFixed(0)} ms`);
		console.log(`[perf] Deltas produced: ${deltaCount}`);
		console.log(`[perf] Heap delta: ${(heapDeltaBytes / 1024 / 1024).toFixed(2)} MB`);
		console.log(`[perf] ~${(bytesPerDelta / 1024).toFixed(2)} KB/delta`);
		console.log(
			`[perf] Estimated capacity at 128 MB: ~${Math.floor(CF_WORKER_MEMORY_LIMIT / Math.max(bytesPerDelta, 1))} deltas`,
		);

		// Hard gate: poll cycle must stay well under 128 MB
		// Allow 64 MB for the connector — leaves headroom for the gateway + runtime
		expect(heapDeltaBytes).toBeLessThan(64 * 1024 * 1024);
	});
});
