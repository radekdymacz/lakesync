// ---------------------------------------------------------------------------
// Jira Connector — E2E tests against a real Jira Cloud instance
//
// Requires a `.env` file in packages/connector-jira/ with:
//   JIRA_DOMAIN, JIRA_EMAIL, JIRA_API_TOKEN
//
// Skipped automatically when credentials are not present.
// ---------------------------------------------------------------------------

import { SyncGateway } from "@lakesync/gateway";
import { describe, expect, it } from "vitest";
import { JiraClient } from "../client";
import { JiraSourcePoller } from "../poller";
import type { JiraConnectorConfig } from "../types";

// ---------------------------------------------------------------------------
// Read credentials from env (loaded via vitest dotenv)
// ---------------------------------------------------------------------------

const config: JiraConnectorConfig | null = (() => {
	const domain = process.env.JIRA_DOMAIN;
	const email = process.env.JIRA_EMAIL;
	const apiToken = process.env.JIRA_API_TOKEN;

	if (!domain || !email || !apiToken) {
		return null;
	}

	return {
		domain,
		email,
		apiToken,
		jql: process.env.JIRA_JQL,
	};
})();

const hasCredentials = config !== null;

// E2E tests hit live APIs — allow generous timeouts
const E2E_TIMEOUT = 60_000;

/** Bounded JQL for tests — avoids unbounded queries rejected by the new search API. */
const testJql = () => config!.jql || "updated >= -1d";

// ---------------------------------------------------------------------------
// Tests — skip entire suite when credentials are absent
// ---------------------------------------------------------------------------

describe.skipIf(!hasCredentials)("Jira E2E", () => {
	it("searches issues", { timeout: E2E_TIMEOUT }, async () => {
		const client = new JiraClient(config!);
		const result = await client.searchIssues(testJql(), undefined, 100);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.length).toBeGreaterThan(0);
			expect(result.value[0]!.key).toBeTruthy();
			expect(result.value[0]!.fields.summary).toBeTruthy();
		}
	});

	it("fetches comments for an issue", { timeout: E2E_TIMEOUT }, async () => {
		const client = new JiraClient(config!);
		const issuesResult = await client.searchIssues(testJql(), undefined, 1);
		expect(issuesResult.ok).toBe(true);
		if (!issuesResult.ok) return;

		const issueKey = issuesResult.value[0]!.key;
		const result = await client.getComments(issueKey);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(Array.isArray(result.value)).toBe(true);
		}
	});

	it("fetches projects", { timeout: E2E_TIMEOUT }, async () => {
		const client = new JiraClient(config!);
		const result = await client.getProjects();

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.length).toBeGreaterThan(0);
			expect(result.value[0]!.key).toBeTruthy();
			expect(result.value[0]!.name).toBeTruthy();
		}
	});

	it("handles empty search result", { timeout: E2E_TIMEOUT }, async () => {
		const client = new JiraClient(config!);
		const result = await client.searchIssues("key = NONEXISTENT-99999");

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toHaveLength(0);
		}
	});

	it("polls and pushes deltas to a gateway", { timeout: E2E_TIMEOUT }, async () => {
		const gateway = new SyncGateway({
			gatewayId: "jira-e2e",
			maxBufferBytes: 16 * 1024 * 1024,
			maxBufferAgeMs: 30_000,
		});

		const poller = new JiraSourcePoller(
			{
				...config!,
				jql: testJql(),
				includeComments: false,
				includeProjects: false,
			},
			undefined,
			"jira-e2e",
			gateway,
		);

		await poller.poll();

		const pullResult = gateway.pullFromBuffer({
			clientId: "e2e-test-client",
			sinceHlc: 0n as never,
			maxDeltas: 10_000,
		});

		expect(pullResult.ok).toBe(true);
		if (pullResult.ok) {
			expect(pullResult.value.deltas.length).toBeGreaterThan(0);

			const issueDelta = pullResult.value.deltas.find((d) => d.table === "jira_issues");
			expect(issueDelta).toBeDefined();
		}
	});

	it("polls issues + projects together", { timeout: E2E_TIMEOUT }, async () => {
		const gateway = new SyncGateway({
			gatewayId: "jira-e2e-full",
			maxBufferBytes: 16 * 1024 * 1024,
			maxBufferAgeMs: 30_000,
		});

		const poller = new JiraSourcePoller(
			{
				...config!,
				jql: testJql(),
				includeComments: false,
				includeProjects: true,
			},
			undefined,
			"jira-e2e-full",
			gateway,
		);

		await poller.poll();

		const pullResult = gateway.pullFromBuffer({
			clientId: "e2e-test-client",
			sinceHlc: 0n as never,
			maxDeltas: 10_000,
		});

		expect(pullResult.ok).toBe(true);
		if (pullResult.ok) {
			const tables = new Set(pullResult.value.deltas.map((d) => d.table));
			expect(tables.has("jira_issues")).toBe(true);
			expect(tables.has("jira_projects")).toBe(true);
		}
	});
});

// ---------------------------------------------------------------------------
// Performance benchmarks — CF Worker 128 MB budget
// ---------------------------------------------------------------------------

const CF_WORKER_MEMORY_LIMIT = 128 * 1024 * 1024; // 128 MB

describe.skipIf(!hasCredentials)("Jira E2E — Performance", () => {
	it("measures fetch time per 100 issues", { timeout: E2E_TIMEOUT }, async () => {
		const client = new JiraClient(config!);

		const start = performance.now();
		const result = await client.searchIssues(testJql(), undefined, 100);
		const elapsed = performance.now() - start;

		expect(result.ok).toBe(true);
		if (!result.ok) return;

		const count = result.value.length;
		const msPerTicket = count > 0 ? elapsed / count : 0;

		console.log(`[perf] Fetched ${count} issues in ${elapsed.toFixed(0)} ms`);
		console.log(`[perf] Avg ${msPerTicket.toFixed(1)} ms/ticket`);

		// Sanity: fetching 100 tickets should complete within the timeout
		expect(elapsed).toBeLessThan(E2E_TIMEOUT);
	});

	it("measures memory usage for a full poll cycle", { timeout: E2E_TIMEOUT }, async () => {
		// Force GC if available (run vitest with --expose-gc for accuracy)
		if (typeof globalThis.gc === "function") globalThis.gc();

		const heapBefore = process.memoryUsage().heapUsed;

		const gateway = new SyncGateway({
			gatewayId: "jira-perf",
			maxBufferBytes: 16 * 1024 * 1024,
			maxBufferAgeMs: 30_000,
		});

		const poller = new JiraSourcePoller(
			{
				...config!,
				jql: testJql(),
				includeComments: false,
				includeProjects: false,
			},
			undefined,
			"jira-perf",
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
