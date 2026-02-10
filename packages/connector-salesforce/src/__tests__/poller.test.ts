import { Ok, type RowDelta, type SyncPush } from "@lakesync/core";
import { SyncGateway } from "@lakesync/gateway";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { SalesforceClient } from "../client";
import { SalesforceSourcePoller } from "../poller";
import type { SfAccount, SfContact, SfLead, SfOpportunity } from "../types";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Collect all deltas across all handlePush calls. */
function collectDeltas(spy: { mock: { calls: unknown[][] } }): RowDelta[] {
	const all: RowDelta[] = [];
	for (const call of spy.mock.calls) {
		const push = call[0] as SyncPush;
		for (const d of push.deltas) all.push(d);
	}
	return all;
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

function makeAccount(id: string, lastModified: string): SfAccount {
	return {
		Id: id,
		Name: `Account ${id}`,
		Type: "Customer",
		Industry: "Technology",
		Website: null,
		Phone: null,
		BillingCity: null,
		BillingState: null,
		BillingCountry: null,
		AnnualRevenue: null,
		NumberOfEmployees: null,
		Owner: null,
		CreatedDate: "2025-01-01T00:00:00.000+0000",
		LastModifiedDate: lastModified,
	};
}

function makeContact(id: string, lastModified: string): SfContact {
	return {
		Id: id,
		FirstName: "Jane",
		LastName: "Doe",
		Email: null,
		Phone: null,
		Title: null,
		AccountId: null,
		Account: null,
		MailingCity: null,
		MailingState: null,
		MailingCountry: null,
		Owner: null,
		CreatedDate: "2025-01-01T00:00:00.000+0000",
		LastModifiedDate: lastModified,
	};
}

function makeOpportunity(id: string, lastModified: string): SfOpportunity {
	return {
		Id: id,
		Name: `Opp ${id}`,
		StageName: "Prospecting",
		Amount: null,
		CloseDate: null,
		Probability: null,
		AccountId: null,
		Account: null,
		Type: null,
		LeadSource: null,
		IsClosed: false,
		IsWon: false,
		Owner: null,
		CreatedDate: "2025-01-01T00:00:00.000+0000",
		LastModifiedDate: lastModified,
	};
}

function makeLead(id: string, lastModified: string): SfLead {
	return {
		Id: id,
		FirstName: "John",
		LastName: "Smith",
		Company: "Widget Co",
		Email: null,
		Phone: null,
		Title: null,
		Status: "Open",
		LeadSource: null,
		IsConverted: false,
		ConvertedAccountId: null,
		ConvertedContactId: null,
		ConvertedOpportunityId: null,
		Owner: null,
		CreatedDate: "2025-01-01T00:00:00.000+0000",
		LastModifiedDate: lastModified,
	};
}

// ---------------------------------------------------------------------------
// Mock SalesforceClient
// ---------------------------------------------------------------------------

function createMockClient(overrides: {
	accounts?: SfAccount[];
	contacts?: SfContact[];
	opportunities?: SfOpportunity[];
	leads?: SfLead[];
}): SalesforceClient {
	const client = {
		query: vi.fn().mockImplementation((soql: string) => {
			if (soql.includes("FROM Account")) {
				return Promise.resolve(Ok(overrides.accounts ?? []));
			}
			if (soql.includes("FROM Contact")) {
				return Promise.resolve(Ok(overrides.contacts ?? []));
			}
			if (soql.includes("FROM Opportunity")) {
				return Promise.resolve(Ok(overrides.opportunities ?? []));
			}
			if (soql.includes("FROM Lead")) {
				return Promise.resolve(Ok(overrides.leads ?? []));
			}
			return Promise.resolve(Ok([]));
		}),
		authenticate: vi.fn().mockResolvedValue(Ok(undefined)),
	};
	return client as unknown as SalesforceClient;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("SalesforceSourcePoller", () => {
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

	it("pushes account deltas to gateway on first poll", async () => {
		const client = createMockClient({
			accounts: [makeAccount("001A", "2025-01-15T00:00:00.000+0000")],
		});

		const poller = new SalesforceSourcePoller(
			{
				instanceUrl: "https://test.salesforce.com",
				clientId: "cid",
				clientSecret: "csecret",
				username: "user",
				password: "pass",
				includeContacts: false,
				includeOpportunities: false,
				includeLeads: false,
			},
			undefined,
			"sf-test",
			gateway,
			client,
		);

		const handlePushSpy = vi.spyOn(gateway, "handlePush");
		await poller.poll();

		expect(handlePushSpy).toHaveBeenCalled();
		const allDeltas = collectDeltas(handlePushSpy);
		expect(allDeltas.length).toBeGreaterThan(0);

		const accountDelta = allDeltas.find((d) => d.table === "sf_accounts");
		expect(accountDelta).toBeDefined();
		expect(accountDelta!.rowId).toBe("001A");
	});

	it("polls all four entity types by default", async () => {
		const client = createMockClient({
			accounts: [makeAccount("001A", "2025-01-15T00:00:00.000+0000")],
			contacts: [makeContact("003A", "2025-01-15T00:00:00.000+0000")],
			opportunities: [makeOpportunity("006A", "2025-01-15T00:00:00.000+0000")],
			leads: [makeLead("00QA", "2025-01-15T00:00:00.000+0000")],
		});

		const poller = new SalesforceSourcePoller(
			{
				instanceUrl: "https://test.salesforce.com",
				clientId: "cid",
				clientSecret: "csecret",
				username: "user",
				password: "pass",
			},
			undefined,
			"sf-test",
			gateway,
			client,
		);

		const handlePushSpy = vi.spyOn(gateway, "handlePush");
		await poller.poll();

		expect(handlePushSpy).toHaveBeenCalled();
		const allDeltas = collectDeltas(handlePushSpy);

		const tables = new Set(allDeltas.map((d) => d.table));
		expect(tables.has("sf_accounts")).toBe(true);
		expect(tables.has("sf_contacts")).toBe(true);
		expect(tables.has("sf_opportunities")).toBe(true);
		expect(tables.has("sf_leads")).toBe(true);
	});

	it("advances cursor on subsequent polls", async () => {
		const client = createMockClient({
			accounts: [makeAccount("001A", "2025-01-15T00:00:00.000+0000")],
		});

		const poller = new SalesforceSourcePoller(
			{
				instanceUrl: "https://test.salesforce.com",
				clientId: "cid",
				clientSecret: "csecret",
				username: "user",
				password: "pass",
				includeContacts: false,
				includeOpportunities: false,
				includeLeads: false,
			},
			undefined,
			"sf-test",
			gateway,
			client,
		);

		await poller.poll();

		// Second poll — should include LastModifiedDate > cursor in SOQL
		(client.query as ReturnType<typeof vi.fn>).mockResolvedValueOnce(Ok([]));
		await poller.poll();

		const secondCall = (client.query as ReturnType<typeof vi.fn>).mock.calls[1]!;
		const soql = secondCall[0] as string;
		expect(soql).toContain("LastModifiedDate > 2025-01-15T00:00:00.000+0000");
	});

	it("does not push when no records found", async () => {
		const client = createMockClient({});

		const poller = new SalesforceSourcePoller(
			{
				instanceUrl: "https://test.salesforce.com",
				clientId: "cid",
				clientSecret: "csecret",
				username: "user",
				password: "pass",
			},
			undefined,
			"sf-test",
			gateway,
			client,
		);

		const handlePushSpy = vi.spyOn(gateway, "handlePush");
		await poller.poll();

		expect(handlePushSpy).not.toHaveBeenCalled();
	});

	it("skips disabled entity types", async () => {
		const client = createMockClient({
			accounts: [makeAccount("001A", "2025-01-15T00:00:00.000+0000")],
			contacts: [makeContact("003A", "2025-01-15T00:00:00.000+0000")],
			opportunities: [makeOpportunity("006A", "2025-01-15T00:00:00.000+0000")],
			leads: [makeLead("00QA", "2025-01-15T00:00:00.000+0000")],
		});

		const poller = new SalesforceSourcePoller(
			{
				instanceUrl: "https://test.salesforce.com",
				clientId: "cid",
				clientSecret: "csecret",
				username: "user",
				password: "pass",
				includeAccounts: false,
				includeLeads: false,
			},
			undefined,
			"sf-test",
			gateway,
			client,
		);

		const handlePushSpy = vi.spyOn(gateway, "handlePush");
		await poller.poll();

		const allDeltas = collectDeltas(handlePushSpy);
		const tables = new Set(allDeltas.map((d) => d.table));
		expect(tables.has("sf_accounts")).toBe(false);
		expect(tables.has("sf_leads")).toBe(false);
		expect(tables.has("sf_contacts")).toBe(true);
		expect(tables.has("sf_opportunities")).toBe(true);
	});

	it("start/stop lifecycle", async () => {
		vi.useFakeTimers();

		const client = createMockClient({});

		const poller = new SalesforceSourcePoller(
			{
				instanceUrl: "https://test.salesforce.com",
				clientId: "cid",
				clientSecret: "csecret",
				username: "user",
				password: "pass",
			},
			{ intervalMs: 1000 },
			"sf-test",
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
		(client.query as ReturnType<typeof vi.fn>).mockRejectedValueOnce(new Error("Network error"));

		const poller = new SalesforceSourcePoller(
			{
				instanceUrl: "https://test.salesforce.com",
				clientId: "cid",
				clientSecret: "csecret",
				username: "user",
				password: "pass",
				includeContacts: false,
				includeOpportunities: false,
				includeLeads: false,
			},
			undefined,
			"sf-test",
			gateway,
			client,
		);

		// poll() itself should throw (swallowing happens in schedulePoll)
		await expect(poller.poll()).rejects.toThrow("Network error");
	});
});
