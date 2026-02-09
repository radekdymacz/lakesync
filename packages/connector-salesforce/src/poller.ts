// ---------------------------------------------------------------------------
// SalesforceSourcePoller — polls Salesforce CRM and pushes deltas to SyncGateway
// ---------------------------------------------------------------------------

import type { HLCTimestamp, RowDelta, SyncPush } from "@lakesync/core";
import { extractDelta, HLC } from "@lakesync/core";
import type { SyncGateway } from "@lakesync/gateway";
import { SalesforceClient } from "./client";
import { mapAccount, mapContact, mapLead, mapOpportunity } from "./mapping";
import type {
	SalesforceConnectorConfig,
	SalesforceIngestConfig,
	SfAccount,
	SfContact,
	SfLead,
	SfOpportunity,
} from "./types";

const DEFAULT_INTERVAL_MS = 30_000;

// ---------------------------------------------------------------------------
// SOQL field lists
// ---------------------------------------------------------------------------

const ACCOUNT_FIELDS = [
	"Id",
	"Name",
	"Type",
	"Industry",
	"Website",
	"Phone",
	"BillingCity",
	"BillingState",
	"BillingCountry",
	"AnnualRevenue",
	"NumberOfEmployees",
	"Owner.Name",
	"CreatedDate",
	"LastModifiedDate",
].join(", ");

const CONTACT_FIELDS = [
	"Id",
	"FirstName",
	"LastName",
	"Email",
	"Phone",
	"Title",
	"AccountId",
	"Account.Name",
	"MailingCity",
	"MailingState",
	"MailingCountry",
	"Owner.Name",
	"CreatedDate",
	"LastModifiedDate",
].join(", ");

const OPPORTUNITY_FIELDS = [
	"Id",
	"Name",
	"StageName",
	"Amount",
	"CloseDate",
	"Probability",
	"AccountId",
	"Account.Name",
	"Type",
	"LeadSource",
	"IsClosed",
	"IsWon",
	"Owner.Name",
	"CreatedDate",
	"LastModifiedDate",
].join(", ");

const LEAD_FIELDS = [
	"Id",
	"FirstName",
	"LastName",
	"Company",
	"Email",
	"Phone",
	"Title",
	"Status",
	"LeadSource",
	"IsConverted",
	"ConvertedAccountId",
	"ConvertedContactId",
	"ConvertedOpportunityId",
	"Owner.Name",
	"CreatedDate",
	"LastModifiedDate",
].join(", ");

/**
 * Polls Salesforce CRM for accounts, contacts, opportunities, and leads
 * and pushes detected changes into a {@link SyncGateway} via `handlePush()`.
 *
 * Follows the same lifecycle contract as `JiraSourcePoller`:
 * `start()`, `stop()`, `isRunning`, `poll()`.
 */
export class SalesforceSourcePoller {
	private readonly connectionConfig: SalesforceConnectorConfig;
	private readonly intervalMs: number;
	private readonly gateway: SyncGateway;
	private readonly client: SalesforceClient;
	private readonly hlc: HLC;
	private readonly clientId: string;

	private timer: ReturnType<typeof setTimeout> | null = null;
	private running = false;

	/** Per-entity cursors: max LastModifiedDate from the last poll. */
	private cursors: Record<string, string | undefined> = {
		accounts: undefined,
		contacts: undefined,
		opportunities: undefined,
		leads: undefined,
	};

	constructor(
		connectionConfig: SalesforceConnectorConfig,
		ingestConfig: SalesforceIngestConfig | undefined,
		name: string,
		gateway: SyncGateway,
		client?: SalesforceClient,
	) {
		this.connectionConfig = connectionConfig;
		this.intervalMs = ingestConfig?.intervalMs ?? DEFAULT_INTERVAL_MS;
		this.gateway = gateway;
		this.client = client ?? new SalesforceClient(connectionConfig);
		this.hlc = new HLC();
		this.clientId = `ingest:${name}`;
	}

	/** Start the polling loop. */
	start(): void {
		if (this.running) return;
		this.running = true;
		this.schedulePoll();
	}

	/** Stop the polling loop. */
	stop(): void {
		this.running = false;
		if (this.timer) {
			clearTimeout(this.timer);
			this.timer = null;
		}
	}

	/** Whether the poller is currently running. */
	get isRunning(): boolean {
		return this.running;
	}

	/** Execute a single poll cycle across all enabled entity types. */
	async poll(): Promise<void> {
		const allDeltas: RowDelta[] = [];

		const includeAccounts = this.connectionConfig.includeAccounts ?? true;
		if (includeAccounts) {
			const deltas = await this.pollEntity<SfAccount>(
				"Account",
				ACCOUNT_FIELDS,
				"accounts",
				"sf_accounts",
				mapAccount,
			);
			for (const d of deltas) allDeltas.push(d);
		}

		const includeContacts = this.connectionConfig.includeContacts ?? true;
		if (includeContacts) {
			const deltas = await this.pollEntity<SfContact>(
				"Contact",
				CONTACT_FIELDS,
				"contacts",
				"sf_contacts",
				mapContact,
			);
			for (const d of deltas) allDeltas.push(d);
		}

		const includeOpportunities = this.connectionConfig.includeOpportunities ?? true;
		if (includeOpportunities) {
			const deltas = await this.pollEntity<SfOpportunity>(
				"Opportunity",
				OPPORTUNITY_FIELDS,
				"opportunities",
				"sf_opportunities",
				mapOpportunity,
			);
			for (const d of deltas) allDeltas.push(d);
		}

		const includeLeads = this.connectionConfig.includeLeads ?? true;
		if (includeLeads) {
			const deltas = await this.pollEntity<SfLead>(
				"Lead",
				LEAD_FIELDS,
				"leads",
				"sf_leads",
				mapLead,
			);
			for (const d of deltas) allDeltas.push(d);
		}

		if (allDeltas.length === 0) return;

		const push: SyncPush = {
			clientId: this.clientId,
			deltas: allDeltas,
			lastSeenHlc: 0n as HLCTimestamp,
		};

		this.gateway.handlePush(push);
	}

	// -----------------------------------------------------------------------
	// Poll scheduling (recursive setTimeout — no overlap)
	// -----------------------------------------------------------------------

	private schedulePoll(): void {
		if (!this.running) return;

		this.timer = setTimeout(async () => {
			try {
				await this.poll();
			} catch {
				// Swallow errors — a failed poll must never crash the server
			}
			this.schedulePoll();
		}, this.intervalMs);
	}

	// -----------------------------------------------------------------------
	// Generic entity polling via LastModifiedDate cursor
	// -----------------------------------------------------------------------

	private async pollEntity<T extends { Id: string; LastModifiedDate: string | null }>(
		sObjectType: string,
		fields: string,
		cursorKey: string,
		table: string,
		mapFn: (record: T) => { rowId: string; row: Record<string, unknown> },
	): Promise<RowDelta[]> {
		const cursor = this.cursors[cursorKey];
		const soql = this.buildSoql(sObjectType, fields, cursor);

		const result = await this.client.query<T>(soql);
		if (!result.ok) return [];

		const records = result.value;
		if (records.length === 0) return [];

		const deltas: RowDelta[] = [];
		let maxLastModified = cursor;

		for (const record of records) {
			const { rowId, row } = mapFn(record);

			const delta = await extractDelta(null, row, {
				table,
				rowId,
				clientId: this.clientId,
				hlc: this.hlc.now(),
			});

			if (delta) {
				deltas.push(delta);
			}

			const lastModified = record.LastModifiedDate;
			if (lastModified && (!maxLastModified || lastModified > maxLastModified)) {
				maxLastModified = lastModified;
			}
		}

		this.cursors[cursorKey] = maxLastModified;
		return deltas;
	}

	// -----------------------------------------------------------------------
	// SOQL query builder
	// -----------------------------------------------------------------------

	private buildSoql(sObjectType: string, fields: string, cursor: string | undefined): string {
		const clauses: string[] = [];

		if (cursor) {
			clauses.push(`LastModifiedDate > ${cursor}`);
		}

		if (this.connectionConfig.soqlFilter) {
			clauses.push(this.connectionConfig.soqlFilter);
		}

		const where = clauses.length > 0 ? ` WHERE ${clauses.join(" AND ")}` : "";
		return `SELECT ${fields} FROM ${sObjectType}${where} ORDER BY LastModifiedDate ASC`;
	}
}
