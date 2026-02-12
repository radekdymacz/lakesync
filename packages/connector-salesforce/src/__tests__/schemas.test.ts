import { describe, expect, it } from "vitest";
import { mapAccount, mapContact, mapLead, mapOpportunity } from "../mapping";
import { SALESFORCE_TABLE_SCHEMAS } from "../schemas";
import type { SfAccount, SfContact, SfLead, SfOpportunity } from "../types";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function schemaFor(table: string) {
	const schema = SALESFORCE_TABLE_SCHEMAS.find((s) => s.table === table);
	if (!schema) throw new Error(`No schema found for table: ${table}`);
	return schema;
}

function columnNames(table: string): string[] {
	return schemaFor(table).columns.map((c) => c.name);
}

// ---------------------------------------------------------------------------
// Fixtures — minimal records with all fields populated
// ---------------------------------------------------------------------------

const fullAccount: SfAccount = {
	Id: "001",
	Name: "Acme",
	Type: "Customer",
	Industry: "Tech",
	Website: "https://acme.com",
	Phone: "+1234",
	BillingCity: "London",
	BillingState: "England",
	BillingCountry: "UK",
	AnnualRevenue: 1_000_000,
	NumberOfEmployees: 50,
	Owner: { Name: "Alice" },
	CreatedDate: "2025-01-01T00:00:00.000+0000",
	LastModifiedDate: "2025-06-01T00:00:00.000+0000",
};

const fullContact: SfContact = {
	Id: "003",
	FirstName: "Jane",
	LastName: "Doe",
	Email: "jane@acme.com",
	Phone: "+1234",
	Title: "CTO",
	AccountId: "001",
	Account: { Name: "Acme" },
	MailingCity: "London",
	MailingState: "England",
	MailingCountry: "UK",
	Owner: { Name: "Alice" },
	CreatedDate: "2025-01-01T00:00:00.000+0000",
	LastModifiedDate: "2025-06-01T00:00:00.000+0000",
};

const fullOpportunity: SfOpportunity = {
	Id: "006",
	Name: "Big Deal",
	StageName: "Closed Won",
	Amount: 500_000,
	CloseDate: "2025-12-31",
	Probability: 100,
	AccountId: "001",
	Account: { Name: "Acme" },
	Type: "New Business",
	LeadSource: "Web",
	IsClosed: true,
	IsWon: true,
	Owner: { Name: "Alice" },
	CreatedDate: "2025-01-01T00:00:00.000+0000",
	LastModifiedDate: "2025-06-01T00:00:00.000+0000",
};

const fullLead: SfLead = {
	Id: "00Q",
	FirstName: "John",
	LastName: "Smith",
	Company: "Widget Co",
	Email: "john@widget.co",
	Phone: "+5678",
	Title: "VP Sales",
	Status: "Working",
	LeadSource: "Conference",
	IsConverted: true,
	ConvertedAccountId: "001",
	ConvertedContactId: "003",
	ConvertedOpportunityId: "006",
	Owner: { Name: "Bob" },
	CreatedDate: "2025-01-01T00:00:00.000+0000",
	LastModifiedDate: "2025-06-01T00:00:00.000+0000",
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("SALESFORCE_TABLE_SCHEMAS", () => {
	it("exports schemas for all four entity tables", () => {
		const tables = SALESFORCE_TABLE_SCHEMAS.map((s) => s.table);
		expect(tables).toEqual(["sf_accounts", "sf_contacts", "sf_opportunities", "sf_leads"]);
	});

	it("all columns use 'string' type", () => {
		for (const schema of SALESFORCE_TABLE_SCHEMAS) {
			for (const col of schema.columns) {
				expect(col.type).toBe("string");
			}
		}
	});

	it("sf_accounts schema matches mapAccount() output keys", () => {
		const schemaColumns = columnNames("sf_accounts");
		const rowKeys = Object.keys(mapAccount(fullAccount).row);
		expect(schemaColumns).toEqual(rowKeys);
	});

	it("sf_contacts schema matches mapContact() output keys", () => {
		const schemaColumns = columnNames("sf_contacts");
		const rowKeys = Object.keys(mapContact(fullContact).row);
		expect(schemaColumns).toEqual(rowKeys);
	});

	it("sf_opportunities schema matches mapOpportunity() output keys", () => {
		const schemaColumns = columnNames("sf_opportunities");
		const rowKeys = Object.keys(mapOpportunity(fullOpportunity).row);
		expect(schemaColumns).toEqual(rowKeys);
	});

	it("sf_leads schema matches mapLead() output keys", () => {
		const schemaColumns = columnNames("sf_leads");
		const rowKeys = Object.keys(mapLead(fullLead).row);
		expect(schemaColumns).toEqual(rowKeys);
	});

	it("sf_contacts.account_id references sf_accounts", () => {
		const col = schemaFor("sf_contacts").columns.find((c) => c.name === "account_id")!;
		expect(col.references).toEqual({
			table: "sf_accounts",
			column: "sf_id",
			cardinality: "many-to-one",
		});
	});

	it("sf_opportunities.account_id references sf_accounts", () => {
		const col = schemaFor("sf_opportunities").columns.find((c) => c.name === "account_id")!;
		expect(col.references).toEqual({
			table: "sf_accounts",
			column: "sf_id",
			cardinality: "many-to-one",
		});
	});

	it("sf_leads converted columns reference correct tables", () => {
		const leads = schemaFor("sf_leads");
		const accountRef = leads.columns.find((c) => c.name === "converted_account_id")!;
		const contactRef = leads.columns.find((c) => c.name === "converted_contact_id")!;
		const opptyRef = leads.columns.find((c) => c.name === "converted_opportunity_id")!;

		expect(accountRef.references).toEqual({
			table: "sf_accounts",
			column: "sf_id",
			cardinality: "many-to-one",
		});
		expect(contactRef.references).toEqual({
			table: "sf_contacts",
			column: "sf_id",
			cardinality: "many-to-one",
		});
		expect(opptyRef.references).toEqual({
			table: "sf_opportunities",
			column: "sf_id",
			cardinality: "many-to-one",
		});
	});
});
