import { describe, expect, it } from "vitest";
import { mapAccount, mapContact, mapLead, mapOpportunity } from "../mapping";
import type { SfAccount, SfContact, SfLead, SfOpportunity } from "../types";

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const fullAccount: SfAccount = {
	Id: "001ABC123",
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
};

const nullAccount: SfAccount = {
	Id: "001DEF456",
	Name: null,
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
};

const fullContact: SfContact = {
	Id: "003ABC123",
	FirstName: "Jane",
	LastName: "Doe",
	Email: "jane@acme.com",
	Phone: "555-0200",
	Title: "VP Engineering",
	AccountId: "001ABC123",
	Account: { Name: "Acme Corp" },
	MailingCity: "New York",
	MailingState: "NY",
	MailingCountry: "US",
	Owner: { Name: "Bob" },
	CreatedDate: "2025-02-01T00:00:00.000+0000",
	LastModifiedDate: "2025-02-10T00:00:00.000+0000",
};

const nullContact: SfContact = {
	Id: "003DEF456",
	FirstName: null,
	LastName: null,
	Email: null,
	Phone: null,
	Title: null,
	AccountId: null,
	Account: null,
	MailingCity: null,
	MailingState: null,
	MailingCountry: null,
	Owner: null,
	CreatedDate: null,
	LastModifiedDate: null,
};

const fullOpportunity: SfOpportunity = {
	Id: "006ABC123",
	Name: "Acme Enterprise Deal",
	StageName: "Negotiation",
	Amount: 500_000,
	CloseDate: "2025-06-30",
	Probability: 75,
	AccountId: "001ABC123",
	Account: { Name: "Acme Corp" },
	Type: "New Business",
	LeadSource: "Web",
	IsClosed: false,
	IsWon: false,
	Owner: { Name: "Charlie" },
	CreatedDate: "2025-03-01T00:00:00.000+0000",
	LastModifiedDate: "2025-03-15T00:00:00.000+0000",
};

const nullOpportunity: SfOpportunity = {
	Id: "006DEF456",
	Name: null,
	StageName: null,
	Amount: null,
	CloseDate: null,
	Probability: null,
	AccountId: null,
	Account: null,
	Type: null,
	LeadSource: null,
	IsClosed: null,
	IsWon: null,
	Owner: null,
	CreatedDate: null,
	LastModifiedDate: null,
};

const fullLead: SfLead = {
	Id: "00QABC123",
	FirstName: "John",
	LastName: "Smith",
	Company: "Widget Co",
	Email: "john@widget.com",
	Phone: "555-0300",
	Title: "CTO",
	Status: "Working",
	LeadSource: "Conference",
	IsConverted: true,
	ConvertedAccountId: "001GHI789",
	ConvertedContactId: "003GHI789",
	ConvertedOpportunityId: "006GHI789",
	Owner: { Name: "Diana" },
	CreatedDate: "2025-04-01T00:00:00.000+0000",
	LastModifiedDate: "2025-04-20T00:00:00.000+0000",
};

const nullLead: SfLead = {
	Id: "00QDEF456",
	FirstName: null,
	LastName: null,
	Company: null,
	Email: null,
	Phone: null,
	Title: null,
	Status: null,
	LeadSource: null,
	IsConverted: null,
	ConvertedAccountId: null,
	ConvertedContactId: null,
	ConvertedOpportunityId: null,
	Owner: null,
	CreatedDate: null,
	LastModifiedDate: null,
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("mapAccount", () => {
	it("maps a fully populated account", () => {
		const { rowId, row } = mapAccount(fullAccount);

		expect(rowId).toBe("001ABC123");
		expect(row.sf_id).toBe("001ABC123");
		expect(row.name).toBe("Acme Corp");
		expect(row.type).toBe("Customer");
		expect(row.industry).toBe("Technology");
		expect(row.website).toBe("https://acme.com");
		expect(row.phone).toBe("555-0100");
		expect(row.billing_city).toBe("San Francisco");
		expect(row.billing_state).toBe("CA");
		expect(row.billing_country).toBe("US");
		expect(row.annual_revenue).toBe(1_000_000);
		expect(row.number_of_employees).toBe(50);
		expect(row.owner_name).toBe("Alice");
		expect(row.created_date).toBe("2025-01-01T00:00:00.000+0000");
		expect(row.last_modified_date).toBe("2025-01-15T00:00:00.000+0000");
	});

	it("maps null fields gracefully", () => {
		const { rowId, row } = mapAccount(nullAccount);

		expect(rowId).toBe("001DEF456");
		expect(row.name).toBeNull();
		expect(row.type).toBeNull();
		expect(row.industry).toBeNull();
		expect(row.website).toBeNull();
		expect(row.phone).toBeNull();
		expect(row.billing_city).toBeNull();
		expect(row.billing_state).toBeNull();
		expect(row.billing_country).toBeNull();
		expect(row.annual_revenue).toBeNull();
		expect(row.number_of_employees).toBeNull();
		expect(row.owner_name).toBeNull();
		expect(row.created_date).toBeNull();
		expect(row.last_modified_date).toBeNull();
	});
});

describe("mapContact", () => {
	it("maps a fully populated contact", () => {
		const { rowId, row } = mapContact(fullContact);

		expect(rowId).toBe("003ABC123");
		expect(row.sf_id).toBe("003ABC123");
		expect(row.first_name).toBe("Jane");
		expect(row.last_name).toBe("Doe");
		expect(row.email).toBe("jane@acme.com");
		expect(row.phone).toBe("555-0200");
		expect(row.title).toBe("VP Engineering");
		expect(row.account_id).toBe("001ABC123");
		expect(row.account_name).toBe("Acme Corp");
		expect(row.mailing_city).toBe("New York");
		expect(row.mailing_state).toBe("NY");
		expect(row.mailing_country).toBe("US");
		expect(row.owner_name).toBe("Bob");
		expect(row.created_date).toBe("2025-02-01T00:00:00.000+0000");
		expect(row.last_modified_date).toBe("2025-02-10T00:00:00.000+0000");
	});

	it("maps null fields gracefully", () => {
		const { rowId, row } = mapContact(nullContact);

		expect(rowId).toBe("003DEF456");
		expect(row.first_name).toBeNull();
		expect(row.last_name).toBeNull();
		expect(row.email).toBeNull();
		expect(row.phone).toBeNull();
		expect(row.title).toBeNull();
		expect(row.account_id).toBeNull();
		expect(row.account_name).toBeNull();
		expect(row.mailing_city).toBeNull();
		expect(row.mailing_state).toBeNull();
		expect(row.mailing_country).toBeNull();
		expect(row.owner_name).toBeNull();
		expect(row.created_date).toBeNull();
		expect(row.last_modified_date).toBeNull();
	});
});

describe("mapOpportunity", () => {
	it("maps a fully populated opportunity", () => {
		const { rowId, row } = mapOpportunity(fullOpportunity);

		expect(rowId).toBe("006ABC123");
		expect(row.sf_id).toBe("006ABC123");
		expect(row.name).toBe("Acme Enterprise Deal");
		expect(row.stage_name).toBe("Negotiation");
		expect(row.amount).toBe(500_000);
		expect(row.close_date).toBe("2025-06-30");
		expect(row.probability).toBe(75);
		expect(row.account_id).toBe("001ABC123");
		expect(row.account_name).toBe("Acme Corp");
		expect(row.type).toBe("New Business");
		expect(row.lead_source).toBe("Web");
		expect(row.is_closed).toBe(false);
		expect(row.is_won).toBe(false);
		expect(row.owner_name).toBe("Charlie");
		expect(row.created_date).toBe("2025-03-01T00:00:00.000+0000");
		expect(row.last_modified_date).toBe("2025-03-15T00:00:00.000+0000");
	});

	it("maps null fields gracefully", () => {
		const { rowId, row } = mapOpportunity(nullOpportunity);

		expect(rowId).toBe("006DEF456");
		expect(row.name).toBeNull();
		expect(row.stage_name).toBeNull();
		expect(row.amount).toBeNull();
		expect(row.close_date).toBeNull();
		expect(row.probability).toBeNull();
		expect(row.account_id).toBeNull();
		expect(row.account_name).toBeNull();
		expect(row.type).toBeNull();
		expect(row.lead_source).toBeNull();
		expect(row.is_closed).toBeNull();
		expect(row.is_won).toBeNull();
		expect(row.owner_name).toBeNull();
		expect(row.created_date).toBeNull();
		expect(row.last_modified_date).toBeNull();
	});
});

describe("mapLead", () => {
	it("maps a fully populated lead", () => {
		const { rowId, row } = mapLead(fullLead);

		expect(rowId).toBe("00QABC123");
		expect(row.sf_id).toBe("00QABC123");
		expect(row.first_name).toBe("John");
		expect(row.last_name).toBe("Smith");
		expect(row.company).toBe("Widget Co");
		expect(row.email).toBe("john@widget.com");
		expect(row.phone).toBe("555-0300");
		expect(row.title).toBe("CTO");
		expect(row.status).toBe("Working");
		expect(row.lead_source).toBe("Conference");
		expect(row.is_converted).toBe(true);
		expect(row.converted_account_id).toBe("001GHI789");
		expect(row.converted_contact_id).toBe("003GHI789");
		expect(row.converted_opportunity_id).toBe("006GHI789");
		expect(row.owner_name).toBe("Diana");
		expect(row.created_date).toBe("2025-04-01T00:00:00.000+0000");
		expect(row.last_modified_date).toBe("2025-04-20T00:00:00.000+0000");
	});

	it("maps null fields gracefully", () => {
		const { rowId, row } = mapLead(nullLead);

		expect(rowId).toBe("00QDEF456");
		expect(row.first_name).toBeNull();
		expect(row.last_name).toBeNull();
		expect(row.company).toBeNull();
		expect(row.email).toBeNull();
		expect(row.phone).toBeNull();
		expect(row.title).toBeNull();
		expect(row.status).toBeNull();
		expect(row.lead_source).toBeNull();
		expect(row.is_converted).toBeNull();
		expect(row.converted_account_id).toBeNull();
		expect(row.converted_contact_id).toBeNull();
		expect(row.converted_opportunity_id).toBeNull();
		expect(row.owner_name).toBeNull();
		expect(row.created_date).toBeNull();
		expect(row.last_modified_date).toBeNull();
	});
});
