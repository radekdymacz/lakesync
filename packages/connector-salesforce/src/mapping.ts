// ---------------------------------------------------------------------------
// Salesforce Entity → Flat LakeSync Row Mapping
// ---------------------------------------------------------------------------

import type { SfAccount, SfContact, SfLead, SfOpportunity } from "./types";

/**
 * Map a Salesforce Account to a flat row for the `sf_accounts` table.
 *
 * The row ID is the Salesforce record Id.
 */
export function mapAccount(account: SfAccount): { rowId: string; row: Record<string, unknown> } {
	return {
		rowId: account.Id,
		row: {
			sf_id: account.Id,
			name: account.Name ?? null,
			type: account.Type ?? null,
			industry: account.Industry ?? null,
			website: account.Website ?? null,
			phone: account.Phone ?? null,
			billing_city: account.BillingCity ?? null,
			billing_state: account.BillingState ?? null,
			billing_country: account.BillingCountry ?? null,
			annual_revenue: account.AnnualRevenue ?? null,
			number_of_employees: account.NumberOfEmployees ?? null,
			owner_name: account.Owner?.Name ?? null,
			created_date: account.CreatedDate ?? null,
			last_modified_date: account.LastModifiedDate ?? null,
		},
	};
}

/**
 * Map a Salesforce Contact to a flat row for the `sf_contacts` table.
 *
 * The row ID is the Salesforce record Id.
 */
export function mapContact(contact: SfContact): { rowId: string; row: Record<string, unknown> } {
	return {
		rowId: contact.Id,
		row: {
			sf_id: contact.Id,
			first_name: contact.FirstName ?? null,
			last_name: contact.LastName ?? null,
			email: contact.Email ?? null,
			phone: contact.Phone ?? null,
			title: contact.Title ?? null,
			account_id: contact.AccountId ?? null,
			account_name: contact.Account?.Name ?? null,
			mailing_city: contact.MailingCity ?? null,
			mailing_state: contact.MailingState ?? null,
			mailing_country: contact.MailingCountry ?? null,
			owner_name: contact.Owner?.Name ?? null,
			created_date: contact.CreatedDate ?? null,
			last_modified_date: contact.LastModifiedDate ?? null,
		},
	};
}

/**
 * Map a Salesforce Opportunity to a flat row for the `sf_opportunities` table.
 *
 * The row ID is the Salesforce record Id.
 */
export function mapOpportunity(opportunity: SfOpportunity): {
	rowId: string;
	row: Record<string, unknown>;
} {
	return {
		rowId: opportunity.Id,
		row: {
			sf_id: opportunity.Id,
			name: opportunity.Name ?? null,
			stage_name: opportunity.StageName ?? null,
			amount: opportunity.Amount ?? null,
			close_date: opportunity.CloseDate ?? null,
			probability: opportunity.Probability ?? null,
			account_id: opportunity.AccountId ?? null,
			account_name: opportunity.Account?.Name ?? null,
			type: opportunity.Type ?? null,
			lead_source: opportunity.LeadSource ?? null,
			is_closed: opportunity.IsClosed ?? null,
			is_won: opportunity.IsWon ?? null,
			owner_name: opportunity.Owner?.Name ?? null,
			created_date: opportunity.CreatedDate ?? null,
			last_modified_date: opportunity.LastModifiedDate ?? null,
		},
	};
}

/**
 * Map a Salesforce Lead to a flat row for the `sf_leads` table.
 *
 * The row ID is the Salesforce record Id.
 */
export function mapLead(lead: SfLead): { rowId: string; row: Record<string, unknown> } {
	return {
		rowId: lead.Id,
		row: {
			sf_id: lead.Id,
			first_name: lead.FirstName ?? null,
			last_name: lead.LastName ?? null,
			company: lead.Company ?? null,
			email: lead.Email ?? null,
			phone: lead.Phone ?? null,
			title: lead.Title ?? null,
			status: lead.Status ?? null,
			lead_source: lead.LeadSource ?? null,
			is_converted: lead.IsConverted ?? null,
			converted_account_id: lead.ConvertedAccountId ?? null,
			converted_contact_id: lead.ConvertedContactId ?? null,
			converted_opportunity_id: lead.ConvertedOpportunityId ?? null,
			owner_name: lead.Owner?.Name ?? null,
			created_date: lead.CreatedDate ?? null,
			last_modified_date: lead.LastModifiedDate ?? null,
		},
	};
}
