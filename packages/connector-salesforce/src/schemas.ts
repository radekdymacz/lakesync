// ---------------------------------------------------------------------------
// Salesforce Table Schemas — column definitions for each synced entity
// ---------------------------------------------------------------------------

import type { TableSchema } from "@lakesync/core";

/** Column helper — all Salesforce columns are mapped to string. */
function textCol(name: string): { name: string; type: "string" } {
	return { name, type: "string" };
}

/** Table schemas for all Salesforce entity types synced by the connector. */
export const SALESFORCE_TABLE_SCHEMAS: ReadonlyArray<TableSchema> = [
	{
		table: "sf_accounts",
		columns: [
			textCol("sf_id"),
			textCol("name"),
			textCol("type"),
			textCol("industry"),
			textCol("website"),
			textCol("phone"),
			textCol("billing_city"),
			textCol("billing_state"),
			textCol("billing_country"),
			textCol("annual_revenue"),
			textCol("number_of_employees"),
			textCol("owner_name"),
			textCol("created_date"),
			textCol("last_modified_date"),
		],
	},
	{
		table: "sf_contacts",
		columns: [
			textCol("sf_id"),
			textCol("first_name"),
			textCol("last_name"),
			textCol("email"),
			textCol("phone"),
			textCol("title"),
			{
				name: "account_id",
				type: "string",
				references: { table: "sf_accounts", column: "sf_id", cardinality: "many-to-one" },
			},
			textCol("account_name"),
			textCol("mailing_city"),
			textCol("mailing_state"),
			textCol("mailing_country"),
			textCol("owner_name"),
			textCol("created_date"),
			textCol("last_modified_date"),
		],
	},
	{
		table: "sf_opportunities",
		columns: [
			textCol("sf_id"),
			textCol("name"),
			textCol("stage_name"),
			textCol("amount"),
			textCol("close_date"),
			textCol("probability"),
			{
				name: "account_id",
				type: "string",
				references: { table: "sf_accounts", column: "sf_id", cardinality: "many-to-one" },
			},
			textCol("account_name"),
			textCol("type"),
			textCol("lead_source"),
			textCol("is_closed"),
			textCol("is_won"),
			textCol("owner_name"),
			textCol("created_date"),
			textCol("last_modified_date"),
		],
	},
	{
		table: "sf_leads",
		columns: [
			textCol("sf_id"),
			textCol("first_name"),
			textCol("last_name"),
			textCol("company"),
			textCol("email"),
			textCol("phone"),
			textCol("title"),
			textCol("status"),
			textCol("lead_source"),
			textCol("is_converted"),
			{
				name: "converted_account_id",
				type: "string",
				references: { table: "sf_accounts", column: "sf_id", cardinality: "many-to-one" },
			},
			{
				name: "converted_contact_id",
				type: "string",
				references: { table: "sf_contacts", column: "sf_id", cardinality: "many-to-one" },
			},
			{
				name: "converted_opportunity_id",
				type: "string",
				references: { table: "sf_opportunities", column: "sf_id", cardinality: "many-to-one" },
			},
			textCol("owner_name"),
			textCol("created_date"),
			textCol("last_modified_date"),
		],
	},
];
