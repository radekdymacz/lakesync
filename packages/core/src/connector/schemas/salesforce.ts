/** JSON Schema (draft-07) for Salesforce CRM connector configuration. */
export const SALESFORCE_CONFIG_SCHEMA: Record<string, unknown> = {
	$schema: "http://json-schema.org/draft-07/schema#",
	type: "object",
	properties: {
		instanceUrl: {
			type: "string",
			description: "Salesforce instance URL (e.g. https://mycompany.salesforce.com).",
		},
		clientId: {
			type: "string",
			description: "Connected App consumer key.",
		},
		clientSecret: {
			type: "string",
			description: "Connected App consumer secret.",
		},
		username: {
			type: "string",
			description: "Salesforce username.",
		},
		password: {
			type: "string",
			description: "Salesforce password + security token concatenated.",
		},
		apiVersion: {
			type: "string",
			description: 'REST API version (default "v62.0").',
		},
		isSandbox: {
			type: "boolean",
			description: "Use test.salesforce.com for auth (default false).",
		},
		soqlFilter: {
			type: "string",
			description: "Optional WHERE clause fragment appended to all SOQL queries.",
		},
		includeAccounts: {
			type: "boolean",
			description: "Whether to include Account objects (default true).",
		},
		includeContacts: {
			type: "boolean",
			description: "Whether to include Contact objects (default true).",
		},
		includeOpportunities: {
			type: "boolean",
			description: "Whether to include Opportunity objects (default true).",
		},
		includeLeads: {
			type: "boolean",
			description: "Whether to include Lead objects (default true).",
		},
	},
	required: ["instanceUrl", "clientId", "clientSecret", "username", "password"],
	additionalProperties: false,
};
