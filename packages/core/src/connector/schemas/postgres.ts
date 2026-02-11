/** JSON Schema (draft-07) for PostgreSQL connector configuration. */
export const POSTGRES_CONFIG_SCHEMA: Record<string, unknown> = {
	$schema: "http://json-schema.org/draft-07/schema#",
	type: "object",
	properties: {
		connectionString: {
			type: "string",
			description: "PostgreSQL connection string (e.g. postgres://user:pass@host/db).",
		},
	},
	required: ["connectionString"],
	additionalProperties: false,
};
