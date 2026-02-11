/** JSON Schema (draft-07) for MySQL connector configuration. */
export const MYSQL_CONFIG_SCHEMA: Record<string, unknown> = {
	$schema: "http://json-schema.org/draft-07/schema#",
	type: "object",
	properties: {
		connectionString: {
			type: "string",
			description: "MySQL connection string (e.g. mysql://user:pass@host/db).",
		},
	},
	required: ["connectionString"],
	additionalProperties: false,
};
