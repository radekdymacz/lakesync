/** JSON Schema (draft-07) for database connector ingest configuration. */
export const DATABASE_INGEST_SCHEMA: Record<string, unknown> = {
	$schema: "http://json-schema.org/draft-07/schema#",
	type: "object",
	properties: {
		tables: {
			type: "array",
			description: "Tables to poll for changes.",
			items: {
				type: "object",
				properties: {
					table: { type: "string", description: "Target table name in LakeSync." },
					query: {
						type: "string",
						description: "SQL query to poll (must return rowId + data columns).",
					},
					rowIdColumn: {
						type: "string",
						description: 'Primary key column name (default "id").',
					},
					strategy: {
						oneOf: [
							{
								type: "object",
								properties: {
									type: { const: "cursor" },
									cursorColumn: { type: "string" },
									lookbackMs: { type: "number" },
								},
								required: ["type", "cursorColumn"],
							},
							{
								type: "object",
								properties: {
									type: { const: "diff" },
								},
								required: ["type"],
							},
						],
					},
				},
				required: ["table", "query", "strategy"],
			},
		},
		intervalMs: {
			type: "number",
			description: "Poll interval in milliseconds (default 10 000).",
		},
		chunkSize: {
			type: "number",
			description: "Deltas per push chunk (default 500).",
		},
		memoryBudgetBytes: {
			type: "number",
			description: "Approximate memory budget in bytes — triggers flush at 70%.",
		},
	},
	required: ["tables"],
	additionalProperties: false,
};

/** JSON Schema (draft-07) for API connector ingest configuration. */
export const API_INGEST_SCHEMA: Record<string, unknown> = {
	$schema: "http://json-schema.org/draft-07/schema#",
	type: "object",
	properties: {
		intervalMs: {
			type: "number",
			description: "Poll interval in milliseconds (default 10 000).",
		},
		chunkSize: {
			type: "number",
			description: "Deltas per push chunk (default 500).",
		},
		memoryBudgetBytes: {
			type: "number",
			description: "Approximate memory budget in bytes — triggers flush at 70%.",
		},
	},
	additionalProperties: false,
};
