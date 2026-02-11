/** JSON Schema (draft-07) for BigQuery connector configuration. */
export const BIGQUERY_CONFIG_SCHEMA: Record<string, unknown> = {
	$schema: "http://json-schema.org/draft-07/schema#",
	type: "object",
	properties: {
		projectId: {
			type: "string",
			description: "GCP project ID.",
		},
		dataset: {
			type: "string",
			description: "BigQuery dataset name.",
		},
		keyFilename: {
			type: "string",
			description: "Path to service account JSON key file. Falls back to ADC when omitted.",
		},
		location: {
			type: "string",
			description: 'Dataset location (default "US").',
		},
	},
	required: ["projectId", "dataset"],
	additionalProperties: false,
};
