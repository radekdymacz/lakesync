/** JSON Schema (draft-07) for Jira Cloud connector configuration. */
export const JIRA_CONFIG_SCHEMA: Record<string, unknown> = {
	$schema: "http://json-schema.org/draft-07/schema#",
	type: "object",
	properties: {
		domain: {
			type: "string",
			description: "Jira Cloud domain (e.g. mycompany for mycompany.atlassian.net).",
		},
		email: {
			type: "string",
			description: "Email address for Basic auth.",
		},
		apiToken: {
			type: "string",
			description: "API token paired with the email.",
		},
		jql: {
			type: "string",
			description: "Optional JQL filter to scope issue polling.",
		},
		includeComments: {
			type: "boolean",
			description: "Whether to include comments (default true).",
		},
		includeProjects: {
			type: "boolean",
			description: "Whether to include projects (default true).",
		},
	},
	required: ["domain", "email", "apiToken"],
	additionalProperties: false,
};
