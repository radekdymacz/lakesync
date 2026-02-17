/**
 * OpenAPI 3.1 specification for the LakeSync Gateway API.
 *
 * Exported as a plain TypeScript object so it can be:
 * 1. Served at `GET /v1/openapi.json` by gateway-worker and gateway-server
 * 2. Extracted at build time via `generateOpenApiJson()`
 */

// ---------------------------------------------------------------------------
// Reusable component schemas
// ---------------------------------------------------------------------------

const ColumnDeltaSchema = {
	type: "object",
	required: ["column", "value"],
	properties: {
		column: { type: "string", description: "Column name" },
		value: { description: "Serialisable JSON value (never undefined, use null)" },
	},
} as const;

const RowDeltaSchema = {
	type: "object",
	required: ["op", "table", "rowId", "clientId", "columns", "hlc", "deltaId"],
	properties: {
		op: {
			type: "string",
			enum: ["INSERT", "UPDATE", "DELETE"],
			description: "Delta operation type",
		},
		table: { type: "string", description: "Table name" },
		rowId: { type: "string", description: "Row identifier" },
		clientId: { type: "string", description: "Client identifier for LWW tiebreak" },
		columns: {
			type: "array",
			items: { $ref: "#/components/schemas/ColumnDelta" },
			description: "Changed columns (empty for DELETE)",
		},
		hlc: { type: "string", description: "HLC timestamp (branded bigint serialised as string)" },
		deltaId: { type: "string", description: "Deterministic SHA-256 identifier" },
	},
} as const;

const ErrorResponseSchema = {
	type: "object",
	required: ["error", "code"],
	properties: {
		error: { type: "string", description: "Human-readable error message" },
		code: {
			type: "string",
			enum: [
				"VALIDATION_ERROR",
				"SCHEMA_ERROR",
				"BACKPRESSURE_ERROR",
				"CLOCK_DRIFT",
				"AUTH_ERROR",
				"FORBIDDEN",
				"NOT_FOUND",
				"RATE_LIMITED",
				"ADAPTER_ERROR",
				"FLUSH_ERROR",
				"INTERNAL_ERROR",
			],
			description: "Machine-readable error code",
		},
		requestId: { type: "string", format: "uuid", description: "Unique request identifier" },
	},
} as const;

const TableSchemaSchema = {
	type: "object",
	required: ["table", "columns"],
	properties: {
		table: { type: "string", description: "Destination table name" },
		sourceTable: {
			type: "string",
			description: "Delta table name to match against (defaults to table)",
		},
		columns: {
			type: "array",
			items: {
				type: "object",
				required: ["name", "type"],
				properties: {
					name: { type: "string", description: "Column name" },
					type: {
						type: "string",
						enum: ["string", "number", "boolean", "json", "null"],
						description: "Column type",
					},
					references: {
						type: "object",
						properties: {
							table: { type: "string" },
							column: { type: "string" },
							cardinality: { type: "string", enum: ["many-to-one", "one-to-many"] },
						},
					},
				},
			},
		},
		primaryKey: {
			type: "array",
			items: { type: "string" },
			description: "Composite primary key columns",
		},
		softDelete: {
			type: "boolean",
			description: "Whether to soft-delete tombstoned rows (default: true)",
		},
		externalIdColumn: {
			type: "string",
			description: "Column for upsert resolution instead of primary key",
		},
	},
} as const;

const ActionSchema = {
	type: "object",
	required: ["actionId", "clientId", "hlc", "connector", "actionType", "params"],
	properties: {
		actionId: { type: "string", description: "Unique action identifier (SHA-256)" },
		clientId: { type: "string", description: "Client that initiated the action" },
		hlc: { type: "string", description: "HLC timestamp" },
		connector: { type: "string", description: "Target connector name" },
		actionType: { type: "string", description: "Action type within the connector" },
		params: {
			type: "object",
			additionalProperties: true,
			description: "Connector-specific parameters",
		},
		idempotencyKey: { type: "string", description: "Optional idempotency key" },
	},
} as const;

const ActionResultSchema = {
	type: "object",
	required: ["actionId", "data", "serverHlc"],
	properties: {
		actionId: { type: "string" },
		data: { type: "object", additionalProperties: true },
		serverHlc: { type: "string" },
	},
} as const;

const ActionErrorResultSchema = {
	type: "object",
	required: ["actionId", "code", "message", "retryable"],
	properties: {
		actionId: { type: "string" },
		code: { type: "string" },
		message: { type: "string" },
		retryable: { type: "boolean" },
	},
} as const;

const ConnectorConfigSchema = {
	type: "object",
	required: ["type", "name"],
	properties: {
		type: {
			type: "string",
			enum: ["postgres", "mysql", "bigquery", "jira", "salesforce"],
			description: "Connector type identifier",
		},
		name: { type: "string", description: "Unique connector name" },
		ingest: {
			type: "object",
			description: "Optional ingest polling configuration",
			properties: {
				tables: {
					type: "array",
					items: {
						type: "object",
						required: ["table", "query", "rowIdColumn"],
						properties: {
							table: { type: "string" },
							query: { type: "string" },
							rowIdColumn: { type: "string" },
							strategy: { type: "string", enum: ["cursor", "diff"] },
						},
					},
				},
				pollIntervalMs: { type: "integer" },
				chunkSize: { type: "integer" },
				memoryBudgetBytes: { type: "integer" },
			},
		},
	},
	additionalProperties: true,
} as const;

const SyncRulesSchema = {
	type: "object",
	required: ["buckets"],
	properties: {
		buckets: {
			type: "array",
			items: {
				type: "object",
				required: ["name", "tables"],
				properties: {
					name: { type: "string" },
					tables: { type: "array", items: { type: "string" } },
					filters: {
						type: "array",
						items: {
							type: "object",
							required: ["column", "op", "value"],
							properties: {
								column: { type: "string" },
								op: { type: "string", enum: ["eq", "neq", "in", "gt", "lt", "gte", "lte"] },
								value: {},
							},
						},
					},
				},
			},
		},
	},
} as const;

// ---------------------------------------------------------------------------
// Security schemes
// ---------------------------------------------------------------------------

const securitySchemes = {
	BearerAuth: {
		type: "http",
		scheme: "bearer",
		bearerFormat: "JWT",
		description:
			"JWT token signed with HMAC-SHA256. Claims must include `sub` (client ID), `gatewayId`, and `role` (client | admin).",
	},
} as const;

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

function errorResponse(description: string, statusCode: string) {
	return {
		[statusCode]: {
			description,
			content: {
				"application/json": {
					schema: { $ref: "#/components/schemas/ErrorResponse" },
				},
			},
		},
	};
}

const commonErrors = {
	...errorResponse("Missing or invalid Bearer token", "401"),
	...errorResponse("Insufficient permissions", "403"),
};

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

const paths = {
	"/health": {
		get: {
			summary: "Health check",
			description: "Returns gateway health status. Does not require authentication.",
			operationId: "healthCheck",
			tags: ["Health"],
			security: [],
			responses: {
				"200": {
					description: "Gateway is healthy",
					content: {
						"application/json": {
							schema: {
								type: "object",
								properties: {
									status: { type: "string", enum: ["ok"] },
								},
							},
						},
					},
				},
			},
		},
	},

	"/v1/connectors/types": {
		get: {
			summary: "List available connector types",
			description:
				"Returns static metadata about all supported connector types. Does not require authentication.",
			operationId: "listConnectorTypes",
			tags: ["Connectors"],
			security: [],
			responses: {
				"200": {
					description: "List of connector type descriptors",
					content: {
						"application/json": {
							schema: {
								type: "array",
								items: {
									type: "object",
									properties: {
										type: { type: "string" },
										label: { type: "string" },
										description: { type: "string" },
									},
								},
							},
						},
					},
				},
			},
		},
	},

	"/v1/sync/{gatewayId}/push": {
		post: {
			summary: "Push deltas",
			description:
				"Push local deltas to the gateway. All deltas are validated atomically — if any delta fails validation, no deltas are accepted.",
			operationId: "pushDeltas",
			tags: ["Sync"],
			security: [{ BearerAuth: [] }],
			parameters: [{ $ref: "#/components/parameters/GatewayId" }],
			requestBody: {
				required: true,
				content: {
					"application/json": {
						schema: {
							type: "object",
							required: ["clientId", "deltas", "lastSeenHlc"],
							properties: {
								clientId: { type: "string", description: "Client identifier" },
								deltas: {
									type: "array",
									items: { $ref: "#/components/schemas/RowDelta" },
									maxItems: 1000,
									description: "Deltas to push (max 1000)",
								},
								lastSeenHlc: { type: "string", description: "Client's last-seen HLC timestamp" },
							},
						},
					},
				},
			},
			responses: {
				"200": {
					description: "Push accepted",
					content: {
						"application/json": {
							schema: {
								type: "object",
								properties: {
									serverHlc: { type: "string", description: "Server HLC after processing" },
									accepted: { type: "integer", description: "Number of deltas accepted" },
									deltas: {
										type: "array",
										items: { $ref: "#/components/schemas/RowDelta" },
										description: "Deltas actually ingested (excludes idempotent re-pushes)",
									},
								},
							},
						},
					},
				},
				...errorResponse("Validation error (invalid delta, schema mismatch, clock drift)", "400"),
				...commonErrors,
				...errorResponse("Backpressure — buffer full, try again later", "429"),
			},
		},
	},

	"/v1/sync/{gatewayId}/pull": {
		get: {
			summary: "Pull deltas",
			description: "Pull remote deltas from the gateway since a given HLC timestamp.",
			operationId: "pullDeltas",
			tags: ["Sync"],
			security: [{ BearerAuth: [] }],
			parameters: [
				{ $ref: "#/components/parameters/GatewayId" },
				{
					name: "since",
					in: "query",
					required: true,
					schema: { type: "string" },
					description: "Return deltas with HLC strictly after this value",
				},
				{
					name: "clientId",
					in: "query",
					required: true,
					schema: { type: "string" },
					description: "Client identifier",
				},
				{
					name: "limit",
					in: "query",
					required: false,
					schema: { type: "integer", minimum: 1, maximum: 10000, default: 1000 },
					description: "Maximum number of deltas to return",
				},
				{
					name: "source",
					in: "query",
					required: false,
					schema: { type: "string" },
					description: "Named source adapter for adapter-sourced pulls",
				},
			],
			responses: {
				"200": {
					description: "Pull response",
					content: {
						"application/json": {
							schema: {
								type: "object",
								properties: {
									deltas: {
										type: "array",
										items: { $ref: "#/components/schemas/RowDelta" },
									},
									serverHlc: { type: "string", description: "Current server HLC" },
									hasMore: { type: "boolean", description: "Whether more deltas are available" },
								},
							},
						},
					},
				},
				...errorResponse("Invalid parameters", "400"),
				...commonErrors,
				...errorResponse("Named source adapter not found", "404"),
			},
		},
	},

	"/v1/sync/{gatewayId}/action": {
		post: {
			summary: "Execute actions",
			description: "Execute imperative actions against registered connectors.",
			operationId: "executeActions",
			tags: ["Actions"],
			security: [{ BearerAuth: [] }],
			parameters: [{ $ref: "#/components/parameters/GatewayId" }],
			requestBody: {
				required: true,
				content: {
					"application/json": {
						schema: {
							type: "object",
							required: ["clientId", "actions"],
							properties: {
								clientId: { type: "string", description: "Client identifier" },
								actions: {
									type: "array",
									items: { $ref: "#/components/schemas/Action" },
									description: "Actions to execute",
								},
							},
						},
					},
				},
			},
			responses: {
				"200": {
					description: "Action results",
					content: {
						"application/json": {
							schema: {
								type: "object",
								properties: {
									results: {
										type: "array",
										items: {
											oneOf: [
												{ $ref: "#/components/schemas/ActionResult" },
												{ $ref: "#/components/schemas/ActionErrorResult" },
											],
										},
									},
									serverHlc: { type: "string" },
								},
							},
						},
					},
				},
				...errorResponse("Invalid action body", "400"),
				...commonErrors,
			},
		},
	},

	"/v1/sync/{gatewayId}/actions": {
		get: {
			summary: "Discover available actions",
			description: "List all registered connectors and their supported action types.",
			operationId: "discoverActions",
			tags: ["Actions"],
			security: [{ BearerAuth: [] }],
			parameters: [{ $ref: "#/components/parameters/GatewayId" }],
			responses: {
				"200": {
					description: "Action discovery response",
					content: {
						"application/json": {
							schema: {
								type: "object",
								properties: {
									connectors: {
										type: "object",
										additionalProperties: {
											type: "array",
											items: {
												type: "object",
												properties: {
													actionType: { type: "string" },
													description: { type: "string" },
													paramsSchema: { type: "object", additionalProperties: true },
												},
											},
										},
									},
								},
							},
						},
					},
				},
				...commonErrors,
			},
		},
	},

	"/v1/sync/{gatewayId}/checkpoint": {
		get: {
			summary: "Download checkpoint",
			description:
				"Download a proto-encoded checkpoint for initial sync. Chunks contain all rows; sync rules filtering is applied at serve time via JWT claims.",
			operationId: "getCheckpoint",
			tags: ["Sync"],
			security: [{ BearerAuth: [] }],
			parameters: [{ $ref: "#/components/parameters/GatewayId" }],
			responses: {
				"200": {
					description: "Checkpoint data (proto-encoded binary or JSON)",
					content: {
						"application/octet-stream": {
							schema: { type: "string", format: "binary" },
						},
						"application/json": {
							schema: {
								type: "object",
								description: "Checkpoint metadata and chunks",
							},
						},
					},
				},
				...commonErrors,
			},
		},
	},

	"/v1/sync/{gatewayId}/ws": {
		get: {
			summary: "WebSocket connection",
			description:
				"Upgrade to WebSocket for real-time sync. Binary protobuf with tag-based framing (0x01=push, 0x02=pull, 0x03=broadcast). Token via Authorization header or `?token=` query param.",
			operationId: "webSocket",
			tags: ["Sync"],
			security: [{ BearerAuth: [] }],
			parameters: [
				{ $ref: "#/components/parameters/GatewayId" },
				{
					name: "token",
					in: "query",
					required: false,
					schema: { type: "string" },
					description:
						"Alternative JWT token for WebSocket auth (when Authorization header is not supported)",
				},
			],
			responses: {
				"101": {
					description: "WebSocket upgrade successful",
				},
				...commonErrors,
			},
		},
	},

	"/v1/admin/flush/{gatewayId}": {
		post: {
			summary: "Trigger flush",
			description: "Flush the delta buffer to the configured adapter. Requires admin role.",
			operationId: "flushBuffer",
			tags: ["Admin"],
			security: [{ BearerAuth: [] }],
			parameters: [{ $ref: "#/components/parameters/GatewayId" }],
			responses: {
				"200": {
					description: "Flush completed",
					content: {
						"application/json": {
							schema: {
								type: "object",
								properties: {
									flushed: { type: "boolean", enum: [true] },
								},
							},
						},
					},
				},
				...commonErrors,
				...errorResponse("Flush failed", "500"),
			},
		},
	},

	"/v1/admin/schema/{gatewayId}": {
		post: {
			summary: "Save table schema",
			description:
				"Register or update a table schema for delta validation. Identifiers must match `^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`. Requires admin role.",
			operationId: "saveSchema",
			tags: ["Admin"],
			security: [{ BearerAuth: [] }],
			parameters: [{ $ref: "#/components/parameters/GatewayId" }],
			requestBody: {
				required: true,
				content: {
					"application/json": {
						schema: { $ref: "#/components/schemas/TableSchema" },
					},
				},
			},
			responses: {
				"200": {
					description: "Schema saved",
					content: {
						"application/json": {
							schema: {
								type: "object",
								properties: {
									saved: { type: "boolean", enum: [true] },
								},
							},
						},
					},
				},
				...errorResponse("Invalid schema", "400"),
				...commonErrors,
			},
		},
	},

	"/v1/admin/sync-rules/{gatewayId}": {
		post: {
			summary: "Save sync rules",
			description: "Register or update sync rules for filtered pulls. Requires admin role.",
			operationId: "saveSyncRules",
			tags: ["Admin"],
			security: [{ BearerAuth: [] }],
			parameters: [{ $ref: "#/components/parameters/GatewayId" }],
			requestBody: {
				required: true,
				content: {
					"application/json": {
						schema: { $ref: "#/components/schemas/SyncRules" },
					},
				},
			},
			responses: {
				"200": {
					description: "Sync rules saved",
					content: {
						"application/json": {
							schema: {
								type: "object",
								properties: {
									saved: { type: "boolean", enum: [true] },
								},
							},
						},
					},
				},
				...errorResponse("Invalid sync rules", "400"),
				...commonErrors,
			},
		},
	},

	"/v1/admin/connectors/{gatewayId}": {
		post: {
			summary: "Register connector",
			description:
				"Register a new connector and optionally start its ingest poller. Requires admin role.",
			operationId: "registerConnector",
			tags: ["Admin", "Connectors"],
			security: [{ BearerAuth: [] }],
			parameters: [{ $ref: "#/components/parameters/GatewayId" }],
			requestBody: {
				required: true,
				content: {
					"application/json": {
						schema: { $ref: "#/components/schemas/ConnectorConfig" },
					},
				},
			},
			responses: {
				"200": {
					description: "Connector registered",
					content: {
						"application/json": {
							schema: {
								type: "object",
								properties: {
									registered: { type: "boolean", enum: [true] },
									name: { type: "string" },
								},
							},
						},
					},
				},
				...errorResponse("Invalid connector config", "400"),
				...commonErrors,
				...errorResponse("Connector name already exists", "409"),
			},
		},
		get: {
			summary: "List connectors",
			description: "List all registered connectors for this gateway. Requires admin role.",
			operationId: "listConnectors",
			tags: ["Admin", "Connectors"],
			security: [{ BearerAuth: [] }],
			parameters: [{ $ref: "#/components/parameters/GatewayId" }],
			responses: {
				"200": {
					description: "List of connectors",
					content: {
						"application/json": {
							schema: {
								type: "array",
								items: {
									type: "object",
									properties: {
										name: { type: "string" },
										type: { type: "string" },
										hasIngest: { type: "boolean" },
									},
								},
							},
						},
					},
				},
				...commonErrors,
			},
		},
	},

	"/v1/admin/connectors/{gatewayId}/{connectorName}": {
		delete: {
			summary: "Unregister connector",
			description: "Remove a registered connector and stop its poller. Requires admin role.",
			operationId: "unregisterConnector",
			tags: ["Admin", "Connectors"],
			security: [{ BearerAuth: [] }],
			parameters: [
				{ $ref: "#/components/parameters/GatewayId" },
				{
					name: "connectorName",
					in: "path",
					required: true,
					schema: { type: "string" },
					description: "Connector name to remove",
				},
			],
			responses: {
				"200": {
					description: "Connector unregistered",
					content: {
						"application/json": {
							schema: {
								type: "object",
								properties: {
									unregistered: { type: "boolean", enum: [true] },
									name: { type: "string" },
								},
							},
						},
					},
				},
				...commonErrors,
				...errorResponse("Connector not found", "404"),
			},
		},
	},

	"/v1/admin/metrics/{gatewayId}": {
		get: {
			summary: "Get gateway metrics",
			description: "Retrieve buffer statistics and metrics for this gateway. Requires admin role.",
			operationId: "getMetrics",
			tags: ["Admin"],
			security: [{ BearerAuth: [] }],
			parameters: [{ $ref: "#/components/parameters/GatewayId" }],
			responses: {
				"200": {
					description: "Gateway metrics",
					content: {
						"application/json": {
							schema: {
								type: "object",
								properties: {
									logSize: { type: "integer", description: "Number of deltas in buffer" },
									indexSize: { type: "integer", description: "Number of indexed entries" },
									byteSize: { type: "integer", description: "Estimated buffer size in bytes" },
								},
								additionalProperties: true,
							},
						},
					},
				},
				...commonErrors,
			},
		},
	},
} as const;

// ---------------------------------------------------------------------------
// Full specification
// ---------------------------------------------------------------------------

/**
 * The complete OpenAPI 3.1 specification for the LakeSync Gateway API.
 *
 * This is a plain object — no runtime dependencies, no side effects.
 */
export const openApiSpec = {
	openapi: "3.1.0",
	info: {
		title: "LakeSync Gateway API",
		version: "1.0.0",
		description:
			"Declarative data sync engine. Push and pull deltas, execute actions, and manage gateway configuration.",
		license: {
			name: "Apache-2.0",
			url: "https://www.apache.org/licenses/LICENSE-2.0",
		},
	},
	servers: [
		{
			url: "https://{host}",
			description: "LakeSync Gateway",
			variables: {
				host: {
					default: "localhost:3000",
					description: "Gateway host and port",
				},
			},
		},
	],
	tags: [
		{ name: "Health", description: "Health and readiness checks" },
		{ name: "Sync", description: "Delta push, pull, checkpoint, and WebSocket" },
		{ name: "Actions", description: "Imperative action execution and discovery" },
		{ name: "Admin", description: "Gateway administration (requires admin role)" },
		{ name: "Connectors", description: "Connector type discovery and management" },
	],
	paths,
	components: {
		securitySchemes,
		parameters: {
			GatewayId: {
				name: "gatewayId",
				in: "path",
				required: true,
				schema: { type: "string" },
				description: "Gateway identifier (must match JWT `gatewayId` claim)",
			},
		},
		schemas: {
			ColumnDelta: ColumnDeltaSchema,
			RowDelta: RowDeltaSchema,
			ErrorResponse: ErrorResponseSchema,
			TableSchema: TableSchemaSchema,
			Action: ActionSchema,
			ActionResult: ActionResultSchema,
			ActionErrorResult: ActionErrorResultSchema,
			ConnectorConfig: ConnectorConfigSchema,
			SyncRules: SyncRulesSchema,
		},
	},
	security: [{ BearerAuth: [] }],
};

/**
 * Generate the OpenAPI spec as a JSON string.
 * Useful for build-time extraction or serving at `/v1/openapi.json`.
 */
export function generateOpenApiJson(): string {
	return JSON.stringify(openApiSpec, null, 2);
}
