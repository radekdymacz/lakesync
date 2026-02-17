import { vi } from "vitest";

/** Create a mock pg Pool with configurable query responses */
export function createMockPool() {
	const queryResults: Array<{ rows: Record<string, unknown>[]; rowCount: number }> = [];
	let queryCallIndex = 0;

	const mockQuery = vi.fn().mockImplementation(() => {
		if (queryCallIndex < queryResults.length) {
			return Promise.resolve(queryResults[queryCallIndex++]);
		}
		return Promise.resolve({ rows: [], rowCount: 0 });
	});

	const mockEnd = vi.fn().mockResolvedValue(undefined);

	const pool = {
		query: mockQuery,
		end: mockEnd,
	};

	return {
		pool,
		mockQuery,
		mockEnd,
		/** Queue a result for the next query call */
		queueResult(rows: Record<string, unknown>[], rowCount?: number) {
			queryResults.push({ rows, rowCount: rowCount ?? rows.length });
		},
		/** Reset call index for fresh test */
		reset() {
			queryCallIndex = 0;
			queryResults.length = 0;
			mockQuery.mockClear();
		},
	};
}

/** Build a mock organisation row as it would come from Postgres */
export function mockOrgRow(
	overrides: Partial<Record<string, unknown>> = {},
): Record<string, unknown> {
	return {
		id: "org_abc123",
		name: "Test Org",
		slug: "test-org",
		plan: "free",
		stripe_customer_id: null,
		stripe_subscription_id: null,
		created_at: new Date("2025-01-01").toISOString(),
		updated_at: new Date("2025-01-01").toISOString(),
		...overrides,
	};
}

/** Build a mock gateway row as it would come from Postgres */
export function mockGatewayRow(
	overrides: Partial<Record<string, unknown>> = {},
): Record<string, unknown> {
	return {
		id: "gw_abc123",
		org_id: "org_abc123",
		name: "Test Gateway",
		region: null,
		status: "active",
		created_at: new Date("2025-01-01").toISOString(),
		updated_at: new Date("2025-01-01").toISOString(),
		...overrides,
	};
}

/** Build a mock API key row as it would come from Postgres */
export function mockApiKeyRow(
	overrides: Partial<Record<string, unknown>> = {},
): Record<string, unknown> {
	return {
		id: "key_abc123",
		org_id: "org_abc123",
		gateway_id: null,
		name: "Test Key",
		key_hash: "abc123hash",
		key_prefix: "lk_ABCDEFgh",
		role: "client",
		scopes: null,
		expires_at: null,
		last_used_at: null,
		created_at: new Date("2025-01-01").toISOString(),
		...overrides,
	};
}

/** Build a mock member row as it would come from Postgres */
export function mockMemberRow(
	overrides: Partial<Record<string, unknown>> = {},
): Record<string, unknown> {
	return {
		org_id: "org_abc123",
		user_id: "user_abc123",
		role: "member",
		created_at: new Date("2025-01-01").toISOString(),
		...overrides,
	};
}

/** Simulate a Postgres unique violation error (code 23505) */
export function duplicateKeyError(detail = "duplicate"): Error {
	const error = new Error(detail) as Error & { code: string };
	error.code = "23505";
	return error;
}
