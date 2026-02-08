import type { SyncRulesConfig } from "./types";

/**
 * Create a pass-all sync rules configuration.
 *
 * Every delta reaches every client — equivalent to having no rules at all.
 * Useful for apps without multi-tenancy or per-user data isolation.
 */
export function createPassAllRules(): SyncRulesConfig {
	return {
		version: 1,
		buckets: [
			{
				name: "all",
				tables: [],
				filters: [],
			},
		],
	};
}

/**
 * Create user-scoped sync rules configuration.
 *
 * Filters rows by matching a configurable column against the JWT `sub` claim,
 * so each client only receives deltas belonging to the authenticated user.
 *
 * @param tables - Which tables to scope. Empty array means all tables.
 * @param userColumn - Column to match against `jwt:sub`. Defaults to `"user_id"`.
 */
export function createUserScopedRules(tables: string[], userColumn = "user_id"): SyncRulesConfig {
	return {
		version: 1,
		buckets: [
			{
				name: "user",
				tables,
				filters: [
					{
						column: userColumn,
						op: "eq",
						value: "jwt:sub",
					},
				],
			},
		],
	};
}
