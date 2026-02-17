import { Pool, type PoolConfig } from "pg";

export interface PgPoolConfig {
	readonly connectionString: string;
	readonly poolMax?: number;
	readonly idleTimeoutMs?: number;
	readonly connectionTimeoutMs?: number;
}

/** Create a pg Pool from configuration */
export function createPool(config: PgPoolConfig): Pool {
	const poolConfig: PoolConfig = {
		connectionString: config.connectionString,
		max: config.poolMax ?? 10,
		idleTimeoutMillis: config.idleTimeoutMs ?? 10_000,
		connectionTimeoutMillis: config.connectionTimeoutMs ?? 30_000,
	};
	return new Pool(poolConfig);
}

const INIT_SQL = `
CREATE TABLE IF NOT EXISTS organisations (
	id TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	slug TEXT NOT NULL UNIQUE,
	plan TEXT NOT NULL DEFAULT 'free',
	stripe_customer_id TEXT,
	stripe_subscription_id TEXT,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_organisations_slug ON organisations (slug);

CREATE TABLE IF NOT EXISTS org_members (
	org_id TEXT NOT NULL REFERENCES organisations(id) ON DELETE CASCADE,
	user_id TEXT NOT NULL,
	role TEXT NOT NULL DEFAULT 'member',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	PRIMARY KEY (org_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_org_members_user ON org_members (user_id);

CREATE TABLE IF NOT EXISTS gateways (
	id TEXT PRIMARY KEY,
	org_id TEXT NOT NULL REFERENCES organisations(id) ON DELETE CASCADE,
	name TEXT NOT NULL,
	region TEXT,
	status TEXT NOT NULL DEFAULT 'active',
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_gateways_org ON gateways (org_id);

CREATE TABLE IF NOT EXISTS api_keys (
	id TEXT PRIMARY KEY,
	org_id TEXT NOT NULL REFERENCES organisations(id) ON DELETE CASCADE,
	gateway_id TEXT REFERENCES gateways(id) ON DELETE CASCADE,
	name TEXT NOT NULL,
	key_hash TEXT NOT NULL UNIQUE,
	key_prefix TEXT NOT NULL,
	role TEXT NOT NULL DEFAULT 'client',
	scopes JSONB,
	expires_at TIMESTAMPTZ,
	last_used_at TIMESTAMPTZ,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_org ON api_keys (org_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys (key_hash);

CREATE TABLE IF NOT EXISTS audit_events (
	id TEXT PRIMARY KEY,
	org_id TEXT NOT NULL,
	actor_id TEXT NOT NULL,
	actor_type TEXT NOT NULL,
	action TEXT NOT NULL,
	resource TEXT NOT NULL,
	metadata JSONB,
	ip_address TEXT,
	timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_events_org_ts ON audit_events (org_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_audit_events_action ON audit_events (org_id, action, timestamp DESC);

CREATE TABLE IF NOT EXISTS usage_events (
	gateway_id TEXT NOT NULL,
	org_id TEXT,
	event_type TEXT NOT NULL,
	count BIGINT NOT NULL DEFAULT 0,
	window_start TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (gateway_id, event_type, window_start)
);

CREATE INDEX IF NOT EXISTS idx_usage_events_org_window ON usage_events (org_id, window_start);
CREATE INDEX IF NOT EXISTS idx_usage_events_org_type ON usage_events (org_id, event_type, window_start);
`;

/** Run the initial migration against the given pool */
export async function runMigrations(pool: Pool): Promise<void> {
	await pool.query(INIT_SQL);
}
