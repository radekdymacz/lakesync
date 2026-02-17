-- Control plane schema: organisations, members, gateways, API keys

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
