# Security Model

This document describes the authentication, authorisation, and threat model for LakeSync deployments.

## Authentication

### JWT (HS256)

All sync routes (`/sync/:gatewayId/push`, `/sync/:gatewayId/pull`) require a valid JWT in the `Authorization: Bearer <token>` header.

**Token format**: Standard JWT with HS256 (HMAC-SHA256) signature.

**Required claims**:

| Claim | Type | Description |
|-------|------|-------------|
| `sub` | `string` | Client identifier. Maps to `clientId` in deltas. |
| `gw` | `string` | Authorised gateway ID. The token is only valid for this gateway. |
| `exp` | `number` | Expiry time (Unix seconds). Tokens past expiry are rejected. |

**Verification flow**:
1. Extract `Bearer <token>` from the `Authorization` header
2. Decode header — must be `{"alg":"HS256","typ":"JWT"}`
3. Verify HMAC-SHA256 signature using the `JWT_SECRET` environment variable
4. Check `exp` against current server time
5. Extract `sub` (clientId) and `gw` (gatewayId) claims

**Implementation**: Uses the Web Crypto API exclusively — no external JWT libraries. Compatible with Cloudflare Workers, Deno, and Node.js 20+.

### Unauthenticated Routes

| Route | Auth | Purpose |
|-------|------|---------|
| `GET /health` | None | Health check / liveness probe |
| `POST /admin/schema/:gatewayId` | JWT | Register table schema |
| `POST /admin/flush/:gatewayId` | JWT | Trigger manual flush |

> **Note**: The admin routes currently share the same JWT auth as sync routes. In production, consider using a separate admin secret or role-based claim.

## Authorisation

### Current Model (v1.0)

Authorisation in v1.0 is **gateway-scoped**:

- A JWT token grants access to **one gateway ID** (the `gw` claim)
- Any client with a valid token for a gateway can push/pull **any table and any row** within that gateway
- The `clientId` from the JWT `sub` claim is passed to the Durable Object and stamped on deltas, but is **not enforced** against the `clientId` in the push payload

### What Is Not Enforced (v1.0)

| Scope | Status | Notes |
|-------|--------|-------|
| Gateway-level access | Enforced | Token `gw` claim must match route `:gatewayId` |
| Table-level access | Not enforced | Any authenticated client can write to any table |
| Row-level access | Not enforced | Any authenticated client can write to any row |
| Client identity binding | Not enforced | Push payload `clientId` is not validated against JWT `sub` |
| Write vs read | Not enforced | Any authenticated client can both push and pull |

### Planned (Post v1.0)

- **Client identity binding**: Reject pushes where `payload.clientId !== jwt.sub`
- **Role claims**: `role: "reader" | "writer" | "admin"` to separate read/write access
- **Table-level ACLs**: `tables: ["todos", "notes"]` claim to restrict table access
- **Row-level ownership**: Enforce that clients can only modify rows they created

## Threat Model

### Assumptions

1. **The gateway is trusted**: It runs on infrastructure you control (Cloudflare Workers, your own server). It has access to the JWT secret and storage credentials.
2. **The object store is trusted**: R2/S3/MinIO access is controlled by the gateway — clients never write directly to storage.
3. **Clients are untrusted**: Browser clients could be modified, replayed, or spoofed. The JWT is the only trust anchor.
4. **The network is untrusted**: All communication should use HTTPS (enforced by Cloudflare Workers by default).

### Threats and Mitigations

#### Replay Attacks

| Threat | Mitigation | Status |
|--------|-----------|--------|
| Replaying a valid push request | `deltaId` deduplication — the gateway silently accepts duplicate deltas without re-applying them | Mitigated |
| Replaying an expired token | `exp` claim is checked on every request | Mitigated |
| Replaying a token for a different gateway | `gw` claim is checked against the route's `:gatewayId` | Mitigated |

#### Clock Manipulation

| Threat | Mitigation | Status |
|--------|-----------|--------|
| Client sends deltas with far-future HLC | Gateway rejects deltas with HLC wall clock > 5s ahead of server time (`ClockDriftError`) | Mitigated |
| Client sends deltas with far-past HLC | Accepted, but will lose all LWW conflicts against newer deltas. No data corruption risk. | Acceptable |
| Client manipulates counter to win tiebreaks | Counter is 16-bit (0–65535) and part of the HLC. Inflated counters only affect ordering within the same millisecond. | Low risk |

#### Data Integrity

| Threat | Mitigation | Status |
|--------|-----------|--------|
| Client pushes malformed deltas | Schema validation via `SchemaManager` (when configured) rejects deltas with unknown tables or columns | Mitigated (when schema registered) |
| Client impersonates another clientId | Not mitigated in v1.0 — push payload `clientId` is not bound to JWT `sub` | Known gap |
| Cross-tenant data injection | Gateway-scoped JWT prevents access to other gateways. Within a gateway, all clients share the same data space. | Partially mitigated |
| Tampering with Parquet files in storage | Not mitigated — assumes trusted storage. Consider enabling R2 object versioning for audit trails. | Accepted risk |

#### Denial of Service

| Threat | Mitigation | Status |
|--------|-----------|--------|
| Client floods gateway with pushes | Cloudflare Workers rate limiting (external). No built-in rate limiting in LakeSync. | Partially mitigated |
| Client pushes extremely large deltas | No payload size limit in LakeSync. Rely on Cloudflare Workers 100MB request limit or configure a reverse proxy. | Partially mitigated |
| Client creates millions of queue entries | Dead-lettering after 10 retries prevents unbounded queue growth client-side | Mitigated (client-side) |

## Secret Management

### Required Secrets

| Secret | Where | Purpose |
|--------|-------|---------|
| `JWT_SECRET` | Gateway worker env | HS256 signing/verification key |
| R2/S3 credentials | Gateway worker env (R2 binding) or env vars (MinIO) | Object store access |
| Nessie credentials | Gateway worker env (if catalogue enabled) | Iceberg catalogue API |

### Recommendations

1. **Rotate `JWT_SECRET` periodically** — since HS256 is a symmetric algorithm, the same secret signs and verifies. Use Cloudflare Workers secrets (`wrangler secret put JWT_SECRET`) rather than `wrangler.toml`.
2. **Use short-lived tokens** — the dev JWT helper uses 1-hour expiry. Production should use shorter durations (5–15 minutes) with a refresh mechanism.
3. **Never embed secrets in client code** — the todo-app's dev JWT helper is for local development only. Production clients should obtain tokens from an auth endpoint.
4. **Enable R2 object versioning** — provides an audit trail and protection against accidental overwrites.

## Transport Security

- **HTTPS**: Cloudflare Workers enforce HTTPS by default. Self-hosted deployments should use TLS termination.
- **CORS**: Not configured by default. Add CORS headers in the Worker if serving browser clients from a different origin.
- **WebSocket**: The gateway-worker supports WebSocket connections for real-time push. WS connections inherit the same JWT auth from the initial HTTP upgrade request.

## Audit Trail

Every delta includes `clientId` and `hlc`, providing a built-in audit trail:

- **Who**: `clientId` identifies the client that made the change
- **When**: `hlc` provides a high-resolution timestamp
- **What**: `columns` array shows exactly which fields changed and to what values
- **Provenance**: `deltaId` is a deterministic hash that can verify delta integrity

Parquet files in the object store are append-only (new files are written, old files are never modified). Combined with Iceberg snapshot history, this gives you a complete, immutable history of all changes.
