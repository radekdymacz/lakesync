# Phase 11 — SaaS MVP

## Overview

Transform LakeSync from a library into a self-service SaaS product. The sync engine (phases 1-10) is production-ready. This phase adds the SaaS wrapper: control plane, billing, tenant management, dashboard, and developer self-service.

**Package:** `apps/control-plane` (new Next.js app), `packages/control-plane-api` (new), plus modifications to existing gateway packages.

## Architecture

```
                    ┌─────────────────────────────────┐
                    │       Dashboard (Next.js)        │
                    │  apps/dashboard                  │
                    │  Signup / Login / Gateway mgmt   │
                    │  API keys / Usage / Billing      │
                    └──────────────┬──────────────────┘
                                   │
                    ┌──────────────▼──────────────────┐
                    │      Control Plane API           │
                    │  packages/control-plane          │
                    │  Auth (Clerk) / Org CRUD         │
                    │  Gateway provisioning            │
                    │  API key mgmt / Stripe billing   │
                    │  Usage aggregation               │
                    └──┬───────────┬──────────────┬───┘
                       │           │              │
             ┌─────────▼──┐  ┌────▼─────┐  ┌─────▼──────┐
             │  Gateway    │  │ Metering │  │ Usage DB   │
             │  Workers    │  │ (hooks)  │  │ (Postgres) │
             │  (existing) │  │          │  │            │
             └─────────────┘  └──────────┘  └────────────┘
```

## Dependency Graph

```
PARALLEL GROUP A (Quick Wins — no deps, all independent)
  ├── A1: SQL identifier sanitisation
  ├── A2: API versioning (/v1/ prefix)
  ├── A3: signToken utility + auth docs
  ├── A4: Security headers middleware
  ├── A5: Structured error codes in responses
  └── A6: Request ID in responses

PARALLEL GROUP B (Control Plane Foundation — no deps on A)
  ├── B1: Tenant/Org data model + DB schema
  ├── B2: Auth integration (Clerk)        ← after B1
  ├── B3: Gateway provisioning API         ← after B1 + B2
  └── B4: API key management               ← after B1 + B2

PARALLEL GROUP C (Billing & Metering — after B1)
  ├── C1: UsageRecorder interface + hook into gateway  ← after B1
  ├── C2: Quota enforcement middleware                  ← after C1
  └── C3: Stripe billing integration                   ← after B1 + C1

PARALLEL GROUP D (Dashboard — after B2 + B3 + B4)
  ├── D1: Next.js app scaffold + Clerk auth  ← after B2
  ├── D2: Gateway management pages           ← after B3 + D1
  ├── D3: API key management pages           ← after B4 + D1
  └── D4: Usage & billing pages              ← after C3 + D1

PARALLEL GROUP E (Security & Compliance — after B1)
  ├── E1: Audit logging                  ← after B1
  ├── E2: GDPR data deletion API         ← after B1
  ├── E3: RBAC expansion                 ← after B1 + B2
  └── E4: JWT secret rotation            ← independent

PARALLEL GROUP F (DevOps & DX — independent)
  ├── F1: CD pipeline (GitHub Actions)   ← independent
  ├── F2: OpenAPI spec                   ← independent
  ├── F3: CLI tool                       ← after A3 + B3 + B4
  └── F4: Webhook system                 ← after B1
```

## Execution Order (maximise parallelism)

| Step | Tasks | Parallel? | Notes |
|------|-------|-----------|-------|
| 1 | A1, A2, A3, A4, A5, A6, E4, F1, F2 | ALL PARALLEL | Quick wins + independent DevOps |
| 2 | B1 | SEQUENTIAL | Foundation for everything else |
| 3 | B2, C1, E1, E2, F4 | PARALLEL | All depend only on B1 |
| 4 | B3, B4, E3, C2 | PARALLEL | Depend on B1+B2 or C1 |
| 5 | C3, D1 | PARALLEL | Stripe needs B1+C1; Dashboard needs B2 |
| 6 | D2, D3, F3 | PARALLEL | Dashboard pages + CLI |
| 7 | D4 | SEQUENTIAL | Needs C3 + D1 |

---

## PARALLEL GROUP A — Quick Wins

All tasks are independent. Each is a single-agent task. Ship these immediately.

### Task A1 — SQL Identifier Sanitisation

**Priority:** P0 (security)
**Files:** `packages/gateway/src/validation.ts`, `packages/core/src/schema.ts`
**Effort:** 1 hour

Add character validation to table names and column names. Prevent SQL injection via crafted identifiers.

**Implementation:**
1. Add `isValidIdentifier(name: string): boolean` to `packages/core/src/schema.ts`:
   ```ts
   const IDENTIFIER_RE = /^[a-zA-Z_][a-zA-Z0-9_]{0,63}$/;
   export function isValidIdentifier(name: string): boolean {
     return IDENTIFIER_RE.test(name);
   }
   ```
2. Wire into `validateSchemaBody()` in gateway validation:
   - Validate `body.table` against `isValidIdentifier`
   - Validate each `col.name` against `isValidIdentifier`
   - Return `{ error: "Invalid identifier: must match [a-zA-Z_][a-zA-Z0-9_]{0,63}" }` on failure
3. Also validate in `handlePush` delta table names (defence in depth)
4. Tests:
   - Valid identifiers pass
   - `foo"; DROP TABLE` rejected
   - Column names with backticks rejected
   - Empty string rejected
   - 65+ char name rejected

**Acceptance criteria:**
- All table/column names validated before any SQL interpolation
- No breaking change to existing valid schemas
- Existing tests still pass

---

### Task A2 — API Versioning

**Priority:** P0 (SaaS blocker)
**Files:** `apps/gateway-worker/src/index.ts`, `packages/gateway-server/src/pipeline.ts`, `packages/gateway-server/src/routes.ts`
**Effort:** 1 day

Add `/v1/` URL prefix to all sync and admin endpoints. Old paths return 301 redirect for backward compat during migration period.

**Implementation:**
1. **gateway-worker**: Update route matching in `handleRequest()`:
   - All `/sync/...` routes become `/v1/sync/...`
   - All `/admin/...` routes become `/v1/admin/...`
   - `/health` and `/ready` stay unprefixed (infra endpoints)
   - `/connectors/types` becomes `/v1/connectors/types`
   - Add redirect middleware: old paths → 301 to `/v1/` equivalent (with deprecation header)
2. **gateway-server**: Update `buildServerRouteHandlers()` route keys:
   - Same pattern: `/v1/sync/:id/push`, `/v1/admin/flush/:id`, etc.
   - Health/ready stay unprefixed
   - Route matching in pipeline updated
3. **Client SDK**: Update `HttpTransport` base URL handling:
   - Default `basePath` changes from `""` to `"/v1"`
   - Accept explicit override for backward compat
4. **Docs**: Update all curl examples and API reference
5. Add `API-Version: v1` response header on all versioned routes
6. Add `Sunset` header on legacy redirect routes

**Acceptance criteria:**
- All versioned endpoints work at `/v1/...`
- Old paths return 301 with `Location` header
- Client SDK defaults to `/v1`
- Health/ready endpoints unchanged
- All existing tests updated

---

### Task A3 — signToken Utility + Auth Documentation

**Priority:** P0 (DX blocker)
**Files:** `packages/core/src/auth.ts`, `apps/docs/content/docs/auth.mdx` (new)
**Effort:** 1 day

Export a `signToken()` function for server-side JWT creation. Document the complete auth setup flow.

**Implementation:**
1. **`signToken()` in `packages/core/src/auth.ts`:**
   ```ts
   export interface TokenPayload {
     sub: string;          // clientId
     gw: string;           // gatewayId
     role?: 'admin' | 'client';  // default: 'client'
     exp?: number;         // default: now + 1h
     [key: string]: string | string[] | number | undefined; // custom claims
   }

   export async function signToken(
     payload: TokenPayload,
     secret: string
   ): Promise<string>
   ```
   - Uses Web Crypto API (same as verify — edge-runtime compatible)
   - Sets `alg: "HS256"`, `typ: "JWT"` header
   - Defaults `exp` to `Math.floor(Date.now() / 1000) + 3600`
   - Defaults `role` to `"client"`
   - Returns base64url-encoded JWT string
2. **Export `TokenPayload` type** from `@lakesync/core`
3. **Tests:**
   - `signToken` → `verifyToken` roundtrip
   - Default expiry set when omitted
   - Custom claims preserved
   - Admin role token works
4. **Auth documentation page** (`apps/docs/content/docs/auth.mdx`):
   - How JWT auth works in LakeSync
   - Required claims (`sub`, `gw`, `role`, `exp`)
   - Server-side token generation example
   - Client SDK token configuration (`token` vs `getToken`)
   - Sync rules + JWT claims
   - Security best practices (secret management, expiry, rotation)
5. **Remove `createDevJwt` from todo-app** — use `signToken` instead

**Acceptance criteria:**
- `signToken` exported from `@lakesync/core` and `lakesync`
- Roundtrip test passes
- Auth docs page deployed
- todo-app uses `signToken`

---

### Task A4 — Security Headers Middleware

**Priority:** P1
**Files:** `apps/gateway-worker/src/index.ts`, `packages/gateway-server/src/pipeline.ts`
**Effort:** 2 hours

Add standard security headers to all responses.

**Implementation:**
1. **Headers to add:**
   ```
   X-Content-Type-Options: nosniff
   X-Frame-Options: DENY
   Cache-Control: no-store      (on /sync/* and /admin/* only)
   Strict-Transport-Security: max-age=31536000; includeSubDomains  (when behind TLS)
   ```
2. **gateway-worker**: Add headers in the response construction (after CORS)
3. **gateway-server**: Add as first middleware in `buildServerPipeline()` (before CORS)
4. **Tests:** Verify headers present on sync, admin, and health responses

**Acceptance criteria:**
- All responses include `X-Content-Type-Options: nosniff`
- Sync/admin responses include `Cache-Control: no-store`
- Health endpoint does NOT include `Cache-Control: no-store`

---

### Task A5 — Structured Error Codes in API Responses

**Priority:** P1 (DX)
**Files:** `packages/gateway/src/request-handlers.ts`, `apps/gateway-worker/src/index.ts`, `packages/gateway-server/src/routes.ts`
**Effort:** 1 day

Expose internal error codes in API responses. Currently responses are `{ "error": "string" }` — change to `{ "error": "message", "code": "ERROR_CODE" }`.

**Implementation:**
1. **Define error code enum** in `packages/core/src/errors.ts`:
   ```ts
   export const API_ERROR_CODES = {
     VALIDATION_ERROR: 'VALIDATION_ERROR',
     SCHEMA_ERROR: 'SCHEMA_ERROR',
     BACKPRESSURE_ERROR: 'BACKPRESSURE_ERROR',
     CLOCK_DRIFT: 'CLOCK_DRIFT',
     AUTH_ERROR: 'AUTH_ERROR',
     FORBIDDEN: 'FORBIDDEN',
     NOT_FOUND: 'NOT_FOUND',
     RATE_LIMITED: 'RATE_LIMITED',
     ADAPTER_ERROR: 'ADAPTER_ERROR',
     FLUSH_ERROR: 'FLUSH_ERROR',
     INTERNAL_ERROR: 'INTERNAL_ERROR',
   } as const;
   ```
2. **Update shared request handlers** to return `{ error, code }` tuples
3. **Update gateway-worker and gateway-server** response formatting to include `code` field
4. **Update client SDK** error parsing to extract `code` when available
5. **Tests:** Verify error codes in responses for each error type

**Acceptance criteria:**
- All error responses include `code` field
- Client SDK exposes error code
- Backward compatible (old clients can ignore `code`)

---

### Task A6 — Request ID in API Responses

**Priority:** P1 (debugging)
**Files:** `apps/gateway-worker/src/index.ts`, `packages/gateway-server/src/pipeline.ts`
**Effort:** 2 hours

Return request ID in response headers for debugging/support.

**Implementation:**
1. **gateway-worker**: Generate `crypto.randomUUID()` per request, add `X-Request-Id` response header
2. **gateway-server**: Already generates requestId internally — add `X-Request-Id` to response headers
3. **Both**: Accept `X-Request-Id` from incoming request (pass-through from load balancer). If present, use it; otherwise generate.
4. **Include in error responses** as `requestId` field: `{ "error": "...", "code": "...", "requestId": "..." }`
5. **Client SDK**: Extract and expose `requestId` from error responses for logging

**Acceptance criteria:**
- All responses include `X-Request-Id` header
- Error responses include `requestId` field
- Pass-through from incoming request works

---

## PARALLEL GROUP B — Control Plane Foundation

### Task B1 — Tenant / Org Data Model

**Priority:** P0
**Package:** `packages/control-plane` (new)
**Effort:** 3 days

Define the core SaaS entities and persistence layer. This is the foundation for everything else.

**Implementation:**

1. **Create `packages/control-plane/`** with standard package setup (tsconfig, vitest, biome)

2. **Entity types** (`src/entities.ts`):
   ```ts
   interface Organisation {
     id: string;           // nanoid
     name: string;
     slug: string;         // unique, URL-safe
     createdAt: Date;
     updatedAt: Date;
     plan: PlanId;
     stripeCustomerId?: string;
     stripeSubscriptionId?: string;
   }

   interface OrgMember {
     orgId: string;
     userId: string;       // Clerk user ID
     role: OrgRole;        // 'owner' | 'admin' | 'member' | 'viewer'
     createdAt: Date;
   }

   interface Gateway {
     id: string;           // nanoid, becomes gatewayId
     orgId: string;
     name: string;
     region?: string;      // 'auto' | 'us' | 'eu' | 'ap'
     status: 'active' | 'suspended' | 'deleted';
     createdAt: Date;
     updatedAt: Date;
   }

   interface ApiKey {
     id: string;
     orgId: string;
     gatewayId?: string;   // null = org-wide
     name: string;
     keyHash: string;      // SHA-256 of the actual key (never store plaintext)
     keyPrefix: string;    // first 8 chars for identification
     role: 'admin' | 'client';
     scopes?: string[];    // future: per-table, per-operation
     expiresAt?: Date;
     lastUsedAt?: Date;
     createdAt: Date;
   }

   type PlanId = 'free' | 'starter' | 'pro' | 'enterprise';

   interface Plan {
     id: PlanId;
     name: string;
     maxGateways: number;
     maxDeltasPerMonth: number;
     maxStorageBytes: number;
     maxConnectionsPerGateway: number;
     rateLimit: number;    // requests per minute
     price: number;        // cents/month (0 for free)
     stripePriceId?: string;
   }
   ```

3. **Repository interfaces** (`src/repositories.ts`):
   ```ts
   interface OrgRepository {
     create(org: CreateOrgInput): Promise<Result<Organisation, ControlPlaneError>>;
     getById(id: string): Promise<Result<Organisation | null, ControlPlaneError>>;
     getBySlug(slug: string): Promise<Result<Organisation | null, ControlPlaneError>>;
     update(id: string, input: UpdateOrgInput): Promise<Result<Organisation, ControlPlaneError>>;
     delete(id: string): Promise<Result<void, ControlPlaneError>>;
   }

   interface GatewayRepository {
     create(gw: CreateGatewayInput): Promise<Result<Gateway, ControlPlaneError>>;
     getById(id: string): Promise<Result<Gateway | null, ControlPlaneError>>;
     listByOrg(orgId: string): Promise<Result<Gateway[], ControlPlaneError>>;
     update(id: string, input: UpdateGatewayInput): Promise<Result<Gateway, ControlPlaneError>>;
     delete(id: string): Promise<Result<void, ControlPlaneError>>;
   }

   interface ApiKeyRepository {
     create(key: CreateApiKeyInput): Promise<Result<{ apiKey: ApiKey; rawKey: string }, ControlPlaneError>>;
     getByHash(keyHash: string): Promise<Result<ApiKey | null, ControlPlaneError>>;
     listByOrg(orgId: string): Promise<Result<ApiKey[], ControlPlaneError>>;
     revoke(id: string): Promise<Result<void, ControlPlaneError>>;
     updateLastUsed(id: string): Promise<Result<void, ControlPlaneError>>;
   }

   interface MemberRepository {
     add(member: CreateMemberInput): Promise<Result<OrgMember, ControlPlaneError>>;
     remove(orgId: string, userId: string): Promise<Result<void, ControlPlaneError>>;
     listByOrg(orgId: string): Promise<Result<OrgMember[], ControlPlaneError>>;
     getRole(orgId: string, userId: string): Promise<Result<OrgRole | null, ControlPlaneError>>;
     updateRole(orgId: string, userId: string, role: OrgRole): Promise<Result<void, ControlPlaneError>>;
   }
   ```

4. **Postgres implementation** (`src/postgres/`):
   - Migration files for all tables
   - Repository implementations using `pg` driver
   - Connection pool management

5. **Plan definitions** (`src/plans.ts`):
   ```ts
   export const PLANS: Record<PlanId, Plan> = {
     free:       { maxGateways: 1,  maxDeltasPerMonth: 10_000,     maxStorageBytes: 100_MB,  ... },
     starter:    { maxGateways: 3,  maxDeltasPerMonth: 100_000,    maxStorageBytes: 1_GB,    ... },
     pro:        { maxGateways: 10, maxDeltasPerMonth: 1_000_000,  maxStorageBytes: 10_GB,   ... },
     enterprise: { maxGateways: -1, maxDeltasPerMonth: -1,         maxStorageBytes: -1,      ... },
   };
   ```

6. **Tests:** Full CRUD for each repository, unique constraint tests, cascade deletes

**Acceptance criteria:**
- All entity types exported from `@lakesync/control-plane`
- Postgres migrations run cleanly
- All repositories have full test coverage
- Result<T, E> pattern — no throws

---

### Task B2 — Auth Integration (Clerk)

**Priority:** P0
**Depends on:** B1
**Files:** `packages/control-plane/src/auth/`, `apps/gateway-worker/src/auth.ts`
**Effort:** 2 days

Integrate Clerk for user authentication. Clerk handles signup, login, OAuth, MFA. We handle org/role mapping.

**Implementation:**
1. **Clerk webhook handler** (`packages/control-plane/src/auth/clerk-webhook.ts`):
   - `user.created` → create org (if first user) or add as member
   - `user.deleted` → remove from all orgs
   - `session.created` → log for audit trail
   - Webhook signature verification
2. **Token exchange** (`packages/control-plane/src/auth/token-exchange.ts`):
   - Verify Clerk session token → look up org membership → sign LakeSync JWT with `sub`, `gw`, `role`, custom claims
   - This is how dashboard users get gateway access tokens
3. **API key auth** (`packages/control-plane/src/auth/api-key-auth.ts`):
   - Accept `Authorization: Bearer lk_...` (API key format)
   - Hash key, look up in ApiKeyRepository
   - Return org, gateway, role context
   - Update `lastUsedAt`
4. **Dual auth middleware** for control plane API:
   - Accept either Clerk session token OR API key
   - Attach `AuthContext { userId, orgId, role }` to request
5. **Tests:** Webhook processing, token exchange, API key validation

**Acceptance criteria:**
- Clerk webhook events processed correctly
- Users can exchange Clerk token for LakeSync JWT
- API key authentication works
- Org membership enforced

---

### Task B3 — Gateway Provisioning API

**Priority:** P0
**Depends on:** B1, B2
**Files:** `packages/control-plane/src/api/gateways.ts`
**Effort:** 2 days

API for creating, managing, and deleting gateways. This is the tenant provisioning layer.

**Implementation:**
1. **REST endpoints:**
   - `POST /api/orgs/:orgId/gateways` — create gateway (checks plan quota)
   - `GET /api/orgs/:orgId/gateways` — list gateways
   - `GET /api/orgs/:orgId/gateways/:id` — get gateway details
   - `PATCH /api/orgs/:orgId/gateways/:id` — update gateway (name, status)
   - `DELETE /api/orgs/:orgId/gateways/:id` — soft-delete gateway
2. **Gateway provisioning logic:**
   - Generate unique gatewayId (nanoid)
   - Check org plan quota (`maxGateways`)
   - For CF Workers: gateway is auto-available (DO creates on first request)
   - For self-hosted: return connection details / provisioning instructions
3. **Status management:**
   - `active` → normal operation
   - `suspended` → gateway rejects all requests (quota exceeded, payment failed)
   - `deleted` → soft delete, data retained for 30 days
4. **Gateway suspension hook:**
   - When org is suspended (payment failed), mark all gateways as `suspended`
   - Gateway auth middleware checks status before processing requests
5. **Tests:** CRUD, quota enforcement, suspension flow

**Acceptance criteria:**
- Full gateway lifecycle (create, update, suspend, delete)
- Plan quota enforced on create
- Auth middleware checks gateway status
- Org-level operations cascade to gateways

---

### Task B4 — API Key Management

**Priority:** P0
**Depends on:** B1, B2
**Files:** `packages/control-plane/src/api/api-keys.ts`
**Effort:** 2 days

Self-service API key creation, rotation, and revocation.

**Implementation:**
1. **Key format:** `lk_live_<random>` (production) / `lk_test_<random>` (test)
   - 32-byte random, base62 encoded
   - Prefix `lk_` for easy identification in logs/configs
   - Only shown ONCE at creation time — stored as SHA-256 hash
2. **REST endpoints:**
   - `POST /api/orgs/:orgId/api-keys` — create key (returns raw key once)
   - `GET /api/orgs/:orgId/api-keys` — list keys (prefix + metadata only)
   - `DELETE /api/orgs/:orgId/api-keys/:id` — revoke key
   - `POST /api/orgs/:orgId/api-keys/:id/rotate` — revoke old, create new
3. **Scoping:**
   - Org-wide key: access all gateways in org
   - Gateway-scoped key: access single gateway
   - Role: `admin` (full access) or `client` (sync only)
4. **Key → JWT translation:**
   - When API key is used to authenticate to a gateway, the auth middleware:
     1. Hashes the key
     2. Looks up in ApiKeyRepository
     3. Validates org/gateway access
     4. Creates an ephemeral JWT for the gateway request
5. **Rate limiting per key:** Track usage per API key for quota enforcement
6. **Tests:** Create, list, revoke, rotate, scope enforcement

**Acceptance criteria:**
- API keys can be created with org-wide or gateway-scoped access
- Raw key shown only once
- Revocation is immediate
- Rotation creates new key and revokes old atomically
- Usage tracked per key

---

## PARALLEL GROUP C — Billing & Metering

### Task C1 — UsageRecorder Interface + Gateway Integration

**Priority:** P0
**Depends on:** B1
**Files:** `packages/core/src/usage.ts` (new), `packages/gateway/src/gateway.ts`, `packages/gateway-server/src/routes.ts`, `apps/gateway-worker/src/sync-gateway-do.ts`
**Effort:** 3 days

Add usage recording hooks into the gateway request path. Every billable event gets recorded.

**Implementation:**
1. **UsageRecorder interface** (`packages/core/src/usage.ts`):
   ```ts
   interface UsageEvent {
     gatewayId: string;
     orgId?: string;        // resolved from gateway → org mapping
     eventType: UsageEventType;
     count: number;          // deltas, bytes, connections, etc.
     timestamp: Date;
   }

   type UsageEventType =
     | 'push_deltas'        // count = number of deltas pushed
     | 'pull_deltas'        // count = number of deltas pulled
     | 'flush_bytes'        // count = bytes flushed to adapter
     | 'flush_deltas'       // count = deltas flushed
     | 'storage_bytes'      // count = total storage bytes (periodic snapshot)
     | 'api_call'           // count = 1
     | 'ws_connection'      // count = 1 (open), -1 (close)
     | 'action_executed';   // count = 1

   interface UsageRecorder {
     record(event: UsageEvent): void;  // fire-and-forget, never blocks request
     flush(): Promise<void>;           // periodic flush to persistent store
   }
   ```
2. **MemoryUsageRecorder** (default, for backward compat):
   - Buffers events in memory
   - Aggregates by gatewayId + eventType per minute
   - `flush()` writes aggregated counters to a `UsageStore`
3. **UsageStore interface** (`packages/control-plane/src/usage/`):
   ```ts
   interface UsageStore {
     recordAggregates(aggregates: UsageAggregate[]): Promise<void>;
     queryUsage(orgId: string, from: Date, to: Date): Promise<UsageSummary>;
     queryGatewayUsage(gatewayId: string, from: Date, to: Date): Promise<UsageSummary>;
   }
   ```
4. **Postgres UsageStore** — time-series table with hourly rollups
5. **Wire into gateway:**
   - `GatewayConfig.usageRecorder?: UsageRecorder`
   - `handlePush`: record `push_deltas` (count = deltas.length)
   - `handlePull`: record `pull_deltas` (count = result.length)
   - `flushEntries`: record `flush_bytes` + `flush_deltas`
   - `handleAction`: record `action_executed`
   - WebSocket open/close: record `ws_connection`
6. **Wire into gateway-server and gateway-worker** route handlers
7. **Tests:** Recording, aggregation, flush, query

**Acceptance criteria:**
- All billable events recorded
- Fire-and-forget — never blocks request path
- Aggregated to reduce write volume
- Queryable per org and per gateway
- Backward compatible (no recorder = no-op)

---

### Task C2 — Quota Enforcement Middleware

**Priority:** P1
**Depends on:** C1
**Files:** `packages/control-plane/src/quota/`
**Effort:** 2 days

Check usage against plan limits before processing requests. Reject with 429 when quota exceeded.

**Implementation:**
1. **QuotaChecker** interface:
   ```ts
   interface QuotaChecker {
     checkPush(orgId: string, deltaCount: number): Promise<QuotaResult>;
     checkConnection(orgId: string): Promise<QuotaResult>;
     checkGatewayCreate(orgId: string): Promise<QuotaResult>;
   }

   type QuotaResult = { allowed: true } | { allowed: false; reason: string; resetAt?: Date };
   ```
2. **CachedQuotaChecker** — caches current period usage with 60s TTL
   - Reads from UsageStore (aggregated counters)
   - Compares against plan limits
   - Local counter for in-flight requests (optimistic check)
3. **Middleware integration:**
   - gateway-worker: check quota in auth middleware (after JWT verify, before DO forward)
   - gateway-server: check quota in rate limit middleware slot
   - Return 429 with `Retry-After` and `X-Quota-Remaining` headers
4. **Graceful degradation:**
   - If UsageStore is unavailable, allow requests (fail-open)
   - Log warning for monitoring
5. **Tests:** Under-limit passes, over-limit rejects, cache works, fail-open on error

**Acceptance criteria:**
- Push/pull/connect checked against plan limits
- 429 response with clear error message
- Cached for performance (not checking DB every request)
- Fail-open on quota service failure

---

### Task C3 — Stripe Billing Integration

**Priority:** P0
**Depends on:** B1, C1
**Files:** `packages/control-plane/src/billing/`
**Effort:** 3 days

Integrate Stripe for subscription management and payment processing.

**Implementation:**
1. **Stripe customer lifecycle:**
   - On org creation: create Stripe customer, store `stripeCustomerId`
   - Free plan: no subscription needed
   - Upgrade: create Stripe subscription with selected plan's `stripePriceId`
   - Downgrade: update subscription (prorated)
   - Cancel: cancel subscription at period end
2. **Stripe webhook handler:**
   - `customer.subscription.updated` → update org plan
   - `customer.subscription.deleted` → downgrade to free, suspend if over limits
   - `invoice.payment_succeeded` → update billing status
   - `invoice.payment_failed` → suspend org after grace period (3 days)
3. **Billing API endpoints:**
   - `GET /api/orgs/:orgId/billing` — current plan, usage, next invoice
   - `POST /api/orgs/:orgId/billing/checkout` — create Stripe checkout session (upgrade)
   - `POST /api/orgs/:orgId/billing/portal` — create Stripe billing portal session
4. **Usage reporting to Stripe:**
   - For usage-based plans: report metered usage to Stripe via Usage Records API
   - Run daily aggregation job
5. **Tests:** Subscription lifecycle, webhook processing, usage reporting

**Acceptance criteria:**
- Org can upgrade/downgrade plans via Stripe checkout
- Payment failures suspend org after grace period
- Usage reported to Stripe for metered billing
- Billing portal for self-service invoice/payment management

---

## PARALLEL GROUP D — Dashboard

### Task D1 — Dashboard App Scaffold

**Priority:** P1
**Depends on:** B2
**Package:** `apps/dashboard` (new)
**Effort:** 2 days

**Implementation:**
1. **Next.js 15 app** with:
   - Clerk auth (signup, login, org switcher)
   - Tailwind CSS v4 + shadcn/ui components
   - App router with layout (sidebar nav, org context)
2. **Pages:**
   - `/` — redirect to `/dashboard`
   - `/sign-in`, `/sign-up` — Clerk hosted
   - `/dashboard` — overview (gateway count, usage summary)
   - `/gateways` — gateway list (placeholder)
   - `/api-keys` — API key list (placeholder)
   - `/usage` — usage charts (placeholder)
   - `/settings` — org settings, billing (placeholder)
3. **API client:** typed fetch wrapper for control plane API
4. **Deploy:** Vercel or Cloudflare Pages

**Acceptance criteria:**
- Auth flow works (signup → dashboard)
- Org context available throughout
- Navigation between all pages
- Responsive layout

---

### Task D2 — Gateway Management Pages

**Priority:** P1
**Depends on:** B3, D1
**Files:** `apps/dashboard/app/gateways/`
**Effort:** 2 days

**Implementation:**
1. **Gateway list page** (`/gateways`):
   - Table: name, status, region, created, delta count (from usage)
   - Create button → modal with name + region selector
   - Status badges (active, suspended)
2. **Gateway detail page** (`/gateways/:id`):
   - Overview: status, created, region
   - Schema tab: current table schemas (read-only)
   - Sync rules tab: current rules (read-only initially, editable later)
   - Connectors tab: registered connectors
   - Metrics tab: buffer stats, push/pull counts
   - Settings tab: rename, suspend, delete
3. **Connection instructions:**
   - Show code snippet for connecting client SDK to this gateway
   - Include gateway URL and instructions for generating a token

**Acceptance criteria:**
- Full CRUD for gateways
- Gateway detail page with all tabs
- Connection instructions with copy-to-clipboard

---

### Task D3 — API Key Management Pages

**Priority:** P1
**Depends on:** B4, D1
**Files:** `apps/dashboard/app/api-keys/`
**Effort:** 1 day

**Implementation:**
1. **API key list page** (`/api-keys`):
   - Table: name, prefix, role, scope, last used, created
   - Create button → modal with name, role, gateway scope
   - Show raw key ONCE in creation success dialog
2. **Key actions:** Revoke, rotate (with confirmation)
3. **Usage column:** Show last used timestamp

**Acceptance criteria:**
- Create key with one-time raw key display
- Revoke and rotate work
- Key list shows metadata (never raw key)

---

### Task D4 — Usage & Billing Pages

**Priority:** P1
**Depends on:** C3, D1
**Files:** `apps/dashboard/app/usage/`, `apps/dashboard/app/settings/billing/`
**Effort:** 2 days

**Implementation:**
1. **Usage page** (`/usage`):
   - Time-series charts (daily): deltas synced, storage used, API calls
   - Per-gateway breakdown
   - Current period vs plan limits (progress bars)
2. **Billing page** (`/settings/billing`):
   - Current plan display
   - Upgrade/downgrade buttons → Stripe checkout
   - Manage payment method → Stripe portal
   - Invoice history → Stripe portal
3. **Charts:** Use Recharts or similar lightweight charting lib

**Acceptance criteria:**
- Usage charts show real data from UsageStore
- Plan limits displayed with current usage
- Stripe checkout flow works
- Billing portal accessible

---

## PARALLEL GROUP E — Security & Compliance

### Task E1 — Audit Logging

**Priority:** P1
**Depends on:** B1
**Files:** `packages/control-plane/src/audit/`
**Effort:** 2 days

**Implementation:**
1. **AuditEvent type:**
   ```ts
   interface AuditEvent {
     id: string;
     orgId: string;
     actorId: string;       // userId or apiKeyId
     actorType: 'user' | 'api_key' | 'system';
     action: AuditAction;
     resource: string;       // 'gateway:abc123', 'api-key:xyz', etc.
     metadata?: Record<string, unknown>;
     ipAddress?: string;
     timestamp: Date;
   }

   type AuditAction =
     | 'gateway.create' | 'gateway.update' | 'gateway.delete' | 'gateway.suspend'
     | 'api_key.create' | 'api_key.revoke' | 'api_key.rotate'
     | 'schema.update' | 'sync_rules.update'
     | 'connector.register' | 'connector.unregister'
     | 'member.add' | 'member.remove' | 'member.role_change'
     | 'billing.plan_change' | 'billing.payment_failed'
     | 'flush.manual';
   ```
2. **AuditLogger interface + PostgresAuditLogger:**
   - Append-only table, no deletes, no updates
   - Indexed by orgId + timestamp
   - 90-day retention (configurable per plan)
3. **Wire into all admin operations:**
   - Control plane API endpoints
   - Gateway admin endpoints (flush, schema, sync-rules, connectors)
4. **Query API:**
   - `GET /api/orgs/:orgId/audit-log?from=...&to=...&action=...`
   - Pagination via cursor
5. **Tests:** Event recording, querying, retention

**Acceptance criteria:**
- All admin operations logged
- Audit log queryable via API
- Append-only (immutable)
- Indexed for fast org-scoped queries

---

### Task E2 — GDPR Data Deletion API

**Priority:** P0 (legal requirement for EU)
**Depends on:** B1
**Files:** `packages/gateway/src/gateway.ts`, `packages/control-plane/src/gdpr/`
**Effort:** 3 days

**Implementation:**
1. **Gateway-level data deletion:**
   - `SyncGateway.purgeDeltas(filter)` — delete deltas matching filter (by clientId, table, or all)
   - Clears DeltaBuffer entries
   - Deletes from adapter (if DatabaseAdapter: DELETE FROM; if LakeAdapter: delete objects)
   - Clears checkpoint data
2. **Control plane deletion API:**
   - `POST /api/orgs/:orgId/data-deletion-request` — GDPR erasure request
   - Accepts: `{ scope: 'user' | 'gateway' | 'org', targetId: string }`
   - Async processing (queue job) — returns request ID
   - `GET /api/orgs/:orgId/data-deletion-request/:id` — check status
3. **User data deletion** (scope: 'user'):
   - Delete all deltas where `clientId` matches user
   - Delete user from all org memberships
   - Delete API keys owned by user
   - Audit log: mark entries as `[REDACTED]` (retain structure, remove PII)
4. **Gateway data deletion** (scope: 'gateway'):
   - Delete all deltas, schemas, sync rules, connectors for gateway
   - Delete R2 objects (Parquet files, checkpoints)
   - Delete adapter data (database rows)
5. **Org data deletion** (scope: 'org'):
   - Delete all gateways + their data
   - Delete all members, API keys, audit log entries
   - Cancel Stripe subscription
6. **Data export API** (GDPR Article 20):
   - `GET /api/orgs/:orgId/data-export` — export all org data as JSON
   - Streaming response for large datasets
7. **Tests:** Each deletion scope, verification of complete removal

**Acceptance criteria:**
- User data completely removable
- Gateway data completely removable
- Org data completely removable
- Data export in machine-readable format
- Async processing with status tracking

---

### Task E3 — RBAC Expansion

**Priority:** P1
**Depends on:** B1, B2
**Files:** `packages/control-plane/src/auth/rbac.ts`
**Effort:** 2 days

Expand from 2 roles (admin/client) to 4 roles with per-resource permissions.

**Implementation:**
1. **Roles:**
   - `owner` — full access, can delete org, transfer ownership
   - `admin` — manage gateways, API keys, connectors, sync rules, members (except owner)
   - `member` — push/pull to assigned gateways, view metrics
   - `viewer` — read-only access (pull only, view metrics)
2. **Permission matrix:**
   ```
                     owner  admin  member  viewer
   org.update         yes    yes    no      no
   org.delete         yes    no     no      no
   gateway.create     yes    yes    no      no
   gateway.delete     yes    yes    no      no
   gateway.push       yes    yes    yes     no
   gateway.pull       yes    yes    yes     yes
   gateway.admin      yes    yes    no      no
   api_key.create     yes    yes    no      no
   api_key.revoke     yes    yes    own     no
   member.manage      yes    yes    no      no
   billing.manage     yes    yes    no      no
   audit.view         yes    yes    no      no
   ```
3. **`checkPermission(actor, action, resource): boolean`** function
4. **Wire into all API endpoints** — check permissions before executing
5. **JWT role mapping:** Org role → gateway JWT role (`member`/`viewer` → `client`, `admin`/`owner` → `admin`)
6. **Tests:** Each role against each permission

**Acceptance criteria:**
- 4 roles with clear permission boundaries
- Permission checks on all endpoints
- Owner can't be removed (must transfer first)
- Backward compatible JWT mapping

---

### Task E4 — JWT Secret Rotation

**Priority:** P1
**Independent**
**Files:** `packages/core/src/auth.ts`, `apps/gateway-worker/src/index.ts`, `packages/gateway-server/src/auth-middleware.ts`
**Effort:** 1 day

Support dual JWT secrets for rolling rotation without downtime.

**Implementation:**
1. **Dual secret verification:**
   - Accept `jwtSecret: string | [string, string]` (primary + previous)
   - `verifyToken` tries primary first, then previous
   - `signToken` always uses primary
2. **Rotation flow:**
   1. Set new secret as primary, old as secondary
   2. Wait for all existing tokens to expire (max 1 hour)
   3. Remove secondary
3. **gateway-worker:** Accept `JWT_SECRET_PREVIOUS` env var
4. **gateway-server:** Accept `jwtSecretPrevious` config option
5. **Tests:** Rotation flow, primary preferred, previous still works, invalid still rejected

**Acceptance criteria:**
- Tokens signed with old secret still verify during rotation window
- New tokens use new secret
- Clear documentation on rotation procedure

---

## PARALLEL GROUP F — DevOps & Developer Experience

### Task F1 — CD Pipeline

**Priority:** P1
**Independent**
**Files:** `.github/workflows/deploy.yml` (new)
**Effort:** 2 days

**Implementation:**
1. **gateway-worker deployment** (`.github/workflows/deploy-worker.yml`):
   - Trigger: push to main (paths: `apps/gateway-worker/**`, `packages/**`)
   - Steps: install, build, `wrangler deploy --env staging`, smoke test, `wrangler deploy --env production`
   - Requires: `CF_API_TOKEN` secret
   - Manual approval gate for production
2. **gateway-server Docker** (`.github/workflows/deploy-server.yml`):
   - Trigger: push to main (paths: `packages/gateway-server/**`, `packages/**`)
   - Steps: build Docker image, push to ghcr.io, tag with commit SHA + `latest`
   - No deployment step (user deploys their own infra)
3. **Wrangler environment config:**
   - Add `[env.staging]` and `[env.production]` blocks to `wrangler.toml`
   - Different DO namespaces, R2 buckets, secrets per environment
4. **NPM publish** (`.github/workflows/publish.yml`):
   - Trigger: GitHub release created
   - Steps: build, test, publish to npm
   - Requires: `NPM_TOKEN` secret

**Acceptance criteria:**
- Worker deploys to staging automatically on push to main
- Production deploy requires manual approval
- Docker image published to ghcr.io
- NPM publish on release

---

### Task F2 — OpenAPI Spec

**Priority:** P2
**Independent**
**Files:** `packages/gateway/src/openapi.ts` (new), `apps/docs/content/docs/api/rest.mdx` (new)
**Effort:** 2 days

**Implementation:**
1. **OpenAPI 3.1 spec** (`packages/gateway/src/openapi.ts`):
   - Define spec as TypeScript object (type-safe)
   - All sync and admin endpoints
   - Request/response schemas matching actual types
   - JWT security scheme
   - Error response schema (with `code` field from A5)
2. **Serve spec:**
   - `GET /v1/openapi.json` endpoint in both gateway-worker and gateway-server
   - `GET /v1/docs` — Swagger UI redirect (optional, low priority)
3. **Generate from spec:**
   - Export OpenAPI JSON at build time
   - Add to docs site as downloadable resource
4. **Validate:** Run `@redocly/cli lint` in CI

**Acceptance criteria:**
- Complete OpenAPI spec covering all endpoints
- Served at `/v1/openapi.json`
- Validated in CI
- Available on docs site

---

### Task F3 — CLI Tool

**Priority:** P2
**Depends on:** A3, B3, B4
**Package:** `packages/cli` (new)
**Effort:** 3 days

**Implementation:**
1. **Package setup:** `packages/cli/` with `commander` or `citty`
   - Binary name: `lakesync`
   - Published as `lakesync-cli` on npm (or `lakesync` with `bin` entry)
2. **Commands:**
   ```
   lakesync init                    # Interactive project setup
   lakesync login                   # Authenticate with dashboard (opens browser)
   lakesync logout                  # Clear stored credentials
   lakesync token create            # Create a JWT (for dev/testing)
     --secret <secret>
     --gateway <gatewayId>
     --client <clientId>
     --role <admin|client>
     --ttl <seconds>
   lakesync gateways list           # List gateways in current org
   lakesync gateways create <name>  # Create gateway
   lakesync gateways delete <id>    # Delete gateway
   lakesync keys create             # Create API key
     --name <name>
     --role <admin|client>
     --gateway <gatewayId>
   lakesync keys list               # List API keys
   lakesync keys revoke <id>        # Revoke key
   lakesync push <file>             # Push deltas from JSON/CSV file
   lakesync pull                    # Pull deltas to stdout/file
     --gateway <id>
     --since <hlc>
     --table <name>
   lakesync status                  # Show gateway status + metrics
   ```
3. **Config file:** `~/.lakesync/config.json` for auth token + default org/gateway
4. **`lakesync init` generates:**
   - `.env.local` with gateway URL and test token
   - Example `sync.ts` file with `createClient()` setup
5. **Tests:** Command parsing, token generation, config persistence

**Acceptance criteria:**
- `npx lakesync-cli token create` works for quick JWT generation
- `npx lakesync-cli init` sets up a new project
- Auth with dashboard via `lakesync login`
- Gateway and key management from CLI

---

### Task F4 — Webhook System

**Priority:** P2
**Depends on:** B1
**Files:** `packages/control-plane/src/webhooks/`
**Effort:** 3 days

**Implementation:**
1. **Webhook registration:**
   - `POST /api/orgs/:orgId/webhooks` — register endpoint
   - `{ url, events: string[], secret: string }`
   - Events: `sync.push`, `sync.pull`, `flush.complete`, `flush.error`, `connector.error`, `schema.change`, `gateway.status`
2. **Webhook delivery:**
   - HMAC-SHA256 signature in `X-LakeSync-Signature` header
   - JSON payload with event type, timestamp, data
   - Retry with exponential backoff (3 attempts)
   - Delivery log with status
3. **Webhook management:**
   - `GET /api/orgs/:orgId/webhooks` — list
   - `DELETE /api/orgs/:orgId/webhooks/:id` — delete
   - `POST /api/orgs/:orgId/webhooks/:id/test` — send test event
   - `GET /api/orgs/:orgId/webhooks/:id/deliveries` — delivery log
4. **Wire events:**
   - Gateway flush → emit `flush.complete` or `flush.error`
   - Gateway push → emit `sync.push` (with delta count)
   - Connector error → emit `connector.error`
5. **Tests:** Registration, delivery, retry, signature verification

**Acceptance criteria:**
- Webhook registration and management API
- Signed deliveries with retry
- Delivery log for debugging
- Test endpoint for verification

---

## Summary — Agent Assignment Guide

For maximum parallelism with Claude Code Teams, assign agents as follows:

| Step | Agent 1 | Agent 2 | Agent 3 | Agent 4 | Agent 5 |
|------|---------|---------|---------|---------|---------|
| 1 | A1+A4 (security) | A2 (versioning) | A3 (signToken) | A5+A6 (errors) | F1 (CD) |
| 2 | B1 (data model) | F2 (OpenAPI) | E4 (JWT rotation) | — | — |
| 3 | B2 (Clerk auth) | C1 (metering) | E1 (audit) | E2 (GDPR) | F4 (webhooks) |
| 4 | B3 (provisioning) | B4 (API keys) | E3 (RBAC) | C2 (quotas) | — |
| 5 | C3 (Stripe) | D1 (dashboard) | F3 (CLI) | — | — |
| 6 | D2 (gateways UI) | D3 (keys UI) | — | — | — |
| 7 | D4 (usage UI) | — | — | — | — |

**Total estimated effort:** ~45 days solo, ~12 days with 5 parallel agents.
