# LakeSync MVP Readiness Review

**Date:** 2026-02-08
**Reviewed by:** 6 parallel agents (architecture, security, tests, DX, infrastructure, documentation)

---

## Executive Summary

LakeSync has **exceptionally strong fundamentals** for an MVP. All 10 packages are feature-complete, the sync protocol is thoroughly tested (47 test files), security is well-designed, and the codebase is clean (zero TODO/FIXME comments). However, there are **critical gaps** in developer experience, observability, documentation accuracy, and a few security hardening items that must be addressed before public launch.

### Overall Scores

| Area | Score | Verdict |
|------|-------|---------|
| Architecture & Completeness | 9/10 | All packages complete, clean dependency graph |
| Security | 7/10 | Strong fundamentals, 3 must-fix items |
| Test Coverage | 9/10 | 47 test files, real behavior testing, 1 gap |
| Developer Experience | 5/10 | Functional but high friction, no reactive queries |
| Infrastructure & CI/CD | 6/10 | CI works, zero observability, no deploy pipeline |
| Documentation | 6/10 | Strong JSDoc, broken getting-started, major doc gaps |

---

## P0 — Ship-Blockers (must fix before any public use)

### Security

| # | Issue | Severity | Effort | Location |
|---|-------|----------|--------|----------|
| S1 | **No admin/client role separation** — any JWT holder can call `/admin/flush`, `/admin/schema`, `/admin/sync-rules` | High | Small | `gateway-worker/src/index.ts` |
| S2 | **WebSocket pull bypasses sync rules** — WS clients get unfiltered deltas, no per-connection claim state | High | Medium | `gateway-worker/src/sync-gateway-do.ts:576-594` |
| S3 | **JWT `exp` claim is optional** — tokens without expiry are valid forever | Medium | Trivial | `gateway-worker/src/auth.ts:152-157` |

### Developer Experience

| # | Issue | Impact | Effort |
|---|-------|--------|--------|
| D1 | **No data change notifications** — no way to subscribe to remote data changes; UI can't react to sync pulls | Critical | Medium |
| D2 | **`syncOnce()` is private** — developers must manually call push/pull; auto-sync doesn't trigger on local mutations | High | Small |
| D3 | **`apache-arrow` is a direct dep of core** — bloats every client bundle by ~500KB for unused Parquet schema types | High | Small (move to `@lakesync/parquet`) |

### Documentation

| # | Issue | Impact | Effort |
|---|-------|--------|--------|
| X1 | **getting-started.mdx has 3 broken code examples** — `LocalDB.create()` (should be `.open()`), wrong SyncCoordinator constructor, wrong tracker.insert args | Critical | Small |
| X2 | **TableSchema types wrong in docs** — shows `text\|integer\|real\|blob` vs actual `string\|number\|boolean\|json\|null` | High | Trivial |

### Infrastructure

| # | Issue | Impact | Effort |
|---|-------|--------|--------|
| I1 | **Zero observability** — no logging, no metrics, no error tracking in gateway-worker. Zero `console.log` calls. | Critical | Medium |

---

## P1 — First-Week Fixes (needed for credible MVP)

### Security

| # | Issue | Effort |
|---|-------|--------|
| S4 | Remove hardcoded dev JWT secret from todo-app client code | Small |
| S5 | Enforce actual body size (not just Content-Length header) on push | Small |
| S6 | CORS defaults to wildcard when `ALLOWED_ORIGINS` not set — fail closed in production | Small |
| S7 | Remove `X-Client-Id` and `X-Auth-Claims` from CORS allowed headers (internal-only) | Trivial |

### Developer Experience

| # | Issue | Effort |
|---|-------|--------|
| D4 | Create `createClient()` factory function (wires LocalDB + schemas + transport + coordinator) | Medium |
| D5 | Export `unwrapOrThrow` from `lakesync/client` (currently requires separate core import) | Trivial |
| D6 | Add `unwrapOr(default)` and `match()` helpers to Result type | Small |
| D7 | Add auto-ID generation option for inserts (default `crypto.randomUUID()`) | Small |
| D8 | Document JWT claims format expected by gateway | Small |
| D9 | Remove `"opfs"` from `DbConfig.backend` type (not implemented, misleading) | Trivial |
| D10 | Make dead-lettered queue entries observable (callback/event, not just console.warn — this is silent data loss) | Small |
| D11 | Configurable auto-sync interval (currently hardcoded 10s) | Trivial |

### Documentation

| # | Issue | Effort |
|---|-------|--------|
| X3 | Write Gateway HTTP API reference (6 endpoints, auth, request/response formats) | Medium |
| X4 | Update adapter README with DatabaseAdapter, Postgres, MySQL, Composite, migrate docs | Small |
| X5 | Fix CLAUDE.md staleness ("database adapters are next milestone" — they're done) | Trivial |
| X6 | Update CHANGELOG.md with Phase 6 entries | Small |
| X7 | Fix architecture.mdx "2 apps" → "3 apps" | Trivial |
| X8 | Fix index.mdx adapter description (not just "Apache Iceberg") | Trivial |

### Infrastructure

| # | Issue | Effort |
|---|-------|--------|
| I2 | Add `build` CI job (tsup bundle is never tested in CI) | Small |
| I3 | Add CF Workers deploy workflow (currently manual `wrangler deploy`) | Medium |
| I4 | Add R2 list pagination in R2Adapter (truncates at 1000 objects) | Small |
| I5 | Add npm package metadata (repository, keywords, engines fields) | Trivial |
| I6 | Set `ALLOWED_ORIGINS` in production wrangler config | Trivial |

### Tests

| # | Issue | Effort |
|---|-------|--------|
| T1 | Add tests for `validation/identifier.ts` — SQL injection defence has zero tests | Small |
| T2 | Add Postgres/MySQL containers to CI integration tests | Medium |

---

## P2 — Production Hardening (post-MVP)

### Security
- Rate limiting on sync endpoints (per-client via DO storage or CF Rate Limiting)
- Validate pull `clientId` against JWT `sub` for audit consistency
- Wire SchemaManager into DO for server-side delta validation

### Developer Experience
- Configurable logger (default console, injectable custom)
- Narrow `LakeSyncError.code` to union type for exhaustive matching
- SSR safety guards (typeof window/document checks)
- WASM loading configuration (`locateFile` for sql.js)
- Typed data for insert/update (currently `Record<string, unknown>`)
- Framework integrations (React hooks, Vue composables)
- Query builder or typed query helpers

### Infrastructure
- Environment separation in wrangler.toml (staging/production)
- Automated npm publish workflow with version bumping
- Request correlation IDs (X-Request-Id)
- CI dependency caching
- Secrets scanning in CI (gitleaks)
- Commitlint for conventional commits

### Tests
- Direct tests for `json.ts` bigintReplacer/bigintReviver
- Load/stress tests for gateway under concurrent push/pull
- Todo-app smoke tests

### Documentation
- Deployment guide (CF Workers, R2, wrangler.toml, JWT_SECRET)
- Database adapter setup guide
- Sync rules configuration guide
- Troubleshooting / FAQ
- Link ADRs from docs site
- Error codes reference
- Migration guide (CompositeAdapter + migrateAdapter)
- API docs for gateway, adapter, proto, compactor, analyst

---

## Architecture Status (All Complete)

| Package | Status | Tests | Quality |
|---------|--------|-------|---------|
| @lakesync/core | Complete | 7 files | Excellent |
| @lakesync/client | Complete | 12 files | Excellent |
| @lakesync/gateway | Complete | 7 files | Very Good |
| @lakesync/adapter | Complete | 6 files | Good |
| @lakesync/proto | Complete | 1 file | Excellent |
| @lakesync/parquet | Complete | 1 file | Excellent |
| @lakesync/catalogue | Complete | 2 files | Good |
| @lakesync/compactor | Complete | 5 files | Excellent |
| @lakesync/analyst | Complete | 3 files | Good |
| lakesync (unified) | Complete | — | Good |
| gateway-worker | Complete | 4 files | Production-ready |
| todo-app | Complete | 0 files | Reference impl |
| docs | Complete | — | Content gaps |

### Dependency Graph
```
core (foundation)
├── proto (wire format)
├── parquet (file format)
├── catalogue (Iceberg REST)
├── adapter (S3/Postgres/MySQL)
├── analyst (DuckDB-WASM)
├── gateway (sync engine)
├── compactor (background maintenance)
├── client (SDK: SQLite + IndexedDB)
├── lakesync (unified npm package)
├── gateway-worker (CF Workers + DO)
├── todo-app (Vite demo)
└── docs (Fumadocs + Next.js)
```

---

## Security Positive Findings

These are already done well:
- SQL injection defence: 2-layer (assertValidIdentifier + quoteIdentifier + parameterised queries)
- XSS protection: `textContent` not `innerHTML` for user data
- JWT: Web Crypto HMAC-SHA256, proper verification, gateway ID + client ID enforcement
- No `eval()` or `new Function()` anywhere
- Result<T,E> prevents exception info leaks
- Secrets properly .gitignored, JWT_SECRET via wrangler secret
- Idempotent push via deltaId deduplication
- All database adapters use parameterised queries

---

## Quick Win Count

| Priority | Count | Effort Profile |
|----------|-------|----------------|
| P0 (ship-blockers) | 8 items | ~3 medium, ~3 small, ~2 trivial |
| P1 (first-week) | 19 items | ~3 medium, ~9 small, ~7 trivial |
| P2 (post-MVP) | ~25 items | Various |

**Bottom line:** The engine is built and tested. The gaps are in the last mile — DX polish, observability, docs accuracy, and 3 security fixes. Addressing the 8 P0 items would make this a credible MVP.
