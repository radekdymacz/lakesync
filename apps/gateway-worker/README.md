# @lakesync/gateway-worker

Cloudflare Workers deployment for the LakeSync sync gateway. Uses Durable Objects for stateful sync coordination, R2 for object storage, and JWT for authentication.

## Architecture

- **Durable Object** (`SyncGatewayDO`) — one per gateway ID, handles push/pull/flush
- **R2 Adapter** — writes Parquet/JSON flush files to Cloudflare R2
- **JWT Auth** — HS256 middleware validates `Authorization: Bearer <token>` on sync routes

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `JWT_SECRET` | Yes | HS256 secret for JWT verification |
| `NESSIE_URI` | No | Nessie catalogue endpoint for Iceberg metadata |
| `LAKE_BUCKET` | Yes | R2 bucket binding name for flush storage |

## Routes

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| POST | `/sync/:gatewayId/push` | JWT | Push client deltas to gateway |
| GET | `/sync/:gatewayId/pull` | JWT | Pull remote deltas since cursor |
| POST | `/sync/:gatewayId/flush` | JWT | Trigger manual flush |
| POST | `/admin/schema/:gatewayId` | None | Register table schema |
| GET | `/health` | None | Health check |

## Local Development

```bash
cd apps/gateway-worker
wrangler dev
```

## Deployment

```bash
# Create R2 bucket (once)
wrangler r2 bucket create lakesync-data

# Deploy
wrangler deploy
```

## Configuration

See `wrangler.toml` for Durable Object and R2 bindings configuration.
