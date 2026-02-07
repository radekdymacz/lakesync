# LakeSync Development Infrastructure

## Quick Start

```bash
docker compose -f docker/docker-compose.yml up -d
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| MinIO | 9000 (API), 9001 (Console) | S3-compatible object store |
| Nessie | 19120 | Iceberg REST catalogue |

## MinIO Console

Open http://localhost:9001 and log in with:
- Username: `lakesync`
- Password: `lakesync123`

The `lakesync-dev` bucket is created automatically on first start.

## Environment Variables

Copy `.env.example` to `.env` to customise:

```bash
cp .env.example .env
```

## Stopping

```bash
docker compose down        # stop containers
docker compose down -v     # stop and remove volumes
```
