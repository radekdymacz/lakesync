# LakeSync Todo App

A minimal reference implementation demonstrating the LakeSync sync pipeline.

## Quick Start

```bash
# From the repository root
bun install

# Start MinIO (optional — for persistent storage)
docker compose -f docker/docker-compose.yml up -d

# Start the dev server
cd apps/examples/todo-app
bun run dev
```

Open http://localhost:5173

## How It Works

1. **Add a todo** — creates an INSERT delta
2. **Toggle completed** — creates an UPDATE delta (only the `completed` column)
3. **Delete a todo** — creates a DELETE delta
4. **Flush** — pushes buffered deltas from the gateway to MinIO

Each change:
- Extracts a column-level delta (only changed fields)
- Queues it in the in-memory sync queue
- Pushes it to the in-process sync gateway
- The gateway resolves conflicts and buffers deltas
- On flush, deltas are written to MinIO as a JSON envelope

## Architecture

```
User Action -> TodoDB (Map) -> extractDelta() -> MemoryQueue -> SyncGateway -> MinIOAdapter -> MinIO
```

No network calls — the gateway runs in-process. Phase 2 will add real network sync.
