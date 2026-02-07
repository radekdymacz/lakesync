# @lakesync/analyst

Analytical query engine powered by DuckDB-Wasm for querying LakeSync lakehouse data. Provides hot/cold union reads across in-memory rows and Parquet files, and time-travel queries that materialise column-level LWW state at any HLC timestamp. All public methods return `Result<T, E>` and never throw.

## Install

```bash
bun add @lakesync/analyst
```

## Quick usage

### DuckDBClient -- query engine wrapper

```ts
import { DuckDBClient } from "@lakesync/analyst";

const client = new DuckDBClient({ threads: 1 });
const init = await client.init();
if (!init.ok) throw init.error;

// Run a SQL query
const result = await client.query<{ answer: number }>("SELECT 42 AS answer");
if (result.ok) console.log(result.value); // [{ answer: 42 }]

// Register an in-memory Parquet buffer for querying
await client.registerParquetBuffer("data.parquet", parquetBytes);
await client.query("SELECT * FROM 'data.parquet'");

// Register a remote Parquet file by URL
await client.registerParquetUrl("remote.parquet", "https://example.com/data.parquet");

await client.close();
```

### UnionReader -- merge hot and cold data

```ts
import { DuckDBClient, UnionReader } from "@lakesync/analyst";

const client = new DuckDBClient();
await client.init();

const reader = new UnionReader({ duckdb: client, tableName: "todos" });
await reader.registerColdData([{ name: "batch-1.parquet", data: parquetBytes }]);

// Query across cold Parquet files and hot in-memory rows
const hotRows = [{ id: "row-3", title: "New task", completed: false }];
const result = await reader.query("SELECT * FROM _union WHERE completed = true", hotRows);

// Query cold data only
const cold = await reader.queryColdOnly("SELECT COUNT(*) AS n FROM _union");
```

### TimeTraveller -- point-in-time queries

```ts
import { DuckDBClient, TimeTraveller } from "@lakesync/analyst";

const client = new DuckDBClient();
await client.init();

const traveller = new TimeTraveller({ duckdb: client });
await traveller.registerDeltas([{ name: "deltas.parquet", data: deltaBytes }]);

// Materialise state as of a specific HLC timestamp
const rows = await traveller.materialiseAsOf(hlcTimestamp);

// Query materialised state with custom SQL (use _state as the table)
const filtered = await traveller.queryAsOf(hlcTimestamp, "SELECT * FROM _state WHERE completed = true");

// Query raw deltas within a time range (use _deltas as the table)
const changelog = await traveller.queryBetween(fromHlc, toHlc, "SELECT * FROM _deltas ORDER BY hlc");
```

## API surface

| Export | Description |
|---|---|
| `DuckDBClient` | DuckDB-Wasm wrapper with `init()`, `query()`, `registerParquetBuffer()`, `registerParquetUrl()`, `close()` |
| `DuckDBClientConfig` | Configuration: `logger` (boolean), `threads` (number) |
| `UnionReader` | Merges hot in-memory rows with cold Parquet data via `UNION ALL BY NAME` |
| `UnionReadConfig` | Configuration: `duckdb` (DuckDBClient), `tableName` (string) |
| `TimeTraveller` | Point-in-time materialisation and range queries over delta Parquet files |
| `TimeTravelConfig` | Configuration: `duckdb` (DuckDBClient) |

## Testing

```bash
bun test --filter analyst
```

Or from the package directory:

```bash
cd packages/analyst
bun test
```

Tests use [Vitest](https://vitest.dev/) and are located in `src/__tests__/`.
