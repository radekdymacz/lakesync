# Phase 2A — Parquet Writer

**Goal:** Replace JSON flush envelopes with Apache Parquet files, enabling Iceberg-compatible data lake output.

**Depends on:** Phase 1 (complete)
**Blocks:** 2B (Iceberg Catalogue), 2D (Cloudflare Workers)

---

## PARALLEL GROUP 1

### Task 2A.1: Arrow schema mapping

- **Package:** `packages/core/src/parquet/`
- **Creates:**
  - `packages/core/src/parquet/schema.ts`
  - `packages/core/src/parquet/__tests__/schema.test.ts`
- **Modifies:**
  - `packages/core/src/index.ts` (add parquet exports)
- **Dependencies:** none
- **Implementation:**
  1. Install `apache-arrow` as dependency in `packages/core/package.json`
  2. Create `schema.ts` with two exported functions:
     - `buildArrowSchema(schema: TableSchema): arrow.Schema`
       - System columns (always present):
         - `op` → `arrow.Utf8` (DeltaOp string)
         - `table` → `arrow.Utf8`
         - `rowId` → `arrow.Utf8`
         - `clientId` → `arrow.Utf8`
         - `hlc` → `arrow.Int64` (bigint preserved as Int64)
         - `deltaId` → `arrow.Utf8`
       - User columns from `TableSchema.columns`:
         - `string` → `arrow.Utf8`
         - `number` → `arrow.Float64`
         - `boolean` → `arrow.Bool`
         - `json` → `arrow.Utf8` (JSON-serialised)
         - `null` → `arrow.Utf8`, nullable
     - `deltasToArrowTable(deltas: RowDelta[], schema: TableSchema): arrow.Table`
       - Build column vectors from delta arrays
       - Map `ColumnDelta[]` values into typed arrays per column
       - Missing columns in a delta → `null` for that row
  3. Re-export from `packages/core/src/index.ts`
- **Tests:**
  - Schema generation: system columns present, user columns typed correctly
  - Table conversion: 5 mixed deltas (INSERT/UPDATE/DELETE) → Arrow Table with correct row count
  - Empty deltas → empty table with correct schema
  - HLC bigint survives roundtrip through Int64
  - `json` type column → JSON.stringify'd Utf8 value
- **Done when:** `buildArrowSchema` and `deltasToArrowTable` pass all tests, `bun run build` succeeds

---

### Task 2A.2: Parquet write/read module

- **Package:** `packages/parquet/` (new package)
- **Creates:**
  - `packages/parquet/package.json`
  - `packages/parquet/tsconfig.json`
  - `packages/parquet/vitest.config.ts`
  - `packages/parquet/src/index.ts`
  - `packages/parquet/src/writer.ts`
  - `packages/parquet/src/reader.ts`
  - `packages/parquet/src/__tests__/roundtrip.test.ts`
- **Modifies:** none
- **Dependencies:** Task 2A.1 (uses `deltasToArrowTable`, `buildArrowSchema`)
- **Implementation:**
  1. Create new package scaffolding:
     ```json
     {
       "name": "@lakesync/parquet",
       "version": "0.0.1",
       "type": "module",
       "exports": { ".": "./src/index.ts", "./*": "./src/*.ts" },
       "scripts": {
         "build": "tsc --noEmit",
         "test": "vitest run",
         "typecheck": "tsc --noEmit"
       },
       "dependencies": {
         "@lakesync/core": "workspace:*",
         "parquet-wasm": "^0.7.0",
         "apache-arrow": "^18.0.0"
       },
       "devDependencies": {
         "typescript": "^5.7.0",
         "vitest": "^3.0.0"
       }
     }
     ```
  2. `tsconfig.json` extending `../../tsconfig.base.json`
  3. `vitest.config.ts` matching other packages
  4. `src/writer.ts`:
     ```typescript
     export async function writeDeltasToParquet(
       deltas: RowDelta[],
       schema: TableSchema,
     ): Promise<Result<Uint8Array, FlushError>>
     ```
     - Call `deltasToArrowTable(deltas, schema)` → Arrow Table
     - `arrow.tableToIPC(table, 'stream')` → IPC stream bytes
     - `parquet_wasm.Table.fromIPCStream(ipcBytes)` → Parquet WASM Table
     - Configure WriterProperties: Snappy compression
     - `parquet_wasm.writeParquet(wasmTable, writerProps)` → `Uint8Array`
     - Wrap in `Result`; catch WASM errors → `FlushError`
  5. `src/reader.ts`:
     ```typescript
     export async function readParquetToDeltas(
       data: Uint8Array,
     ): Promise<Result<RowDelta[], FlushError>>
     ```
     - `parquet_wasm.readParquet(data)` → Parquet WASM Table
     - `.intoIPCStream()` → IPC bytes
     - `arrow.tableFromIPC(ipcBytes)` → Arrow Table
     - Iterate rows → reconstruct `RowDelta[]` (map Int64 back to `HLCTimestamp`)
     - Wrap in `Result`
  6. `src/index.ts`: re-export `writeDeltasToParquet` and `readParquetToDeltas`
  7. Note: `parquet-wasm` requires WASM initialisation. In Node/Bun test env, use `await parquet_wasm.default()` or the auto-init path. Add a module-level `initParquetWasm()` helper if needed.
- **Tests:**
  - Roundtrip: create 10 deltas → write → read → assert deep equality
  - Large batch: 1000 deltas → write + read (no crash, correct count)
  - Mixed column types: string, number, boolean, json columns all preserved
  - HLC bigint: `HLCTimestamp` survives write → read as branded bigint
  - DELETE op: columns array empty, still roundtrips
- **Done when:** All roundtrip tests pass, package builds, re-exported from index

---

## SEQUENTIAL (after Group 1)

### Task 2A.3: Gateway Parquet flush

- **Package:** `packages/gateway/`
- **Creates:**
  - `packages/gateway/src/__tests__/parquet-flush.test.ts`
- **Modifies:**
  - `packages/gateway/src/types.ts` — add `flushFormat` to `GatewayConfig`
  - `packages/gateway/src/gateway.ts` — modify `flush()` method (lines 137–193)
  - `packages/gateway/package.json` — add `@lakesync/parquet` dependency
  - `packages/gateway/src/__tests__/gateway.test.ts` — add `flushFormat: 'json'` to existing tests
- **Dependencies:** Task 2A.1 + 2A.2
- **Implementation:**
  1. Add to `GatewayConfig` in `types.ts`:
     ```typescript
     flushFormat?: "json" | "parquet"; // default: "parquet"
     ```
  2. Add `tableSchema?: TableSchema` to `GatewayConfig` — needed for Parquet schema mapping. Eventually this will come from a schema registry; for now, pass explicitly.
  3. Modify `flush()` in `gateway.ts` (currently lines 137–193):
     - After building the entries array and HLC range:
     - If `flushFormat === 'json'` (or no parquet dependency): keep existing JSON path
     - If `flushFormat === 'parquet'` (default):
       - Call `writeDeltasToParquet(entries, this.config.tableSchema!)` → `Result<Uint8Array, FlushError>`
       - On error, restore buffer, return `Err`
       - Object key: `deltas/{YYYY-MM-DD}/{gatewayId}/{minHlc}-{maxHlc}.parquet`
       - Content type: `application/vnd.apache.parquet`
       - Call `this.adapter.putObject(objectKey, parquetBytes, contentType)`
  4. Update existing gateway tests: explicitly set `flushFormat: 'json'` so they continue to pass unchanged
  5. Write new test `parquet-flush.test.ts`:
     - Flush with Parquet format → adapter receives `.parquet` key and binary data
     - Read back with `readParquetToDeltas()` → assert equality with original deltas
- **Tests:**
  - Existing JSON flush tests still pass with explicit `flushFormat: 'json'`
  - Parquet flush produces `.parquet` object key
  - Parquet flush data can be read back with `readParquetToDeltas()`
  - Flush error handling: adapter failure → buffer restored
- **Done when:** All gateway tests pass (JSON + Parquet), `bun run build` succeeds

---

### Task 2A.4: Integration validation

- **Package:** `tests/integration/`
- **Creates:**
  - `tests/integration/parquet-flush.test.ts`
- **Modifies:**
  - `.github/workflows/ci.yml` (if parquet-wasm needs special handling)
- **Dependencies:** Task 2A.3
- **Implementation:**
  1. Create `tests/integration/parquet-flush.test.ts`:
     - Setup: MinIO adapter, SyncGateway with `flushFormat: 'parquet'`
     - Push 50 deltas → flush → list objects in MinIO → assert `.parquet` file exists
     - Read the `.parquet` file back via adapter → `readParquetToDeltas()` → assert equality
  2. Optionally: if DuckDB CLI is available in CI, add a cross-tool validation:
     - `duckdb -c "SELECT count(*) FROM read_parquet('s3://lakesync-dev/deltas/...')"` via MinIO S3 endpoint
     - Skip this test if `duckdb` is not on PATH (`describe.skipIf`)
  3. Check if `parquet-wasm` requires special WASM init in CI:
     - If Bun handles it natively, no changes needed
     - If not, may need to add a setup step or environment variable
- **Tests:**
  - End-to-end: push → flush → MinIO → read back → equality
  - Object key follows `deltas/{date}/{gatewayId}/{hlcRange}.parquet` pattern
  - Cross-tool DuckDB read (optional, skip if unavailable)
- **Done when:** Integration tests pass in CI, Parquet files are readable by external tools
