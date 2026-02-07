# @lakesync/compactor

Background compaction service that merges small delta Parquet files into optimised base snapshots, writes equality delete files for removed rows, and cleans up orphaned storage objects.

## Install

```bash
bun add @lakesync/compactor
```

## Quick usage

### Compactor -- merge delta files into base snapshots

```ts
import { Compactor, DEFAULT_COMPACTION_CONFIG } from "@lakesync/compactor";
import type { LakeAdapter } from "@lakesync/adapter";
import type { TableSchema } from "@lakesync/core";

const schema: TableSchema = { columns: [{ name: "title", type: "string" }] };
const compactor = new Compactor(adapter, DEFAULT_COMPACTION_CONFIG, schema);

const result = await compactor.compact(deltaFileKeys, "tables/todos/base");
if (result.ok) {
  console.log("Compacted", result.value.deltaFilesCompacted, "files");
}
```

### MaintenanceRunner -- compact, expire, and clean in one pass

```ts
import { MaintenanceRunner, DEFAULT_MAINTENANCE_CONFIG } from "@lakesync/compactor";

const runner = new MaintenanceRunner(compactor, adapter, DEFAULT_MAINTENANCE_CONFIG);

const report = await runner.run(deltaFileKeys, "tables/todos/base", "tables/todos/");
if (report.ok) {
  console.log("Orphans removed:", report.value.orphansRemoved);
}
```

### CompactionScheduler -- interval-based automatic maintenance

```ts
import { CompactionScheduler } from "@lakesync/compactor";

const scheduler = new CompactionScheduler(runner, async () => {
  // Return null to skip this tick, or provide task parameters
  return { deltaFileKeys, outputPrefix: "tables/todos/base", storagePrefix: "tables/todos/" };
}, { intervalMs: 30_000 });

scheduler.start();

// Later, gracefully stop (waits for any in-flight run)
await scheduler.stop();
```

## API surface

| Export | Description |
|---|---|
| `Compactor` | Reads delta Parquet files, resolves LWW per row, writes consolidated base and equality delete files |
| `MaintenanceRunner` | Orchestrates compact + orphan removal in a single maintenance cycle |
| `CompactionScheduler` | Wraps a `MaintenanceRunner` on a configurable interval timer with concurrency guard |
| `CompactionConfig` | Thresholds: `minDeltaFiles`, `maxDeltaFiles`, `targetFileSizeBytes` |
| `DEFAULT_COMPACTION_CONFIG` | Defaults: min 10, max 100, target 128 MB |
| `MaintenanceConfig` | Retention settings: `retainSnapshots`, `orphanAgeMs` |
| `DEFAULT_MAINTENANCE_CONFIG` | Defaults: retain 5 snapshots, orphan age 1 hour |
| `SchedulerConfig` | Timer settings: `intervalMs`, `enabled` |
| `DEFAULT_SCHEDULER_CONFIG` | Defaults: 60 s interval, enabled |
| `MaintenanceReport` | Cycle report: `compaction`, `snapshotsExpired`, `orphansRemoved` |
| `CompactionResult` | Compaction stats: files written, files compacted, bytes read/written |

## Testing

```bash
bun test --filter compactor
```

Or from the package directory:

```bash
cd packages/compactor
bun test
```

Tests use [Vitest](https://vitest.dev/) and are co-located in `src/__tests__/`.
