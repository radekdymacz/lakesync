# LakeSync

## Monorepo
TurboRepo + Bun. Packages in `packages/`, apps in `apps/`.

## Code Style
- TypeScript strict mode, no `any`
- Functional style where practical; classes for stateful components (DO, client)
- Result<T, E> pattern — never throw from public APIs
- JSDoc on all public APIs
- British English in comments and docs (serialise, initialise, synchronise, catalogue, behaviour)
- Vitest for testing, co-located in `__tests__/`

## Task Execution
Read plans/PLAN.md for task breakdown.
For PARALLEL GROUPs: launch all tasks as parallel Task subagents.
For SEQUENTIAL tasks: execute one at a time.

## Hard Rules
- NEVER use localStorage or sessionStorage — use OPFS or IndexedDB
- NEVER throw exceptions from public APIs — use Result<T, E>
- NEVER flush per-sync to Iceberg — always batch
- NEVER suggest PostgreSQL as a backend
- NEVER use `any` type
- NEVER create custom subagents — use built-in Task tool only
