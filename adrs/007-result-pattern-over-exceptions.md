# ADR-007: Result pattern over thrown exceptions

**Status:** Decided
**Date:** 2026-02-06
**Deciders:** Radek Dymacz

## Context

LakeSync needs a consistent error handling strategy across its codebase. As a sync engine where data integrity is paramount, unhandled errors can lead to data loss, corruption, or silent inconsistencies. The candidates are:

- **Thrown exceptions (JavaScript default):** Errors are thrown and caught with `try/catch`. This is the idiomatic JavaScript approach but makes error paths invisible — any function call might throw, and the compiler does not enforce handling.
- **Result<T, E> discriminated union:** Functions return a tagged union — either `{ ok: true, value: T }` or `{ ok: false, error: E }`. The caller must inspect the tag before accessing the value.
- **Either monad:** A monadic type with `Left` (error) and `Right` (success) variants, supporting `map`, `flatMap`, and other functional composition. Powerful but introduces functional programming concepts that may be unfamiliar to the team.

## Decision

All public APIs return `Result<T, E>` instead of throwing exceptions. The type is defined as a discriminated union:

```typescript
type Result<T, E extends LakeSyncError = LakeSyncError> =
  | { ok: true; value: T }
  | { ok: false; error: E };
```

Key conventions:
- **Public APIs** (anything exported from a package or module boundary) must return `Result<T, E>`.
- **Internal code** may throw for programming errors (assertions, invariant violations) using `assert()` — these indicate bugs, not expected error conditions.
- **Error hierarchy:** All errors extend a base `LakeSyncError` class with a `code` property (string enum) and a human-readable `message`. Specific subclasses provide additional context (e.g., `DriftExceededError` includes the measured drift).
- **Async functions** return `Promise<Result<T, E>>`, never reject the promise.

## Consequences

### Positive

- **Explicit error handling at every call site:** The caller must check `result.ok` before accessing `result.value`. Forgotten error handling is visible in code review and can be caught by linting rules.
- **TypeScript compiler assistance:** The discriminated union ensures that accessing `result.value` without checking `result.ok` produces a type error (with strict null checks enabled).
- **No surprise crashes from unhandled rejections:** Since async functions never reject, there are no `UnhandledPromiseRejection` crashes. Every error is captured in the `Result` type.
- **Error codes enable programmatic handling:** Each error has a stable string code (e.g., `DRIFT_EXCEEDED`, `DELTA_CONFLICT`) that can be matched in switch statements without relying on error message strings.
- **Composable:** Results can be chained with utility functions (`mapResult`, `flatMapResult`) for clean pipelines without deeply nested `if` statements.

### Negative

- **More verbose than try/catch:** Every function call that can fail requires an `if (!result.ok)` check. This adds visual noise compared to the "happy path" style of exception-based code.
- **Learning curve:** Developers accustomed to JavaScript's exception model need to adopt the Result pattern consistently. Inconsistent adoption (some functions throw, some return Results) creates confusion.
- **No stack traces by default:** `Result` errors are values, not thrown exceptions, so they do not automatically capture stack traces. The `LakeSyncError` base class explicitly captures `Error.stack` in its constructor to mitigate this.

### Risks

- **Inconsistent adoption:** If some modules use exceptions and others use Results, the boundary between them becomes a source of bugs. Linting rules and code review must enforce consistency.
- **Performance overhead:** Creating error objects (with stack traces) for expected error conditions (e.g., "delta already exists") has a performance cost. For hot paths, error objects should be pre-allocated or stack capture disabled.
- **Third-party library boundaries:** External libraries throw exceptions. Every call to a third-party library must be wrapped in a try/catch that converts exceptions to Result values. Missing a wrapper silently reintroduces thrown exceptions.

## Alternatives Considered

- **Thrown exceptions (JavaScript default):** Rejected because error paths are invisible to the compiler. In a sync engine, an unhandled error during conflict resolution or delta application can silently corrupt data. The risk of "forgot to add a try/catch" is unacceptable.
- **Either monad:** Rejected because it introduces functional programming abstractions (`map`, `flatMap`, `fold`) that add cognitive overhead for a team primarily working in imperative TypeScript. The simpler `Result` discriminated union provides the same safety guarantees with more familiar syntax.
- **Neverthrow library:** Considered as a pre-built Result implementation. Rejected in favour of a minimal custom implementation to avoid external dependencies in core data structures and to keep the type as simple as possible.
- **Go-style tuple returns `[value, error]`:** Rejected because TypeScript's type system cannot enforce that the caller checks the error element before using the value element. The discriminated union provides stronger type safety.
