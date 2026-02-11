import { COLUMN_TYPES } from "@lakesync/core";

/** Maximum push payload size (1 MiB). */
export const MAX_PUSH_PAYLOAD_BYTES = 1_048_576;

/** Maximum number of deltas allowed in a single push. */
export const MAX_DELTAS_PER_PUSH = 10_000;

/** Maximum number of deltas returned in a single pull. */
export const MAX_PULL_LIMIT = 10_000;

/** Default number of deltas returned in a pull when no limit is specified. */
export const DEFAULT_PULL_LIMIT = 100;

/** Allowed column types for schema validation. Derived from core COLUMN_TYPES. */
export const VALID_COLUMN_TYPES: ReadonlySet<string> = new Set(COLUMN_TYPES);

/** Default maximum buffer size before triggering flush (4 MiB). */
export const DEFAULT_MAX_BUFFER_BYTES = 4 * 1024 * 1024;

/** Default maximum buffer age before triggering flush (30 seconds). */
export const DEFAULT_MAX_BUFFER_AGE_MS = 30_000;
