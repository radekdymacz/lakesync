# ADR-001: Column-level delta granularity

**Status:** Decided
**Date:** 2026-02-06
**Deciders:** Radek Dymacz

## Context

LakeSync needs to decide on the granularity of change tracking for synchronisation deltas. The three candidates are:

- **Row-level:** Each change records the entire row. Simple but wastes bandwidth on wide tables — a single column change forces retransmission of every column.
- **Column-level:** Each change records only the modified columns plus the row identifier. Balanced approach.
- **Cell-level:** Each change records individual cell mutations with full positional metadata. Maximum precision but adds excessive metadata overhead (column name, type info, position per cell).

For typical analytics and operational tables with 20-200+ columns, row-level deltas would transmit orders of magnitude more data than necessary. Cell-level deltas would require per-cell metadata that often exceeds the size of the values themselves.

## Decision

Column-level deltas. Each change record (RowDelta) contains only the modified columns alongside the row's primary key. Unmodified columns are omitted entirely from the delta payload.

This provides the optimal balance between bandwidth efficiency and metadata overhead. The delta structure is:

```
RowDelta {
  rowId: PrimaryKey
  hlc: HLC timestamp
  columns: Map<columnName, newValue>  // only modified columns
  type: INSERT | UPDATE | DELETE
}
```

## Consequences

### Positive

- Reduces sync payload by 10-100x for wide tables compared to row-level deltas.
- Enables column-level conflict resolution (see ADR-002), where independent column changes can be merged without conflict.
- Naturally fits the merge-on-read data model (see ADR-003) — partial column updates are appended as lightweight deltas.
- Lower bandwidth consumption improves sync performance over constrained networks.

### Negative

- Requires column-aware comparison logic to detect which columns actually changed. This uses `fast-deep-equal` for nested objects and arrays.
- Column ordering within deltas must be deterministic to support deterministic delta identity (see ADR-009). Keys are sorted lexicographically.
- INSERT deltas must include all columns (no "previous state" to diff against), so the bandwidth benefit is primarily for UPDATE operations.

### Risks

- Schema evolution (column renames, type changes) must be handled carefully — a renamed column could appear as a delete of the old column and insert of a new one.
- Very narrow tables (1-3 columns) see minimal benefit from column-level tracking but still pay the overhead of column-aware diffing.

## Alternatives Considered

- **Row-level deltas:** Rejected due to excessive bandwidth waste on wide tables. A single column change in a 100-column table would transmit 99 unchanged columns.
- **Cell-level deltas:** Rejected because the per-cell metadata (column name, type, position) often exceeds the value size. The additional granularity over column-level provides negligible benefit for conflict resolution.
- **Hybrid (row-level for narrow tables, column-level for wide):** Rejected to avoid the complexity of two code paths. Column-level works acceptably for narrow tables with minimal overhead.
