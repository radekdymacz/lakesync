# ADR-003: Merge-on-Read with equality deletes

**Status:** Decided
**Date:** 2026-02-06
**Deciders:** Radek Dymacz

## Context

LakeSync needs a data model for the lakehouse layer that determines how sync deltas are persisted and how current state is reconstructed. The two primary approaches are:

- **Copy-on-write (COW):** Each write rewrites the affected Parquet data files in their entirety, producing a new snapshot. Reads are fast (no merging required) but writes are expensive — a single column change requires rewriting the entire file.
- **Merge-on-read (MOR):** Deltas are appended to separate delta files. Reads merge the base data files with the accumulated deltas to reconstruct current state. Writes are fast (append-only) but reads pay a merging cost.

For a sync engine that processes high volumes of small, incremental changes, write performance is critical. The typical sync workload is many small updates, not bulk rewrites.

## Decision

Merge-on-read with equality deletes. Deltas from the sync protocol are appended to the lake as delta files. Reads merge the base data files with delta files to reconstruct the current state. Equality deletes identify rows by primary key — a delete delta specifies the primary key values of the row to be removed, rather than the file position.

The merge process at read time:

1. Read base data files.
2. Apply delta files in HLC order.
3. For each row (identified by primary key), apply the latest column values.
4. Remove rows with DELETE tombstones.

## Consequences

### Positive

- **Write-optimised for sync workloads:** Appending a delta is an O(1) operation regardless of table size. No data files need to be rewritten.
- **Natural fit for column-level deltas:** The MOR model directly accommodates partial column updates without rewriting full rows.
- **Compatible with Apache Iceberg's merge-on-read model:** Iceberg natively supports MOR with equality deletes, meaning LakeSync's delta format aligns with the Iceberg specification.
- **Preserves full change history:** All deltas are retained until compaction, enabling time-travel queries over the sync log.

### Negative

- **Read amplification:** Queries must merge base files with potentially many delta files. For tables with high update frequency, this can significantly degrade read performance.
- **Periodic compaction required:** To bound read amplification, a background compaction process must periodically merge delta files into base data files. This adds operational complexity.
- **Memory overhead during merges:** The merge process must hold a mapping of primary keys to their latest column values, which can be memory-intensive for large tables.

### Risks

- **Compaction lag:** If compaction falls behind the rate of incoming deltas, read performance degrades progressively. Monitoring and back-pressure mechanisms are needed.
- **Equality delete performance:** Equality deletes require scanning base data files to find matching primary keys, which is slower than positional deletes. Bloom filters on primary key columns can mitigate this.
- **Orphaned deltas:** If a compaction process crashes mid-way, orphaned delta files may remain. Iceberg's snapshot isolation protects against inconsistency, but cleanup is needed.

## Alternatives Considered

- **Copy-on-write:** Rejected because rewriting entire Parquet files for each sync delta is prohibitively expensive. A single column change in a 1GB data file would require rewriting the entire file.
- **Hybrid (COW for small tables, MOR for large):** Rejected to avoid the complexity of two storage strategies. MOR works acceptably for small tables (minimal merge overhead when few deltas exist).
- **Log-structured merge tree (LSM):** Rejected as it introduces a separate storage engine. Iceberg's native MOR support provides equivalent write optimisation within the lakehouse ecosystem.
- **Positional deletes (instead of equality deletes):** Rejected because positional deletes are fragile — they reference file-specific row positions that change during compaction. Equality deletes are position-independent and compatible with concurrent writers.
