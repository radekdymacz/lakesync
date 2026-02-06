# ADR-004: Iceberg REST Catalogue specification

**Status:** Decided
**Date:** 2026-02-06
**Deciders:** Radek Dymacz

## Context

LakeSync needs a catalogue to manage table metadata, snapshots, and schema evolution in the lakehouse layer. The catalogue is the central registry that tracks which tables exist, their current schemas, and their snapshot history. The candidates are:

- **Hive Metastore:** The traditional Hadoop catalogue. Mature but heavyweight — requires a relational database (MySQL/PostgreSQL) and a Thrift service. Tightly coupled to the Hadoop ecosystem.
- **AWS Glue:** Managed catalogue service from AWS. Convenient but vendor-locked — not usable outside AWS.
- **Custom catalogue:** Purpose-built for LakeSync. Maximum flexibility but requires building and maintaining metadata management, concurrency control, and schema evolution from scratch.
- **Iceberg REST Catalogue specification:** An open standard REST API for catalogue operations. Multiple implementations available (Nessie, Polaris, Gravitino). Vendor-neutral and language-agnostic.

## Decision

Use the Iceberg REST catalogue specification with Nessie as the catalogue implementation.

The Iceberg REST specification defines a standard HTTP API for catalogue operations (create/list/load/drop tables, commit snapshots, manage namespaces). Nessie implements this specification and adds Git-like version control semantics for data — branches, tags, merges, and commit history.

Key configuration:
- Nessie serves the Iceberg REST API on a configurable HTTP port.
- Tables are organised into namespaces (e.g., `lakesync.{tenantId}`).
- Each commit to Nessie is atomic and isolated.
- Nessie supports in-memory storage (for testing) and persistent backends (RocksDB, DynamoDB, PostgreSQL).

## Consequences

### Positive

- **Git-like branching for data:** Nessie enables creating branches of the data lake, which is useful for time travel, experimentation, and staging changes before merging to the main branch.
- **Standards compliance:** The Iceberg REST specification is an open standard supported by multiple engines (Spark, Trino, Flink, DuckDB). LakeSync's data is accessible to any engine that speaks the REST catalogue protocol.
- **Vendor neutrality:** Not locked into any cloud provider. Nessie runs anywhere — local development, on-premises, or any cloud.
- **Atomic commits:** Nessie provides optimistic concurrency control — concurrent writers are serialised, and conflicts are detected at commit time.
- **Open source:** Nessie is Apache-2.0 licensed with an active community.

### Negative

- **Additional service to operate:** Nessie is a separate JVM-based service that must be deployed, monitored, and maintained alongside LakeSync.
- **JVM dependency:** Nessie is written in Java/Quarkus. This introduces a JVM dependency into an otherwise TypeScript/Node.js stack.
- **Learning curve:** The Git-like branching model adds conceptual complexity that may not be needed in v1.0 but is valuable for future features.

### Risks

- **Nessie availability:** If Nessie is down, no catalogue operations can proceed (table creation, snapshot commits). High availability deployment (multiple replicas behind a load balancer) mitigates this.
- **Version compatibility:** The Iceberg REST specification is still evolving. Breaking changes between specification versions could require catalogue client updates.
- **Nessie persistence:** In-memory mode loses all metadata on restart. Production deployments must use a persistent backend, adding another infrastructure dependency.

## Alternatives Considered

- **Hive Metastore:** Rejected due to heavyweight infrastructure requirements (relational database + Thrift service) and tight coupling to the Hadoop ecosystem. LakeSync targets cloud-native and edge deployments where a full Hadoop stack is impractical.
- **AWS Glue:** Rejected due to vendor lock-in. LakeSync must be deployable on any cloud or on-premises.
- **Custom catalogue:** Rejected because building correct metadata management, concurrency control, and schema evolution is a significant engineering effort. The Iceberg REST specification provides all of this as a standard, with multiple production-tested implementations.
- **SQLite-based local catalogue:** Considered for single-node development but rejected because it does not support concurrent access from multiple processes and lacks the versioning semantics Nessie provides.
