# ADR-008: Protobuf wire protocol

**Status:** Decided
**Date:** 2026-02-06
**Deciders:** Radek Dymacz

## Context

LakeSync needs a serialisation format for the sync protocol between clients and the gateway. The wire protocol carries sync deltas, acknowledgements, and control messages. The format must be compact (sync traffic may traverse mobile networks), support schema evolution (the protocol will evolve across versions), and have good TypeScript support. The candidates are:

- **JSON:** Human-readable, universal support, no schema. Verbose — field names repeated in every message. No built-in schema evolution guarantees.
- **MessagePack:** Binary JSON — more compact than JSON but still schemaless. No schema evolution guarantees beyond what JSON provides.
- **Protocol Buffers (Protobuf):** Binary format with a schema definition language (`.proto` files). Strong backward/forward compatibility guarantees. Widely adopted with mature tooling.
- **FlatBuffers:** Zero-copy binary format. Extremely fast deserialisation but complex schema definition and less mature TypeScript support.
- **Cap'n Proto:** Zero-copy with RPC support. Excellent performance but limited TypeScript ecosystem and more complex than Protobuf.

## Decision

Protocol Buffers v3 (proto3) with `protobuf-es` for TypeScript code generation.

Key encoding decisions:
- **HLC timestamps:** Encoded as `fixed64` (exact 8-byte representation). `fixed64` is used instead of `uint64` (varint) because HLC values are always large (48-bit wall clock + 16-bit counter), making varint encoding inefficient (varint would use 8-9 bytes anyway, but with variable-length overhead). `protobuf-es` handles `fixed64` ↔ `BigInt` conversion natively.
- **Column values:** Encoded as `bytes` containing UTF-8 JSON. Each column value is serialised as a JSON string and stored as raw bytes in the Protobuf message. This avoids the complexity of Protobuf's `Any` or `Struct` types while supporting arbitrary column types (strings, numbers, booleans, nested objects, arrays).
- **Delta identity:** The `deltaId` field is a 32-byte `bytes` field containing the SHA-256 hash (see ADR-009).

Example message structure:
```protobuf
message RowDelta {
  bytes delta_id = 1;           // 32-byte SHA-256
  string client_id = 2;
  fixed64 hlc = 3;             // 64-bit HLC timestamp
  string table = 4;
  string row_id = 5;
  map<string, bytes> columns = 6;  // column name → JSON-encoded value
  DeltaType type = 7;
}
```

## Consequences

### Positive

- **Compact binary format:** 2-10x smaller than equivalent JSON, reducing bandwidth consumption for sync traffic over constrained networks.
- **Strong schema evolution guarantees:** Proto3 supports adding new fields, removing optional fields, and renaming fields (by number) without breaking existing clients. Forward and backward compatibility is built into the format.
- **Type-safe code generation:** `protobuf-es` generates TypeScript classes with full type information from `.proto` definitions. No manual serialisation/deserialisation code.
- **`BigInt` ↔ `fixed64` bridge:** `protobuf-es` handles the JavaScript `BigInt` to Protobuf `fixed64` conversion natively, avoiding manual byte manipulation for HLC timestamps.
- **Wide ecosystem:** Protobuf is supported in virtually every language, making it straightforward to implement LakeSync clients in languages beyond TypeScript in the future.

### Negative

- **Not human-readable:** Binary format cannot be inspected with standard text tools. Debugging requires Protobuf-aware tooling (e.g., `protoc --decode`, Wireshark plugins).
- **JSON-in-bytes for column values:** Encoding column values as JSON inside Protobuf bytes is not maximally efficient — a purpose-built typed encoding would be more compact. This is a pragmatic tradeoff for v1.0 simplicity.
- **Build step required:** `.proto` files must be compiled to TypeScript before use, adding a code generation step to the build process.
- **`protobuf-es` dependency:** Ties the TypeScript implementation to a specific Protobuf library. However, `protobuf-es` is actively maintained by the Buf team and is the recommended library for modern TypeScript/ES module usage.

### Risks

- **Proto3 default values:** Proto3 does not distinguish between "field not set" and "field set to default value" (0 for numbers, empty string for strings). This must be accounted for in the delta format — a column set to `0` must be distinguishable from an absent column. Using `map<string, bytes>` for columns avoids this issue (absent keys are simply not in the map).
- **JSON-in-bytes performance:** Serialising column values as JSON and then wrapping in Protobuf bytes adds double-encoding overhead. For Phase 2, typed column encoding (Protobuf `oneof` with native types) may be adopted for performance-critical paths.
- **Protobuf versioning:** The `.proto` files themselves must be version-controlled and evolved carefully. Field numbers must never be reused, even after deletion.

## Alternatives Considered

- **JSON:** Rejected due to verbosity (field names in every message) and lack of schema evolution guarantees. JSON is 2-10x larger than Protobuf for typical sync payloads.
- **MessagePack:** Rejected because, while more compact than JSON, it remains schemaless and does not provide backward/forward compatibility guarantees. Schema evolution would require custom versioning logic.
- **FlatBuffers:** Rejected due to less mature TypeScript support and more complex schema definition. The zero-copy advantage is less relevant for LakeSync's message sizes (typically <100KB).
- **Cap'n Proto:** Rejected due to limited TypeScript ecosystem. The zero-copy and RPC features are compelling but not worth the integration risk for v1.0.
- **Avro:** Considered for its schema-in-header approach (useful for Parquet compatibility) but rejected for the wire protocol because Protobuf's per-message schema evolution is more suitable for a request/response protocol than Avro's per-file schema approach.
