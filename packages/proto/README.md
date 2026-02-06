# @lakesync/proto

Protobuf schema and codec for the LakeSync wire protocol. Handles serialisation and deserialisation of sync messages with a `BigInt`-to-`fixed64` bridge for HLC timestamps. Column values are encoded as UTF-8 JSON bytes, ensuring arbitrary serialisable values survive the round trip. All codec functions return `Result<T, CodecError>` and never throw.

## Install

```bash
bun add @lakesync/proto
```

## Quick usage

### Encode and decode a RowDelta

```ts
import { encodeRowDelta, decodeRowDelta } from "@lakesync/proto";
import type { RowDelta } from "@lakesync/core";

// Encode a core RowDelta to protobuf binary
const encodeResult = encodeRowDelta(delta);
if (encodeResult.ok) {
  const bytes: Uint8Array = encodeResult.value;
  console.log("Encoded size:", bytes.byteLength);

  // Decode back to a core RowDelta
  const decodeResult = decodeRowDelta(bytes);
  if (decodeResult.ok) {
    console.log("Round-tripped delta:", decodeResult.value);
  }
}
```

### Encode and decode SyncPush

```ts
import { encodeSyncPush, decodeSyncPush } from "@lakesync/proto";
import type { SyncPushPayload } from "@lakesync/proto";
import { HLC } from "@lakesync/core";

const clock = new HLC();

const payload: SyncPushPayload = {
  clientId: "client-1",
  deltas: [/* RowDelta[] */],
  lastSeenHlc: clock.now(),
};

// Encode for wire transmission
const encoded = encodeSyncPush(payload);
if (encoded.ok) {
  // Send encoded.value over the network...

  // On the receiving end, decode
  const decoded = decodeSyncPush(encoded.value);
  if (decoded.ok) {
    console.log("Client:", decoded.value.clientId);
    console.log("Deltas:", decoded.value.deltas.length);
  }
}
```

### Encode and decode SyncPull / SyncResponse

```ts
import {
  encodeSyncPull,
  decodeSyncPull,
  encodeSyncResponse,
  decodeSyncResponse,
} from "@lakesync/proto";
import { HLC } from "@lakesync/core";

const clock = new HLC();

// Encode a pull request
const pullEncoded = encodeSyncPull({
  clientId: "client-1",
  sinceHlc: HLC.encode(0, 0),
  maxDeltas: 100,
});

// Encode a sync response
const responseEncoded = encodeSyncResponse({
  deltas: [/* RowDelta[] */],
  serverHlc: clock.now(),
  hasMore: false,
});

if (responseEncoded.ok) {
  const decoded = decodeSyncResponse(responseEncoded.value);
  if (decoded.ok) {
    console.log("Server HLC:", decoded.value.serverHlc);
    console.log("Has more:", decoded.value.hasMore);
  }
}
```

## API surface

### Codec functions

| Export | Description |
|---|---|
| `encodeRowDelta(delta)` | Serialise a core `RowDelta` to protobuf binary |
| `decodeRowDelta(bytes)` | Deserialise protobuf binary to a core `RowDelta` |
| `encodeSyncPush(payload)` | Serialise a `SyncPushPayload` to protobuf binary |
| `decodeSyncPush(bytes)` | Deserialise protobuf binary to a `SyncPushPayload` |
| `encodeSyncPull(payload)` | Serialise a `SyncPullPayload` to protobuf binary |
| `decodeSyncPull(bytes)` | Deserialise protobuf binary to a `SyncPullPayload` |
| `encodeSyncResponse(payload)` | Serialise a `SyncResponsePayload` to protobuf binary |
| `decodeSyncResponse(bytes)` | Deserialise protobuf binary to a `SyncResponsePayload` |

### Payload types

| Export | Description |
|---|---|
| `SyncPushPayload` | `{ clientId, deltas, lastSeenHlc }` using core domain types |
| `SyncPullPayload` | `{ clientId, sinceHlc, maxDeltas }` using core domain types |
| `SyncResponsePayload` | `{ deltas, serverHlc, hasMore }` using core domain types |

### Error

| Export | Description |
|---|---|
| `CodecError` | Error returned when encoding or decoding fails, with `code: 'CODEC_ERROR'` |

### Re-exported Protobuf schemas (from generated code)

| Export | Description |
|---|---|
| `ProtoDeltaOp` | Protobuf enum for delta operation types |
| `ColumnDeltaSchema` | Protobuf schema for column deltas |
| `RowDeltaSchema` | Protobuf schema for row deltas |
| `SyncPushSchema` | Protobuf schema for sync push messages |
| `SyncPullSchema` | Protobuf schema for sync pull messages |
| `SyncResponseSchema` | Protobuf schema for sync response messages |
| `DeltaOpSchema` | Protobuf schema for the DeltaOp enum |
| `ProtoColumnDelta` | Protobuf-generated column delta type |
| `ProtoRowDelta` | Protobuf-generated row delta type |
| `ProtoSyncPush` | Protobuf-generated sync push type |
| `ProtoSyncPull` | Protobuf-generated sync pull type |
| `ProtoSyncResponse` | Protobuf-generated sync response type |

## Testing

```bash
bun test --filter proto
```

Or from the package directory:

```bash
cd packages/proto
bun test
```

Tests use [Vitest](https://vitest.dev/). The protobuf schema is defined in `src/lakesync.proto` and generated code lives in `src/gen/`.
