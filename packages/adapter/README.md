# @lakesync/adapter

Lake adapter interface and MinIO/S3-compatible implementation. Provides a `Result`-returning abstraction over object storage operations, ensuring that all public methods return typed results and never throw. The `LakeAdapter` interface can be implemented for any object store backend; the included `MinIOAdapter` wraps the AWS S3 SDK with `forcePathStyle` for MinIO compatibility.

## Install

```bash
bun add @lakesync/adapter
```

## Quick usage

### Create a MinIOAdapter

```ts
import { MinIOAdapter, type AdapterConfig } from "@lakesync/adapter";

const config: AdapterConfig = {
  endpoint: "http://localhost:9000",
  bucket: "lakesync",
  region: "us-east-1", // optional, defaults to us-east-1
  credentials: {
    accessKeyId: "minioadmin",
    secretAccessKey: "minioadmin",
  },
};

const adapter = new MinIOAdapter(config);
```

### Store and retrieve objects

```ts
// Put an object
const data = new TextEncoder().encode(JSON.stringify({ hello: "world" }));
const putResult = await adapter.putObject("data/example.json", data, "application/json");
if (!putResult.ok) {
  console.error("Put failed:", putResult.error.message);
}

// Get an object
const getResult = await adapter.getObject("data/example.json");
if (getResult.ok) {
  const content = new TextDecoder().decode(getResult.value);
  console.log("Content:", content);
}

// Head -- retrieve metadata without the body
const headResult = await adapter.headObject("data/example.json");
if (headResult.ok) {
  console.log("Size:", headResult.value.size);
  console.log("Last modified:", headResult.value.lastModified);
}

// List objects by prefix
const listResult = await adapter.listObjects("data/");
if (listResult.ok) {
  for (const obj of listResult.value) {
    console.log(obj.key, obj.size, obj.lastModified);
  }
}

// Delete a single object
await adapter.deleteObject("data/example.json");

// Delete multiple objects in a batch
await adapter.deleteObjects(["data/a.json", "data/b.json"]);
```

### Implement a custom adapter

```ts
import type { LakeAdapter, ObjectInfo } from "@lakesync/adapter";
import type { Result, AdapterError } from "@lakesync/core";

class MyCustomAdapter implements LakeAdapter {
  async putObject(
    path: string,
    data: Uint8Array,
    contentType?: string,
  ): Promise<Result<void, AdapterError>> {
    // Custom storage logic...
  }

  async getObject(path: string): Promise<Result<Uint8Array, AdapterError>> {
    // Custom retrieval logic...
  }

  async headObject(
    path: string,
  ): Promise<Result<{ size: number; lastModified: Date }, AdapterError>> {
    // Custom head logic...
  }

  async listObjects(prefix: string): Promise<Result<ObjectInfo[], AdapterError>> {
    // Custom listing logic...
  }

  async deleteObject(path: string): Promise<Result<void, AdapterError>> {
    // Custom delete logic...
  }

  async deleteObjects(paths: string[]): Promise<Result<void, AdapterError>> {
    // Custom batch delete logic...
  }
}
```

## API surface

### Interfaces

| Export | Description |
|---|---|
| `LakeAdapter` | Abstract interface for lake storage operations |
| `AdapterConfig` | Connection configuration: `endpoint`, `bucket`, `region`, `credentials` |
| `ObjectInfo` | Object metadata: `{ key, size, lastModified }` |

### Implementations

| Export | Description |
|---|---|
| `MinIOAdapter` | MinIO/S3-compatible adapter wrapping `@aws-sdk/client-s3` |

### LakeAdapter methods

| Method | Description |
|---|---|
| `putObject(path, data, contentType?)` | Store an object; returns `Result<void, AdapterError>` |
| `getObject(path)` | Retrieve an object; returns `Result<Uint8Array, AdapterError>` |
| `headObject(path)` | Get object metadata (size, last modified) without the body |
| `listObjects(prefix)` | List objects matching a prefix; returns `Result<ObjectInfo[], AdapterError>` |
| `deleteObject(path)` | Delete a single object |
| `deleteObjects(paths)` | Delete multiple objects in a single batch operation |

## Testing

```bash
bun test --filter adapter
```

Or from the package directory:

```bash
cd packages/adapter
bun test
```

Tests use [Vitest](https://vitest.dev/).
