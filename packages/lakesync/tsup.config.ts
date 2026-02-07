import { defineConfig } from "tsup";

export default defineConfig({
	entry: {
		index: "src/index.ts",
		client: "src/client.ts",
		gateway: "src/gateway.ts",
		adapter: "src/adapter.ts",
		proto: "src/proto.ts",
		parquet: "src/parquet.ts",
		catalogue: "src/catalogue.ts",
		compactor: "src/compactor.ts",
		analyst: "src/analyst.ts",
	},
	format: ["esm"],
	dts: { resolve: true },
	splitting: true,
	sourcemap: true,
	clean: true,
	external: [
		"sql.js",
		"idb",
		"@aws-sdk/client-s3",
		"@bufbuild/protobuf",
		"parquet-wasm",
		"apache-arrow",
		"@duckdb/duckdb-wasm",
	],
});
