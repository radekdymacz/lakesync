import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { initSync } from "parquet-wasm/esm";

let initialised = false;

/**
 * Ensures the parquet-wasm WASM module is initialised.
 *
 * Uses `initSync` with the WASM binary loaded from disk.
 * Safe to call multiple times — subsequent calls are no-ops.
 */
export function ensureWasmInitialised(): void {
	if (initialised) return;

	// Resolve the path to the WASM binary relative to the parquet-wasm/esm entry
	const require = createRequire(import.meta.url);
	const esmEntryPath = require.resolve("parquet-wasm/esm");
	const wasmPath = esmEntryPath.replace("parquet_wasm.js", "parquet_wasm_bg.wasm");

	const wasmBytes = readFileSync(wasmPath);
	initSync({ module: wasmBytes });
	initialised = true;
}
