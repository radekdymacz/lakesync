import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		globals: true,
		include: ["src/**/__tests__/**/*.test.ts"],
		passWithNoTests: true,
		// DuckDB WASM init can exceed the default 5s under turbo-parallel CI load
		testTimeout: 30_000,
		hookTimeout: 30_000,
	},
});
