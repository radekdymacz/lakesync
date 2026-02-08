import { defineConfig } from "vite";

export default defineConfig({
	root: ".",
	build: {
		outDir: "dist",
		rollupOptions: {
			// Server-only packages (adapter backends, parquet, etc.) import Node
			// built-ins that are never called in browser code. Externalise all
			// `node:*` imports so Rollup does not attempt named-export resolution.
			external: [/^node:/],
		},
	},
});
