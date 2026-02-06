import { defineConfig } from "vite";

export default defineConfig({
	root: ".",
	build: {
		outDir: "dist",
		rollupOptions: {
			// The gateway imports @lakesync/parquet which uses Node-only modules
			// (node:fs, node:module). These are never called in browser code
			// (parquet flush requires a configured adapter, which is absent here).
			// Mark them as external so Rollup does not attempt named-export resolution.
			external: ["node:fs", "node:module"],
		},
	},
});
