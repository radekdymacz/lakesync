import { loader } from "fumadocs-core/source";
import { docs } from "@/.source";

const rawSource = docs.toFumadocsSource();

// fumadocs-mdx@11 returns files as a lazy getter function,
// but fumadocs-core@15.8 expects a plain array — resolve it.
const rawFiles: unknown = rawSource.files;
const files =
	typeof rawFiles === "function" ? (rawFiles as () => typeof rawSource.files)() : rawSource.files;

export const source = loader({
	baseUrl: "/docs",
	source: { ...rawSource, files },
});
