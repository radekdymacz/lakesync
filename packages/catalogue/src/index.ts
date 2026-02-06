export { NessieCatalogueClient } from "./nessie-client";
export type {
	CatalogueConfig,
	IcebergSchema,
	IcebergField,
	PartitionSpec,
	DataFile,
	Snapshot,
	TableMetadata,
} from "./types";
export { CatalogueError } from "./types";
export {
	tableSchemaToIceberg,
	buildPartitionSpec,
	lakeSyncTableName,
} from "./schema-mapping";
