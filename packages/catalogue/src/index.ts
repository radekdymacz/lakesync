export { NessieCatalogueClient } from "./nessie-client";
export {
	buildPartitionSpec,
	lakeSyncTableName,
	tableSchemaToIceberg,
} from "./schema-mapping";
export type {
	CatalogueConfig,
	DataFile,
	IcebergField,
	IcebergSchema,
	PartitionSpec,
	Snapshot,
	TableMetadata,
} from "./types";
export { CatalogueError } from "./types";
