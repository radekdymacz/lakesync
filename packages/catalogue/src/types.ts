import { LakeSyncError } from "@lakesync/core";

/** Configuration for connecting to a Nessie Iceberg REST catalogue */
export interface CatalogueConfig {
	/** Nessie Iceberg REST API base URI, e.g. "http://localhost:19120/iceberg" */
	nessieUri: string;
	/** Object storage warehouse URI, e.g. "s3://lakesync-warehouse" */
	warehouseUri: string;
	/** Nessie branch name. Defaults to "main". */
	defaultBranch?: string;
}

/** Iceberg schema definition following the Iceberg REST spec */
export interface IcebergSchema {
	type: "struct";
	"schema-id": number;
	fields: IcebergField[];
}

/** A single field within an Iceberg schema */
export interface IcebergField {
	id: number;
	name: string;
	required: boolean;
	type: string; // "string", "long", "double", "boolean"
}

/** Iceberg partition specification */
export interface PartitionSpec {
	"spec-id": number;
	fields: Array<{
		"source-id": number;
		"field-id": number;
		name: string;
		transform: string; // "day", "identity", etc.
	}>;
}

/** A data file reference for Iceberg table commits */
export interface DataFile {
	content: "data";
	"file-path": string;
	"file-format": "PARQUET";
	"record-count": number;
	"file-size-in-bytes": number;
	partition?: Record<string, string>;
}

/** An Iceberg table snapshot */
export interface Snapshot {
	"snapshot-id": number;
	"timestamp-ms": number;
	summary: Record<string, string>;
	"manifest-list"?: string;
}

/** Full table metadata as returned by the Iceberg REST catalogue */
export interface TableMetadata {
	"metadata-location"?: string;
	metadata: {
		"format-version": number;
		"table-uuid": string;
		location: string;
		"current-schema-id": number;
		schemas: IcebergSchema[];
		"current-snapshot-id"?: number;
		snapshots?: Snapshot[];
		"partition-specs"?: PartitionSpec[];
	};
}

/** Catalogue operation error */
export class CatalogueError extends LakeSyncError {
	readonly statusCode: number;

	constructor(message: string, statusCode: number, cause?: Error) {
		super(message, "CATALOGUE_ERROR", cause);
		this.statusCode = statusCode;
	}
}
