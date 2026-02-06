import type { AdapterError, Result } from "@lakesync/core";

/** Information about an object in the lake store */
export interface ObjectInfo {
	/** S3 object key */
	key: string;
	/** Object size in bytes */
	size: number;
	/** Last modification date */
	lastModified: Date;
}

/** Configuration for connecting to the lake store */
export interface AdapterConfig {
	/** Endpoint URL (e.g. http://localhost:9000) */
	endpoint: string;
	/** Bucket name */
	bucket: string;
	/** AWS region (defaults to us-east-1) */
	region?: string;
	/** Access credentials */
	credentials: {
		accessKeyId: string;
		secretAccessKey: string;
	};
}

/** Abstract interface for lake storage operations */
export interface LakeAdapter {
	/** Store an object in the lake */
	putObject(
		path: string,
		data: Uint8Array,
		contentType?: string,
	): Promise<Result<void, AdapterError>>;

	/** Retrieve an object from the lake */
	getObject(path: string): Promise<Result<Uint8Array, AdapterError>>;

	/** Get object metadata without retrieving the body */
	headObject(path: string): Promise<Result<{ size: number; lastModified: Date }, AdapterError>>;

	/** List objects matching a given prefix */
	listObjects(prefix: string): Promise<Result<ObjectInfo[], AdapterError>>;

	/** Delete a single object from the lake */
	deleteObject(path: string): Promise<Result<void, AdapterError>>;

	/** Delete multiple objects from the lake in a single batch operation */
	deleteObjects(paths: string[]): Promise<Result<void, AdapterError>>;
}
