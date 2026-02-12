export type { LakeAdapter, ObjectInfo } from "@lakesync/core";

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
