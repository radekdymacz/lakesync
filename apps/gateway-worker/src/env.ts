/** Cloudflare Workers environment bindings */
export interface Env {
	/** Durable Object namespace for sync gateways */
	SYNC_GATEWAY: DurableObjectNamespace;
	/** R2 bucket for Parquet file storage */
	LAKE_BUCKET: R2Bucket;
	/** Nessie Iceberg REST API URI */
	NESSIE_URI: string;
	/** JWT secret for authentication (set via wrangler secret) */
	JWT_SECRET: string;
	/** Optional comma-separated list of allowed CORS origins */
	ALLOWED_ORIGINS?: string;
	/** Optional JSON shard configuration for table-based sharding across DOs */
	SHARD_CONFIG?: string;
	/** Optional maximum buffer size in bytes (overrides the default 4 MiB). */
	MAX_BUFFER_BYTES?: string;
}
