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
}
