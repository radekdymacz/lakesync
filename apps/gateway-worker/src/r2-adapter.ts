import type { LakeAdapter, ObjectInfo } from "@lakesync/adapter";
import { AdapterError, Err, Ok, type Result } from "@lakesync/core";

/**
 * Normalise a caught value into an Error or undefined.
 * Used as the `cause` argument for AdapterError.
 */
function toCause(error: unknown): Error | undefined {
	return error instanceof Error ? error : undefined;
}

/**
 * Cloudflare R2 lake adapter.
 *
 * Wraps the Workers R2 bucket binding to provide a Result-based interface
 * for interacting with Cloudflare R2 storage. All public methods return
 * `Result` and never throw.
 */
export class R2Adapter implements LakeAdapter {
	constructor(private readonly bucket: R2Bucket) {}

	/**
	 * Execute an R2 operation and wrap any thrown error into an AdapterError Result.
	 * Every public method delegates here so error handling is consistent.
	 * If the operation itself throws an AdapterError, it is returned directly
	 * rather than being wrapped in a second layer.
	 */
	private async wrapR2Call<T>(
		operation: () => Promise<T>,
		errorMessage: string,
	): Promise<Result<T, AdapterError>> {
		try {
			const value = await operation();
			return Ok(value);
		} catch (error) {
			if (error instanceof AdapterError) {
				return Err(error);
			}
			return Err(new AdapterError(errorMessage, toCause(error)));
		}
	}

	/** Store an object in the lake */
	async putObject(
		path: string,
		data: Uint8Array,
		contentType?: string,
	): Promise<Result<void, AdapterError>> {
		return this.wrapR2Call(async () => {
			await this.bucket.put(path, data, {
				httpMetadata: contentType ? { contentType } : undefined,
			});
		}, `Failed to put object: ${path}`);
	}

	/** Retrieve an object from the lake */
	async getObject(path: string): Promise<Result<Uint8Array, AdapterError>> {
		return this.wrapR2Call(async () => {
			const object = await this.bucket.get(path);
			if (!object) {
				throw new AdapterError(`Object not found: ${path}`);
			}
			const buffer = await object.arrayBuffer();
			return new Uint8Array(buffer);
		}, `Failed to get object: ${path}`);
	}

	/** Get object metadata without retrieving the body */
	async headObject(
		path: string,
	): Promise<Result<{ size: number; lastModified: Date }, AdapterError>> {
		return this.wrapR2Call(async () => {
			const head = await this.bucket.head(path);
			if (!head) {
				throw new AdapterError(`Object not found: ${path}`);
			}
			return {
				size: head.size,
				lastModified: head.uploaded,
			};
		}, `Failed to head object: ${path}`);
	}

	/** List objects matching a given prefix */
	async listObjects(prefix: string): Promise<Result<ObjectInfo[], AdapterError>> {
		return this.wrapR2Call(async () => {
			const listed = await this.bucket.list({ prefix });
			return listed.objects.map((obj) => ({
				key: obj.key,
				size: obj.size,
				lastModified: obj.uploaded,
			}));
		}, `Failed to list objects with prefix: ${prefix}`);
	}

	/** Delete a single object from the lake */
	async deleteObject(path: string): Promise<Result<void, AdapterError>> {
		return this.wrapR2Call(async () => {
			await this.bucket.delete(path);
		}, `Failed to delete object: ${path}`);
	}

	/** Delete multiple objects from the lake in a single batch operation */
	async deleteObjects(paths: string[]): Promise<Result<void, AdapterError>> {
		if (paths.length === 0) {
			return Ok(undefined);
		}

		return this.wrapR2Call(async () => {
			await this.bucket.delete(paths);
		}, `Failed to batch delete ${paths.length} objects`);
	}
}
