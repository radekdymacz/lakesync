import {
	DeleteObjectCommand,
	DeleteObjectsCommand,
	GetObjectCommand,
	HeadObjectCommand,
	ListObjectsV2Command,
	PutObjectCommand,
	S3Client,
} from "@aws-sdk/client-s3";
import { AdapterError, Err, Ok, type Result } from "@lakesync/core";
import type { AdapterConfig, LakeAdapter, ObjectInfo } from "./types";

/**
 * Normalise a caught value into an Error or undefined.
 * Used as the `cause` argument for AdapterError.
 */
function toCause(error: unknown): Error | undefined {
	return error instanceof Error ? error : undefined;
}

/**
 * MinIO/S3-compatible lake adapter.
 *
 * Wraps the AWS S3 SDK to provide a Result-based interface for
 * interacting with MinIO or any S3-compatible object store.
 * All public methods return `Result` and never throw.
 */
export class MinIOAdapter implements LakeAdapter {
	private readonly client: S3Client;
	private readonly bucket: string;

	constructor(config: AdapterConfig) {
		this.bucket = config.bucket;
		this.client = new S3Client({
			endpoint: config.endpoint,
			region: config.region ?? "us-east-1",
			credentials: config.credentials,
			forcePathStyle: true, // Required for MinIO
		});
	}

	/**
	 * Execute an S3 operation and wrap any thrown error into an AdapterError Result.
	 * Every public method delegates here so error handling is consistent.
	 * If the operation itself throws an AdapterError, it is returned directly
	 * rather than being wrapped in a second layer.
	 */
	private async wrapS3Call<T>(
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
		return this.wrapS3Call(async () => {
			await this.client.send(
				new PutObjectCommand({
					Bucket: this.bucket,
					Key: path,
					Body: data,
					ContentType: contentType,
				}),
			);
		}, `Failed to put object: ${path}`);
	}

	/** Retrieve an object from the lake */
	async getObject(path: string): Promise<Result<Uint8Array, AdapterError>> {
		return this.wrapS3Call(async () => {
			const response = await this.client.send(
				new GetObjectCommand({
					Bucket: this.bucket,
					Key: path,
				}),
			);
			const bytes = await response.Body?.transformToByteArray();
			if (!bytes) {
				throw new AdapterError(`Empty response for object: ${path}`);
			}
			return bytes;
		}, `Failed to get object: ${path}`);
	}

	/** Get object metadata without retrieving the body */
	async headObject(
		path: string,
	): Promise<Result<{ size: number; lastModified: Date }, AdapterError>> {
		return this.wrapS3Call(async () => {
			const response = await this.client.send(
				new HeadObjectCommand({
					Bucket: this.bucket,
					Key: path,
				}),
			);
			return {
				size: response.ContentLength ?? 0,
				lastModified: response.LastModified ?? new Date(0),
			};
		}, `Failed to head object: ${path}`);
	}

	/** List objects matching a given prefix */
	async listObjects(prefix: string): Promise<Result<ObjectInfo[], AdapterError>> {
		return this.wrapS3Call(async () => {
			const response = await this.client.send(
				new ListObjectsV2Command({
					Bucket: this.bucket,
					Prefix: prefix,
				}),
			);
			return (response.Contents ?? []).map((item) => ({
				key: item.Key ?? "",
				size: item.Size ?? 0,
				lastModified: item.LastModified ?? new Date(0),
			}));
		}, `Failed to list objects with prefix: ${prefix}`);
	}

	/** Delete a single object from the lake */
	async deleteObject(path: string): Promise<Result<void, AdapterError>> {
		return this.wrapS3Call(async () => {
			await this.client.send(
				new DeleteObjectCommand({
					Bucket: this.bucket,
					Key: path,
				}),
			);
		}, `Failed to delete object: ${path}`);
	}

	/** Delete multiple objects from the lake in a single batch operation */
	async deleteObjects(paths: string[]): Promise<Result<void, AdapterError>> {
		if (paths.length === 0) {
			return Ok(undefined);
		}

		return this.wrapS3Call(async () => {
			await this.client.send(
				new DeleteObjectsCommand({
					Bucket: this.bucket,
					Delete: {
						Objects: paths.map((key) => ({ Key: key })),
						Quiet: true,
					},
				}),
			);
		}, `Failed to batch delete ${paths.length} objects`);
	}
}
