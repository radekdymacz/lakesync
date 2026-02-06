import {
	DeleteObjectCommand,
	DeleteObjectsCommand,
	GetObjectCommand,
	HeadObjectCommand,
	ListObjectsV2Command,
	PutObjectCommand,
	S3Client,
} from "@aws-sdk/client-s3";
import { AdapterError, Err, Ok } from "@lakesync/core";
import type { Result } from "@lakesync/core";
import type { AdapterConfig, LakeAdapter, ObjectInfo } from "./types";

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

	/** Store an object in the lake */
	async putObject(
		path: string,
		data: Uint8Array,
		contentType?: string,
	): Promise<Result<void, AdapterError>> {
		try {
			await this.client.send(
				new PutObjectCommand({
					Bucket: this.bucket,
					Key: path,
					Body: data,
					ContentType: contentType,
				}),
			);
			return Ok(undefined);
		} catch (error) {
			return Err(
				new AdapterError(
					`Failed to put object: ${path}`,
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	/** Retrieve an object from the lake */
	async getObject(path: string): Promise<Result<Uint8Array, AdapterError>> {
		try {
			const response = await this.client.send(
				new GetObjectCommand({
					Bucket: this.bucket,
					Key: path,
				}),
			);
			const bytes = await response.Body?.transformToByteArray();
			if (!bytes) {
				return Err(new AdapterError(`Empty response for object: ${path}`));
			}
			return Ok(bytes);
		} catch (error) {
			return Err(
				new AdapterError(
					`Failed to get object: ${path}`,
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	/** Get object metadata without retrieving the body */
	async headObject(
		path: string,
	): Promise<Result<{ size: number; lastModified: Date }, AdapterError>> {
		try {
			const response = await this.client.send(
				new HeadObjectCommand({
					Bucket: this.bucket,
					Key: path,
				}),
			);
			return Ok({
				size: response.ContentLength ?? 0,
				lastModified: response.LastModified ?? new Date(0),
			});
		} catch (error) {
			return Err(
				new AdapterError(
					`Failed to head object: ${path}`,
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	/** List objects matching a given prefix */
	async listObjects(prefix: string): Promise<Result<ObjectInfo[], AdapterError>> {
		try {
			const response = await this.client.send(
				new ListObjectsV2Command({
					Bucket: this.bucket,
					Prefix: prefix,
				}),
			);
			const objects: ObjectInfo[] = (response.Contents ?? []).map((item) => ({
				key: item.Key ?? "",
				size: item.Size ?? 0,
				lastModified: item.LastModified ?? new Date(0),
			}));
			return Ok(objects);
		} catch (error) {
			return Err(
				new AdapterError(
					`Failed to list objects with prefix: ${prefix}`,
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	/** Delete a single object from the lake */
	async deleteObject(path: string): Promise<Result<void, AdapterError>> {
		try {
			await this.client.send(
				new DeleteObjectCommand({
					Bucket: this.bucket,
					Key: path,
				}),
			);
			return Ok(undefined);
		} catch (error) {
			return Err(
				new AdapterError(
					`Failed to delete object: ${path}`,
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	/** Delete multiple objects from the lake in a single batch operation */
	async deleteObjects(paths: string[]): Promise<Result<void, AdapterError>> {
		if (paths.length === 0) {
			return Ok(undefined);
		}

		try {
			await this.client.send(
				new DeleteObjectsCommand({
					Bucket: this.bucket,
					Delete: {
						Objects: paths.map((key) => ({ Key: key })),
						Quiet: true,
					},
				}),
			);
			return Ok(undefined);
		} catch (error) {
			return Err(
				new AdapterError(
					`Failed to batch delete ${paths.length} objects`,
					error instanceof Error ? error : undefined,
				),
			);
		}
	}
}
