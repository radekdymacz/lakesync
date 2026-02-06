/**
 * Ambient type declarations for Fetch API globals.
 * These are available at runtime in Bun and browsers but not
 * included in the ES2022 lib by default.
 */

/* eslint-disable no-var */

interface ResponseInit {
	status?: number;
	statusText?: string;
	headers?: Record<string, string>;
}

interface Response {
	readonly ok: boolean;
	readonly status: number;
	readonly statusText: string;
	readonly body: ReadableStream<Uint8Array> | null;
	json(): Promise<unknown>;
	text(): Promise<string>;
	arrayBuffer(): Promise<ArrayBuffer>;
}

interface RequestInit {
	method?: string;
	headers?: Record<string, string>;
	body?: string | Uint8Array | ArrayBuffer;
	signal?: AbortSignal;
}

declare function fetch(input: string, init?: RequestInit): Promise<Response>;

interface AbortSignalTimeoutStatic {
	timeout(ms: number): AbortSignal;
}

declare let AbortSignal: AbortSignalTimeoutStatic & {
	prototype: AbortSignal;
};

interface AbortSignal {
	readonly aborted: boolean;
}

/**
 * Web Crypto API and encoding globals.
 * Required because @lakesync/core source is resolved directly
 * through workspace:* exports and needs these ambient types.
 */

declare let crypto: {
	subtle: {
		digest(algorithm: string, data: BufferSource): Promise<ArrayBuffer>;
	};
};

declare class TextEncoder {
	encode(input?: string): Uint8Array;
}

type BufferSource = ArrayBufferView | ArrayBuffer;
