/**
 * Ambient type declarations for Web API globals.
 * These are available at runtime in Bun and browsers but not
 * included in the ES2022 lib by default.
 */

declare let crypto: {
	subtle: {
		digest(algorithm: string, data: BufferSource): Promise<ArrayBuffer>;
	};
};

declare class TextEncoder {
	encode(input?: string): Uint8Array;
}

declare class TextDecoder {
	decode(input?: BufferSource): string;
}

type BufferSource = ArrayBufferView | ArrayBuffer;
