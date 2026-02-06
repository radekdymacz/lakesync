/**
 * Ambient type declarations for encoding and crypto globals.
 * These are available at runtime in Bun and browsers but not
 * included in the ES2022 lib by default.
 */

/* eslint-disable no-var */
declare class TextEncoder {
	encode(input?: string): Uint8Array;
}

declare class TextDecoder {
	decode(input?: BufferSource): string;
}

declare var crypto: {
	subtle: {
		digest(algorithm: string, data: BufferSource): Promise<ArrayBuffer>;
	};
};

type BufferSource = ArrayBufferView | ArrayBuffer;
