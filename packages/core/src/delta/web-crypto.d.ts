/**
 * Ambient type declarations for Web Crypto API and encoding globals.
 * These are available at runtime in Bun and browsers but not
 * included in the ES2022 lib by default.
 */

/* eslint-disable no-var */
declare var crypto: {
	subtle: {
		digest(
			algorithm: string,
			data: BufferSource,
		): Promise<ArrayBuffer>;
	};
};

declare class TextEncoder {
	encode(input?: string): Uint8Array;
}

type BufferSource = ArrayBufferView | ArrayBuffer;
