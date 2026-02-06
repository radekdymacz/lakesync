/** Discriminated union representing either success or failure */
export type Result<T, E = LakeSyncError> =
  | { ok: true; value: T }
  | { ok: false; error: E };

import type { LakeSyncError } from './errors';

/** Create a successful Result */
export const Ok = <T>(value: T): Result<T, never> => ({ ok: true, value });

/** Create a failed Result */
export const Err = <E>(error: E): Result<never, E> => ({ ok: false, error });

/** Transform the success value of a Result */
export function mapResult<T, U, E>(result: Result<T, E>, fn: (value: T) => U): Result<U, E> {
  if (result.ok) {
    return Ok(fn(result.value));
  }
  return result;
}

/** Chain Result-returning operations */
export function flatMapResult<T, U, E>(
  result: Result<T, E>,
  fn: (value: T) => Result<U, E>,
): Result<U, E> {
  if (result.ok) {
    return fn(result.value);
  }
  return result;
}

/** Extract the value from a Result or throw the error */
export function unwrapOrThrow<T, E>(result: Result<T, E>): T {
  if (result.ok) {
    return result.value;
  }
  throw result.error;
}

/** Wrap a Promise into a Result */
export async function fromPromise<T>(promise: Promise<T>): Promise<Result<T, Error>> {
  try {
    const value = await promise;
    return Ok(value);
  } catch (error) {
    return Err(error instanceof Error ? error : new Error(String(error)));
  }
}
