export class InvariantViolation extends Error {}

export function invariant(p: boolean, msg: string) {
  if (!p) {
    throw new InvariantViolation(msg);
  }
}

export function nullthrows<T>(x: T | null, msg?: string): T {
  if (x === null) {
    throw new InvariantViolation(msg);
  }

  return x;
}
