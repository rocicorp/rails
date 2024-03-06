export class InvariantViolation extends Error {}

export function invariant(p: boolean, msg: string) {
  if (!p) {
    throw new InvariantViolation(msg);
  }
}
