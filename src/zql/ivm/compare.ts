import {InvariantViolation} from '../error/invariant-violation.js';

export function compareEntityFields<T>(lVal: T, rVal: T) {
  if (lVal === rVal) {
    return 0;
  }
  if (lVal === null || lVal === undefined) {
    return -1;
  }
  if (rVal === null || rVal === undefined) {
    return 1;
  }
  if (lVal < rVal) {
    return -1;
  }
  if (lVal > rVal) {
    return 1;
  }

  throw new InvariantViolation('Unreachable');
}
