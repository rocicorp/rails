import {invariant, nullthrows} from '../../error/InvariantViolation.js';
import {Multiset} from '../Multiset.js';
import {Version} from '../types.js';
import {Queue} from './Queue.js';
import {IOperator} from './operators/Operator.js';

/**
 * Represents the input to an operator.
 *
 * Operators write to their ouput (DifferenceStreamWriter) which fans out
 * to any and all readers (differenceStreamReader) of that output.
 *
 *     o
 *     |
 *     w
 *   / | \
 *  r  r  r
 *  |  |  |
 *  o  o  o
 */
export class DifferenceStreamReader<T = unknown> {
  readonly #queue;
  #operator: IOperator | null = null;
  #lastSeenVersion: Version = -1;

  constructor(queue: Queue<T>) {
    this.#queue = queue;
  }

  setOperator(operator: IOperator) {
    invariant(this.#operator === null, 'Operator already set!');
    this.#operator = operator;
  }

  notify(v: Version) {
    this.#lastSeenVersion = v;
    nullthrows(this.#operator, 'reader is missing operator').run(v);
  }

  notifyCommitted(v: Version) {
    // If we did not process this version in this oeprator
    // then we should not pass notifications down this path.
    if (v !== this.#lastSeenVersion) {
      return;
    }
    nullthrows(this.#operator, 'reader is missing operator').notifyCommitted(v);
  }

  drain(version: Version) {
    const ret: Multiset<T>[] = [];
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const data = this.#queue.peek();
      if (data === null) {
        break;
      }
      if (data[0] > version) {
        break;
      }
      ret.push(data[1]);
      this.#queue.dequeue();
    }
    return ret;
  }

  isEmpty() {
    return this.#queue.isEmpty();
  }
}
