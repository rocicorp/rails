import {invariant, must} from '../../error/asserts.js';
import {Multiset} from '../multiset.js';
import {Version} from '../types.js';
import {Queue} from './queue.js';
import {IOperator} from './operators/operator.js';

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

  run(v: Version) {
    this.#lastSeenVersion = v;
    must(this.#operator, 'reader is missing operator').run(v);
  }

  notify(v: Version) {
    invariant(v === this.#lastSeenVersion, 'notify called out of order');
    must(this.#operator, 'reader is missing operator').notify(v);
  }

  notifyCommitted(v: Version) {
    // If we did not process this version
    // then we should not pass commit notifications down this path.
    if (v !== this.#lastSeenVersion) {
      return;
    }
    must(this.#operator, 'reader is missing operator').notifyCommitted(v);
  }

  drain(version: Version) {
    const ret: Multiset<T>[] = [];
    for (;;) {
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
