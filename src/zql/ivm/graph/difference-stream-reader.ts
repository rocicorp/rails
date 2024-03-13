import {invariant, must} from '../../error/asserts.js';
import {Version} from '../types.js';
import {Queue, QueueEntry} from './queue.js';
import {DifferenceStreamWriter} from './difference-stream-writer.js';
import {Operator} from './operators/operator.js';
import {Request} from './message.js';

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
  readonly #upstreamWriter;
  #downstreamOperator: Operator | null = null;
  #lastSeenVersion: Version = -1;

  constructor(upstream: DifferenceStreamWriter<T>) {
    this.#queue = new Queue<T>();
    this.#upstreamWriter = upstream;
  }

  setOperator(operator: Operator) {
    invariant(this.#downstreamOperator === null, 'Operator already set!');
    this.#downstreamOperator = operator;
  }

  enqueue(data: QueueEntry<T>) {
    this.#queue.enqueue(data);
  }

  run(v: Version) {
    this.#lastSeenVersion = v;
    must(this.#downstreamOperator, 'reader is missing operator').run(v);
  }

  notify(v: Version) {
    invariant(v === this.#lastSeenVersion, 'notify called out of order');
    must(this.#downstreamOperator, 'reader is missing operator').notify(v);
  }

  notifyCommitted(v: Version) {
    // If we did not process this version
    // then we should not pass commit notifications down this path.
    if (v !== this.#lastSeenVersion) {
      return;
    }
    must(
      this.#downstreamOperator,
      'reader is missing operator',
    ).notifyCommitted(v);
  }

  drain(version: Version) {
    const ret: QueueEntry<T>[] = [];
    for (;;) {
      const data = this.#queue.peek();
      if (data === null) {
        break;
      }
      if (data[0] > version) {
        break;
      }
      ret.push(data);
      this.#queue.dequeue();
    }
    return ret;
  }

  isEmpty() {
    return this.#queue.isEmpty();
  }

  destroy() {
    this.#upstreamWriter.removeReaderAndMaybeDestroy(this);
    this.#queue.clear();
  }

  messageUpstream(message: Request) {
    this.#upstreamWriter.messageUpstream(message, this);
  }
}
