import {invariant, must} from '../../error/asserts.js';
import {Multiset} from '../multiset.js';
import {Version} from '../types.js';
import {DifferenceStreamWriter} from './difference-stream-writer.js';
import {Request} from './message.js';
import {Operator} from './operators/operator.js';
import {Queue, QueueEntry} from './queue.js';

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
  protected readonly _queue = new Queue<T>();
  readonly #upstreamWriter;
  #downstreamOperator: Operator | null = null;
  #lastSeenVersion: Version = -1;

  constructor(upstream: DifferenceStreamWriter<T>) {
    this.#upstreamWriter = upstream;
  }

  setOperator(operator: Operator) {
    invariant(this.#downstreamOperator === null, 'Operator already set!');
    this.#downstreamOperator = operator;
  }

  enqueue(data: QueueEntry<T>) {
    this._queue.enqueue(data);
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
      const data = this._queue.peek();
      if (data === null) {
        break;
      }
      if (data[0] > version) {
        break;
      }
      ret.push(data);
      this._queue.dequeue();
    }
    return ret;
  }

  isEmpty() {
    return this._queue.isEmpty();
  }

  destroy() {
    this.#upstreamWriter.removeReaderAndMaybeDestroy(this);
    this._queue.clear();
  }

  messageUpstream(message: Request) {
    this.#upstreamWriter.messageUpstream(message, this);
  }
}

export class DifferenceStreamReaderFromRoot<
  T,
> extends DifferenceStreamReader<T> {
  drain(version: Version) {
    if (this._queue.isEmpty()) {
      return [[version, new Multiset<T>([])]] as QueueEntry<T>[];
    }
    return super.drain(version);
  }
}
