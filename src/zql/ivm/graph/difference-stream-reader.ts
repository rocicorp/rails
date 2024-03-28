import {invariant, must} from '../../error/asserts.js';
import {Multiset} from '../multiset.js';
import {Version} from '../types.js';
import {DifferenceStreamWriter} from './difference-stream-writer.js';
import {Request} from './message.js';
import {Operator} from './operators/operator.js';
import {QueueEntry} from './queue.js';

let id = 0;
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
  protected _pending: QueueEntry<T> | undefined = undefined;
  readonly #upstreamWriter;
  #downstreamOperator: Operator | null = null;
  #lastSeenVersion: Version = -1;
  readonly id = ++id;

  constructor(upstream: DifferenceStreamWriter<T>) {
    this.#upstreamWriter = upstream;
  }

  setOperator(operator: Operator) {
    invariant(this.#downstreamOperator === null, 'Operator already set!');
    this.#downstreamOperator = operator;
  }

  enqueue(data: QueueEntry<T>) {
    invariant(
      this._pending === undefined,
      'queue should be flushed between transactions. id: ' + this.id,
    );
    this._pending = data;
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
    if (this._pending === undefined) {
      return;
    }
    if (this._pending[0] > version) {
      return;
    }
    invariant(this._pending[0] === version, 'unexpected version in queue');
    const ret = this._pending;
    this._pending = undefined;
    return ret;
  }

  isEmpty() {
    return this._pending === undefined;
  }

  destroy() {
    this.#upstreamWriter.removeReaderAndMaybeDestroy(this);
    this._pending = undefined;
  }

  messageUpstream(message: Request) {
    this.#upstreamWriter.messageUpstream(message, this);
  }
}

export class DifferenceStreamReaderFromRoot<
  T,
> extends DifferenceStreamReader<T> {
  drain(version: Version) {
    if (this._pending === undefined) {
      return [version, new Multiset<T>([])] as QueueEntry<T>;
    }
    return super.drain(version);
  }
}
