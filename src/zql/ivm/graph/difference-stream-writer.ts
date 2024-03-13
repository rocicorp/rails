import {assert, invariant} from '../../error/asserts.js';
import {Multiset} from '../multiset.js';
import {Version} from '../types.js';
import {DifferenceStreamReader} from './difference-stream-reader.js';
import {Request} from './message.js';
import {Operator} from './operators/operator.js';

/**
 * Represents the output of an Operator.
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
 *
 * o = operator
 * w = writer
 * r = reader
 */
export class DifferenceStreamWriter<T> {
  #upstreamOperator: Operator | null = null;
  readonly downstreamReaders: DifferenceStreamReader<T>[] = [];
  readonly #pendingRecipients = new Map<number, DifferenceStreamReader<T>>();

  setOperator(operator: Operator) {
    invariant(this.#upstreamOperator === null, 'Operator already set!');
    this.#upstreamOperator = operator;
  }

  /**
   * Prepares data to be sent but does not yet notify readers.
   *
   * Used so we can batch a set of mutations together before running a pipeline.
   */
  queueData(data: [Version, Multiset<T>]) {
    for (const r of this.downstreamReaders) {
      r.enqueue(data);
    }
  }

  /**
   * Notifies readers. Called during transaction commit.
   */
  notify(version: Version) {
    // Tell downstreams to run their operators
    for (const r of this.downstreamReaders) {
      r.run(version);
    }
    // After all operators have been run we can tell them
    // to notify along their output edges which will
    // cause the next level of writers & operators to run and notify.
    for (const r of this.downstreamReaders) {
      r.notify(version);
    }
  }

  /**
   * Notifies any observers that a transaction
   * has completed. Called immediately after transaction commit.
   */
  notifyCommitted(v: Version) {
    for (const r of this.downstreamReaders) {
      r.notifyCommitted(v);
    }
  }

  /**
   * Forks a new reader off of this writer.
   * Values sent to the writer will be fanned out
   * to this new reader.
   */
  newReader(): DifferenceStreamReader<T> {
    const reader = new DifferenceStreamReader(this);
    this.downstreamReaders.push(reader);
    return reader;
  }

  removeReader(reader: DifferenceStreamReader<T>) {
    const idx = this.downstreamReaders.indexOf(reader);
    assert(idx !== -1, 'Reader not found');
    this.downstreamReaders.splice(idx, 1);
  }

  /**
   * Removes a reader from this writer.
   * If this writer has no more readers, it will be destroyed
   * and tell its upstream to destroy itself.
   *
   * If this writer has no readers then the operator
   * that is upstream has no readers and can safely be destroyed.
   */
  removeReaderAndMaybeDestroy(reader: DifferenceStreamReader<T>) {
    this.removeReader(reader);
    if (this.downstreamReaders.length === 0) {
      this.destroy();
    }
  }

  messageUpstream(
    message: Request,
    downstreamSender: DifferenceStreamReader<T>,
  ) {
    this.#pendingRecipients.set(message.id, downstreamSender);
    this.#upstreamOperator?.messageUpstream(message);
  }

  destroy() {
    this.downstreamReaders.length = 0;
    // The root differnce stream will not have an upstream operator
    this.#upstreamOperator?.destroy();
  }
}
