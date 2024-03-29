import {assert, invariant, must} from '../../error/asserts.js';
import {tab} from '../../util/print.js';
import {Source} from '../source/source.js';
import {Version} from '../types.js';
import {
  DifferenceStreamReader,
  DifferenceStreamReaderFromRoot,
} from './difference-stream-reader.js';
import {Request} from './message.js';
import {Operator} from './operators/operator.js';
import {QueueEntry} from './queue.js';

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
let id = 0;
export class DifferenceStreamWriter<T> {
  readonly id = id++;
  #upstreamOperator: Operator | null = null;
  protected readonly _downstreamReaders: DifferenceStreamReader<T>[] = [];

  readonly #pendingRecipients = new Map<number, DifferenceStreamReader<T>>();
  readonly #toNotify: Set<DifferenceStreamReader<T>> = new Set();

  setOperator(operator: Operator) {
    invariant(this.#upstreamOperator === null, 'Operator already set!');
    this.#upstreamOperator = operator;
  }

  /**
   * Prepares data to be sent but does not yet notify readers.
   *
   * Used so we can batch a set of mutations together before running a pipeline.
   *
   * TODO: this whole
   * `queue`, `notify`, `notifyCommitted` pattern will need to be re-done to
   * allow for reading one's writes in a given transaction.
   *
   * 1. `queue` should immediately notify the downstream
   * 2. the downstream should immediately compute if it is able
   * 3. then notify its downstream(s)
   */
  queueData(data: QueueEntry<T>) {
    this.#toNotify.clear();
    const msg = data[2];
    if (msg) {
      // only go down the path from which the message came.
      // no need to visit other paths.
      const recipient = must(
        this.#pendingRecipients.get(msg.replyingTo),
        'No recipient for received message',
      );
      this.#pendingRecipients.delete(msg.replyingTo);
      recipient.enqueue(data);
      this.#toNotify.add(recipient);
    } else {
      for (const r of this._downstreamReaders) {
        r.enqueue(data);
        this.#toNotify.add(r);
      }
    }
  }

  /**
   * Notifies readers. Called during transaction commit.
   */
  notify(version: Version) {
    // Tell downstreams to run their operators
    for (const r of this.#toNotify) {
      r.run(version);
    }
    // After all operators have been run we can tell them
    // to notify along their output edges which will
    // cause the next level of writers & operators to run and notify.
    for (const r of this.#toNotify) {
      r.notify(version);
    }
  }

  /**
   * Notifies any observers that a transaction
   * has completed. Called immediately after transaction commit.
   */
  notifyCommitted(version: Version) {
    for (const r of this.#toNotify) {
      r.notifyCommitted(version);
    }
  }

  /**
   * Forks a new reader off of this writer.
   * Values sent to the writer will be fanned out
   * to all forked readers.
   */
  newReader(): DifferenceStreamReader<T> {
    const reader = new DifferenceStreamReader(this);
    this._downstreamReaders.push(reader);
    return reader;
  }

  removeReader(reader: DifferenceStreamReader<T>) {
    const idx = this._downstreamReaders.indexOf(reader);
    assert(idx !== -1, 'Reader not found');
    this._downstreamReaders.splice(idx, 1);
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
    if (this._downstreamReaders.length === 0) {
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
    this._downstreamReaders.length = 0;
    // The root difference stream will not have an upstream operator
    this.#upstreamOperator?.destroy();
  }

  toString(tabs = 0) {
    return tab(
      tabs,
      `DifferenceStreamWriter(${this.id}) {
upstreamOperator: ${this.#upstreamOperator?.toString(tabs + 1)}
}`,
    );
  }
}

export class RootDifferenceStreamWriter<T> extends DifferenceStreamWriter<T> {
  readonly #source;
  constructor(source: Source<unknown>) {
    super();
    this.#source = source;
  }

  messageUpstream(
    message: Request,
    downstreamSender: DifferenceStreamReader<T>,
  ) {
    this.#source.processMessage(message, downstreamSender);
  }

  newReader() {
    const reader: DifferenceStreamReader<T> =
      new DifferenceStreamReaderFromRoot(this);
    this._downstreamReaders.push(reader);
    return reader;
  }

  toString(tabs = 0): string {
    return tab(tabs, `RootDifferenceStreamWriter(${this.id})`);
  }
}
