import {assert, invariant} from '../../error/asserts.js';
import {Multiset} from '../multiset.js';
import {Version} from '../types.js';
import {DifferenceStreamReader} from './difference-stream-reader.js';
import {IOperator} from './operators/operator.js';
import {Queue} from './queue.js';

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
  readonly queues: Queue<T>[] = [];
  // downstream readers
  readonly readers: DifferenceStreamReader<T>[] = [];
  // upstream operator
  #operator: IOperator | null = null;

  setOperator(operator: IOperator) {
    invariant(this.#operator === null, 'Operator already set!');
    this.#operator = operator;
  }

  /**
   * Prepares data to be sent but does not yet notify readers.
   *
   * Used so we can batch a set of mutations together before running a pipeline.
   */
  queueData(data: [Version, Multiset<T>]) {
    for (const q of this.queues) {
      q.enqueue(data);
    }
  }

  /**
   * Notifies readers. Called during transaction commit.
   */
  notify(version: Version) {
    for (const r of this.readers) {
      r.run(version);
    }
    for (const r of this.readers) {
      r.notify(version);
    }
  }

  /**
   * Notifies any observers that a transaction
   * has completed. Called immediately after transaction commit.
   */
  notifyCommitted(v: Version) {
    for (const r of this.readers) {
      r.notifyCommitted(v);
    }
  }

  /**
   * Forks a new reader off of this writer.
   * Values sent to the writer will be copied off to this new reader.
   */
  newReader(): DifferenceStreamReader<T> {
    const queue = new Queue<T>();
    this.queues.push(queue);
    const reader = new DifferenceStreamReader(this, queue);
    this.readers.push(reader);
    return reader;
  }

  removeReader(reader: DifferenceStreamReader<T>) {
    const idx = this.readers.indexOf(reader);
    assert(idx !== -1, 'Reader not found');
    this.readers.splice(idx, 1);
    this.queues.splice(idx, 1);
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
    if (this.readers.length === 0) {
      this.destroy();
    }
  }

  destroy() {
    this.readers.length = 0;
    // writers will not have a downstream operator
    // if they are the leaf node in the graph
    this.#operator?.destroy();
  }
}
