import {Multiset} from '../multiset.js';
import {Version} from '../types.js';
import {DifferenceStreamReader} from './difference-stream-reader.js';
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
abstract class AbstractDifferenceStreamWriter<T> {
  readonly queues: Queue<T>[] = [];
  readonly readers: DifferenceStreamReader<T>[] = [];

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
    const reader = new DifferenceStreamReader(queue);
    this.readers.push(reader);
    return reader;
  }
}

export class DifferenceStreamWriter<
  T,
> extends AbstractDifferenceStreamWriter<T> {}
