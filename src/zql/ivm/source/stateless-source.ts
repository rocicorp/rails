import {MaterialiteForSourceInternal} from '../materialite.js';
import {Entry, Multiset} from '../multiset.js';
import {DifferenceStream} from '../graph/difference-stream.js';
import {Source, SourceInternal} from './source.js';
import {Version} from '../types.js';
import {createPullResponseMessage} from '../graph/message.js';
import {Request} from '../graph/message.js';
import {DifferenceStreamReader} from '../graph/difference-stream-reader.js';

/**
 * Is a source of values.
 */
export class StatelessSource<T> implements Source<T> {
  #stream: DifferenceStream<T>;
  readonly #internal: SourceInternal;
  readonly #materialite: MaterialiteForSourceInternal;

  #pending: Entry<T>[] = [];

  constructor(materialite: MaterialiteForSourceInternal) {
    this.#materialite = materialite;
    this.#stream = new DifferenceStream<T>();
    this.#internal = {
      // add values to queues, add values to the set
      onCommitEnqueue: (version: Version) => {
        this.#stream.queueData([version, new Multiset(this.#pending)]);
        this.#pending = [];
      },
      // release queues by telling the stream to send data
      onCommitRun: (version: Version) => {
        this.#stream.notify(version);
      },
      // notify effects / listeners
      // this is done once the entire reactive graph has finished computing
      // itself
      onCommitted: (v: Version) => {
        this.#stream.notifyCommitted(v);
      },
      onRollback: () => {
        this.#pending = [];
      },
    };
  }

  get stream(): DifferenceStream<T> {
    return this.#stream;
  }

  processMessage(
    message: Request,
    downstream: DifferenceStreamReader<T>,
  ): void {
    switch (message.type) {
      case 'pull': {
        // This is problematic under the current model of how we run the graph.
        // As in, I don't think this'll work for operators with many inputs.
        // So this presents another reason to move to optimistically running the graph
        // as soon as data is enqueued and making the operators
        // able to handle partial inputs. Something I thought avoiding would be simpler but turns out the opposite.
        // The other reason is interactive transactions as discussed with Erik
        // For interactive transactions we also can't wait until all inputs have been updated
        // before running the graph.
        const response = createPullResponseMessage(message);
        downstream.enqueue([
          this.#materialite.getVersion(),
          new Multiset([]),
          response,
        ]);
        downstream.notify(this.#materialite.getVersion());
        downstream.notifyCommitted(this.#materialite.getVersion());
        break;
      }
    }
  }

  addAll(values: Iterable<T>): this {
    // TODO (mlaw): start a materialite transaction
    for (const v of values) {
      this.#pending.push([v, 1]);
    }
    this.#materialite.addDirtySource(this.#internal);
    return this;
  }

  add(value: T): this {
    this.#pending.push([value, 1]);
    this.#materialite.addDirtySource(this.#internal);
    return this;
  }

  delete(value: T): this {
    this.#pending.push([value, -1]);
    this.#materialite.addDirtySource(this.#internal);
    return this;
  }

  deleteAll(values: Iterable<T>): this {
    for (const v of values) {
      this.#pending.push([v, -1]);
    }
    this.#materialite.addDirtySource(this.#internal);
    return this;
  }
}
