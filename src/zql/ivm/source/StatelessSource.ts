import {MaterialiteForSourceInternal} from '../Materialite.js';
import {Entry, Multiset} from '../Multiset.js';
import {DifferenceStream} from '../graph/DifferenceStream.js';
import {SourceInternal} from './Source.js';
import {Version} from '../types.js';

/**
 * Is a source of values.
 */
export class StatelessSource<T> {
  #stream: DifferenceStream<T>;
  readonly #internal: SourceInternal;
  readonly #materialite: MaterialiteForSourceInternal;

  #pending: Entry<T>[] = [];

  constructor(materialite: MaterialiteForSourceInternal) {
    this.#materialite = materialite;
    this.#stream = new DifferenceStream<T>();
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;
    this.#internal = {
      // add values to queues, add values to the set
      onCommitPhase1(version: Version) {
        self.#stream.queueData([version, new Multiset(self.#pending)]);
        self.#pending = [];
      },
      // release queues by telling the stream to send data
      onCommitPhase2(version: Version) {
        self.#stream.notify(version);
      },
      // notify effects / listeners
      // this is done once the entire reactive graph has finished computing
      // itself
      onCommitted(v: Version) {
        self.#stream.notifyCommitted(v);
      },
      onRollback() {
        self.#pending = [];
      },
    };
  }

  get stream(): DifferenceStream<T> {
    return this.#stream;
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
