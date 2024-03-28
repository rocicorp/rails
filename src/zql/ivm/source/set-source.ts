import {Comparator, ITree} from '@vlcn.io/ds-and-algos/types';
import {MaterialiteForSourceInternal} from '../materialite.js';
import {
  DifferenceStream,
  RootDifferenceStream,
} from '../graph/difference-stream.js';
import {SourceInternal, Source} from './source.js';
import {Entry, Multiset} from '../multiset.js';
import {Version} from '../types.js';
import {Treap} from '@vlcn.io/ds-and-algos/Treap';
import {must} from '../../error/asserts.js';
import {PullMsg, Request, createPullResponseMessage} from '../graph/message.js';
import {DifferenceStreamReader} from '../graph/difference-stream-reader.js';

/**
 * A source that remembers what values it contains.
 *
 * This allows pipelines that are created after a source already
 * exists to be able to receive historical data.
 *
 */
export abstract class SetSource<T extends object> implements Source<T> {
  #stream: DifferenceStream<T>;
  readonly #internal: SourceInternal;
  protected readonly _materialite: MaterialiteForSourceInternal;
  readonly #listeners = new Set<(data: ITree<T>, v: Version) => void>();
  #pending: Entry<T>[] = [];
  #tree: ITree<T>;
  #seeded = false;
  readonly #historyRequests = new Map<DifferenceStreamReader<T>, PullMsg>();
  readonly comparator: Comparator<T>;

  constructor(
    materialite: MaterialiteForSourceInternal,
    comparator: Comparator<T>,
    treapConstructor: (comparator: Comparator<T>) => ITree<T>,
  ) {
    this._materialite = materialite;
    this.#stream = new RootDifferenceStream<T>(this);
    this.#tree = treapConstructor(comparator);
    this.comparator = comparator;

    this.#internal = {
      onCommitEnqueue: (version: Version) => {
        for (let i = 0; i < this.#pending.length; i++) {
          const [val, mult] = must(this.#pending[i]);
          // small optimization to reduce operations for replace
          if (i + 1 < this.#pending.length) {
            const [nextVal, nextMult] = must(this.#pending[i + 1]);
            if (
              Math.abs(mult) === 1 &&
              mult === -nextMult &&
              comparator(val, nextVal) === 0
            ) {
              // The tree doesn't allow dupes -- so this is a replace.
              this.#tree = this.#tree.add(nextMult > 0 ? nextVal : val);
              ++i;
              continue;
            }
          }
          if (mult < 0) {
            this.#tree = this.#tree.delete(val);
          } else if (mult > 0) {
            this.#tree = this.#tree.add(val);
          }
        }

        this.#stream.queueData([version, new Multiset(this.#pending)]);
        this.#pending = [];
      },
      // release queues by telling the stream to send data
      onCommitRun: (version: Version) => {
        this.#stream.notify(version);
      },
      onCommitted: (version: Version) => {
        // In case we have direct source observers
        const tree = this.#tree;
        for (const l of this.#listeners) {
          l(tree, version);
        }
        this.#stream.notifyCommitted(version);
      },
      onRollback: () => {
        this.#pending = [];
      },
    };
  }

  withNewOrdering(comp: Comparator<T>): this {
    const ret = this._withNewOrdering(comp);
    if (this.#seeded) {
      ret.seed(this.#tree);
    }
    return ret;
  }

  protected abstract _withNewOrdering(comp: Comparator<T>): this;

  get stream(): DifferenceStream<T> {
    return this.#stream;
  }

  get value() {
    return this.#tree;
  }

  detachPipelines() {
    this.#stream = new DifferenceStream<T>();
  }

  destroy(): void {
    this.detachPipelines();
    this.#listeners.clear();
  }

  on(cb: (value: ITree<T>, version: Version) => void): () => void {
    this.#listeners.add(cb);
    return () => this.#listeners.delete(cb);
  }

  off(fn: (value: ITree<T>, version: Version) => void): void {
    this.#listeners.delete(fn);
  }

  add(v: T): this {
    this.#pending.push([v, 1]);
    this._materialite.addDirtySource(this.#internal);
    return this;
  }

  delete(v: T): this {
    this.#pending.push([v, -1]);
    this._materialite.addDirtySource(this.#internal);
    return this;
  }

  /**
   * Seeds the source with historical data.
   *
   * Does not send historical data to downstreams
   * unless they have asked for it.
   */
  seed(values: Iterable<T>): this {
    for (const v of values) {
      this.#tree = this.#tree.add(v);
    }
    this.#seeded = true;
    for (const [downstream, message] of this.#historyRequests) {
      this.#historyRequests.delete(downstream);
      this.#sendHistoricalData(message, downstream);
    }
    return this;
  }

  get(key: T): T | undefined {
    const ret = this.#tree.get(key);
    if (ret === null) {
      return undefined;
    }
    return ret;
  }

  processMessage(
    message: Request,
    downstream: DifferenceStreamReader<T>,
  ): void {
    switch (message.type) {
      case 'pull': {
        this.#sendHistoricalData(message, downstream);
        break;
      }
    }
  }

  #sendHistoricalData(message: Request, downstream: DifferenceStreamReader<T>) {
    if (!this.#seeded) {
      // wait till we're seeded.
      this.#historyRequests.set(downstream, message);
      return;
    }

    const response = createPullResponseMessage(message);
    // This is problematic under the current model of how we run the graph.
    // As in, I don't think this'll work for operators with many inputs.
    // So this presents another reason to move to optimistically running the graph
    // as soon as data is enqueued and making the operators
    // able to handle partial inputs. Something I thought avoiding would be simpler but turns out the opposite.
    // The other reason is interactive transactions as discussed with Erik
    // For interactive transactions we also can't wait until all inputs have been updated
    // before running the graph.
    downstream.enqueue([
      this._materialite.getVersion(),
      new Multiset(asEntries(this.#tree, message)),
      response,
    ]);
    downstream.run(this._materialite.getVersion());
    downstream.notify(this._materialite.getVersion());
    downstream.notifyCommitted(this._materialite.getVersion());
  }
}

export class MutableSetSource<T extends object> extends SetSource<T> {
  constructor(
    materialite: MaterialiteForSourceInternal,
    comparator: Comparator<T>,
  ) {
    super(materialite, comparator, comparator => new Treap(comparator));
  }

  protected _withNewOrdering(comp: Comparator<T>): this {
    return new MutableSetSource<T>(this._materialite, comp) as this;
  }
}

function asEntries<T>(m: ITree<T>, _message: Request): Iterable<Entry<T>> {
  // message will contain hoisted expressions so we can do relevant
  // index selection against the source.
  // const after = hoisted.expressions.filter((e) => e._tag === "after")[0];
  // if (after && after.comparator === comparator) {
  //   return {
  //     [Symbol.iterator]() {
  //       return gen(m.iteratorAfter(after.cursor));
  //     },
  //   };
  // }
  return {
    [Symbol.iterator]() {
      return gen(m);
    },
  };
}

function* gen<T>(m: Iterable<T>) {
  for (const v of m) {
    yield [v, 1] as const;
  }
}
