import {Comparator, ITree} from '@vlcn.io/ds-and-algos/types';
import {MaterialiteForSourceInternal} from '../materialite.js';
import {DifferenceStream} from '../graph/difference-stream.js';
import {SourceInternal, Source} from './source.js';
import {Entry, Multiset} from '../multiset.js';
import {Version} from '../types.js';
import {Treap} from '@vlcn.io/ds-and-algos/Treap';
import {must} from '../../error/asserts.js';

/**
 * A source that remembers what values it contains.
 *
 * This allows pipelines that are created after a source already
 * exists to be able to receive historical data.
 *
 */
export abstract class SetSource<T> implements Source<T> {
  #stream: DifferenceStream<T>;
  readonly #internal: SourceInternal;
  protected readonly _materialite: MaterialiteForSourceInternal;
  readonly #listeners = new Set<(data: ITree<T>, v: Version) => void>();
  #pending: Entry<T>[] = [];
  #tree: ITree<T>;
  readonly comparator: Comparator<T>;

  constructor(
    materialite: MaterialiteForSourceInternal,
    comparator: Comparator<T>,
    treapConstructor: (comparator: Comparator<T>) => ITree<T>,
  ) {
    this._materialite = materialite;
    this.#stream = new DifferenceStream<T>();
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

  abstract withNewOrdering(comp: Comparator<T>): this;

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

  get(key: T): T | null {
    return this.#tree.get(key);
  }
}

export class MutableSetSource<T> extends SetSource<T> {
  constructor(
    materialite: MaterialiteForSourceInternal,
    comparator: Comparator<T>,
  ) {
    super(materialite, comparator, comparator => new Treap(comparator));
  }

  withNewOrdering(comp: Comparator<T>): this {
    const ret = new MutableSetSource<T>(this._materialite, comp);
    this._materialite.materialite.tx(() => {
      for (const v of this.value) {
        ret.add(v);
      }
    });
    return ret as this;
  }
}
