import {Comparator, ITree} from '@vlcn.io/ds-and-algos/types';
import {MaterialiteForSourceInternal} from '../materialite.js';
import {DifferenceStream} from '../graph/difference-stream.js';
import {SourceInternal, Source} from './source.js';
import {Entry, Multiset} from '../multiset.js';
import {Version} from '../types.js';
import {Treap} from '@vlcn.io/ds-and-algos/Treap';
import {must} from '../../error/invariant-violation.js';

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

    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;
    this.#internal = {
      onCommitPhase1(version: Version) {
        for (let i = 0; i < self.#pending.length; i++) {
          const [val, mult] = must(self.#pending[i]);
          // small optimization to reduce operations for replace
          if (i + 1 < self.#pending.length) {
            const [nextVal, nextMult] = must(self.#pending[i + 1]);
            if (
              Math.abs(mult) === 1 &&
              mult === -nextMult &&
              comparator(val, nextVal) === 0
            ) {
              // The tree doesn't allow dupes -- so this is a replace.
              self.#tree = self.#tree.add(nextMult > 0 ? nextVal : val);
              ++i;
              continue;
            }
          }
          if (mult < 0) {
            self.#tree = self.#tree.delete(val);
          } else if (mult > 0) {
            self.#tree = self.#tree.add(val);
          }
        }

        self.#stream.queueData([version, new Multiset(self.#pending)]);
        self.#pending = [];
      },
      // release queues by telling the stream to send data
      onCommitPhase2(version: Version) {
        self.#stream.notify(version);
      },
      onCommitted(version: Version) {
        // In case we have direct source observers
        const tree = self.#tree;
        for (const l of self.#listeners) {
          l(tree, version);
        }
        self.#stream.notifyCommitted(version);
      },
      onRollback() {
        self.#pending = [];
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

  // TODO: implement these correctly.
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
}

export class MutableSetSource<T> extends SetSource<T> {
  constructor(
    materialite: MaterialiteForSourceInternal,
    // TODO: comarator is really only on the selected set of fields that participate in the `order-by`
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

// TODO:
// A source may be asked to send all information it has down a path.
// In this case we:
// 1. should send it lazily (since a path may have a limit)
// 2. should ensure source order and view order match
// 3. should check for an `AFTER` param so we can resume from a spot

// function asEntries<T>(
//   m: ITree<T>,
//   comparator: Comparator<T>,
//   hoisted: Hoisted,
// ): Iterable<Entry<T>> {
//   const after = hoisted.expressions.filter(e => e._tag === 'after')[0];
//   if (after && after.comparator === comparator) {
//     return {
//       [Symbol.iterator]() {
//         return gen(m.iteratorAfter(after.cursor as any));
//       },
//     };
//   }
//   return {
//     [Symbol.iterator]() {
//       return gen(m);
//     },
//   };
// }

// function* gen<T>(m: Iterable<T>) {
//   for (const v of m) {
//     yield [v, 1] as const;
//   }
// }
