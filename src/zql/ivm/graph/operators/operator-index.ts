import {Primitive} from '../../../ast/ast.js';
import {Entry, Multiset} from '../../multiset.js';

export class Index<K extends Primitive, V> {
  readonly #index = new Map<K, Entry<V>[]>();
  readonly #getValueIdentity;

  constructor(getValueIdentity: (value: V) => string | symbol | number) {
    this.#getValueIdentity = getValueIdentity;
  }

  add(key: K, value: Entry<V>) {
    let existing = this.#index.get(key);
    if (existing === undefined) {
      existing = [];
      this.#index.set(key, existing);
    }
    existing.push(value);
  }

  extend(index: Index<K, V>) {
    for (const [key, value] of index.#index) {
      for (const entry of value) {
        this.add(key, entry);
      }
    }
  }

  get(key: K): Entry<V>[] {
    return this.#index.get(key) ?? [];
  }

  join<VO, AAlias extends string, BAlias extends string>(
    aAlias: AAlias,
    other: Index<K, VO>,
    bAlias: BAlias,
  ): Multiset<
    {
      [K in AAlias]: V;
    } & {
      [K in BAlias]: VO;
    }
  > {
    const ret: (readonly [
      {
        [K in AAlias]: V;
      } & {
        [K in BAlias]: VO;
      },
      number,
    ])[] = [];
    for (const [key, entry] of this.#index) {
      const otherEntry = other.#index.get(key);
      if (otherEntry === undefined) {
        continue;
      }
      for (const [v1, m1] of entry) {
        for (const [v2, m2] of otherEntry) {
          // Flatten our join results so we don't
          // end up arbitrarily deep after many joins.
          const value = {
            [aAlias]: v1,
            [bAlias]: v2,
          } as {
            [K in AAlias]: V;
          } & {
            [K in BAlias]: VO;
          };
          ret.push([value, m1 * m2] as const);
        }
      }
    }
    return new Multiset(ret);
  }

  compact(keys: K[] = []) {
    const consolidateValues = (values: Entry<V>[]): Entry<V>[] => {
      const consolidated = new Map<string | symbol | number, Entry<V>>();
      for (const [value, multiplicity] of values) {
        if (multiplicity === 0) {
          continue;
        }
        const existing = consolidated.get(this.#getValueIdentity(value));
        if (existing === undefined) {
          consolidated.set(this.#getValueIdentity(value), [
            value,
            multiplicity,
          ]);
        } else {
          const sum = existing[1] + multiplicity;
          if (sum === 0) {
            consolidated.delete(this.#getValueIdentity(value));
          } else {
            consolidated.set(this.#getValueIdentity(value), [value, sum]);
          }
        }
      }

      return [...consolidated.values()];
    };

    const iterableKeys = keys.length !== 0 ? keys : [...this.#index.keys()];
    for (const key of iterableKeys) {
      const entries = this.#index.get(key);
      if (entries === undefined) {
        continue;
      }
      const consolidated = consolidateValues(entries);
      if (consolidated.length !== 0) {
        this.#index.set(key, consolidated);
      } else {
        this.#index.delete(key);
      }
    }
  }
}
