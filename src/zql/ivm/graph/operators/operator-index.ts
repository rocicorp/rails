import {Primitive} from '../../../ast/ast.js';
import {Entry, Multiset} from '../../multiset.js';
import {JoinResult, StrOrNum, isJoinResult, joinSymbol} from '../../types.js';

export class Index<K extends Primitive, V> {
  readonly #index = new Map<K, Entry<V>[]>();
  readonly #getValueIdentity;

  constructor(getValueIdentity: (value: V) => string | number) {
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

  join<
    VO,
    AAlias extends string | undefined,
    BAlias extends string | undefined,
  >(
    aAlias: AAlias | undefined,
    other: Index<K, VO>,
    bAlias: BAlias | undefined,
    getBValueIdentity: (v: VO) => StrOrNum,
  ): Multiset<JoinResult<V, VO, AAlias, BAlias>> {
    const ret: (readonly [JoinResult<V, VO, AAlias, BAlias>, number])[] = [];
    for (const [key, entry] of this.#index) {
      const otherEntry = other.#index.get(key);
      if (otherEntry === undefined) {
        continue;
      }
      for (const [v1, m1] of entry) {
        for (const [v2, m2] of otherEntry) {
          // Flatten our join results so we don't
          // end up arbitrarily deep after many joins.
          let value: JoinResult<V, VO, AAlias, BAlias>;
          if (isJoinResult(v1) && isJoinResult(v2)) {
            value = {...v1, ...v2, id: v1.id + '_' + v2.id} as JoinResult<
              V,
              VO,
              AAlias,
              BAlias
            >;
          } else if (isJoinResult(v1)) {
            value = {
              ...v1,
              [bAlias!]: v2,
              id: v1.id + '_' + getBValueIdentity(v2),
            } as JoinResult<V, VO, AAlias, BAlias>;
          } else if (isJoinResult(v2)) {
            value = {
              ...v2,
              [aAlias!]: v1,
              id: this.#getValueIdentity(v1) + '_' + v2.id,
            } as JoinResult<V, VO, AAlias, BAlias>;
          } else {
            value = {
              [joinSymbol]: true,
              id: this.#getValueIdentity(v1) + '_' + getBValueIdentity(v2),
              [aAlias!]: v1,
              [bAlias!]: v2,
            } as JoinResult<V, VO, AAlias, BAlias>;
          }
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
