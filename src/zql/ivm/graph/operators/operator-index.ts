import {Entity} from '../../../../generate.js';
import {Primitive} from '../../../ast/ast.js';
import {Entry} from '../../multiset.js';

export class Index<K extends Primitive, V extends Entity> {
  readonly #index = new Map<K, Entry<V>[]>();

  constructor() {}

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

  compact(keys: K[] = []) {
    function consolidateValues(values: Entry<V>[]): Entry<V>[] {
      const consolidated = new Map<string, Entry<V>>();
      for (const [value, multiplicity] of values) {
        if (multiplicity === 0) {
          continue;
        }
        const existing = consolidated.get(value.id);
        if (existing === undefined) {
          consolidated.set(value.id, [value, multiplicity]);
        } else {
          const sum = existing[1] + multiplicity;
          if (sum === 0) {
            consolidated.delete(value.id);
          } else {
            consolidated.set(value.id, [value, sum]);
          }
        }
      }

      return [...consolidated.values()];
    }

    // spread `keys` b/c if we do not then when we add below the iterator will continue.
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
