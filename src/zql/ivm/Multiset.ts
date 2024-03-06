export type Entry<T> = readonly [T, Multiplicity];
export type Multiplicity = number;

/**
 * The core concept for IVM.
 * https://www.feldera.com/blog/Z-sets
 *
 * A simplified explanation:
 * - An entry in the set with multiplicity `> 0` means to add the item N times
 * - An entry in the set with multiplicity `< 0` means to remove the item N times
 *
 * This implementation is lazy in that consumers of the multiset
 * can stop pulling values once they're satisfied.
 *
 * I.e., a caller of `map` doesn't have to map the entire set.
 *
 * This is useful for supporting `limit` clauses. Once we hit
 * out limit there's no need to continue our compute pipeline.
 *
 * Methods left out of the current iteration:
 * - equals (for recursive queries)
 * - normalize / compact (for recursive queries)
 * - iterate (for recursive queries)
 * - difference (for EXCEPT)
 * - concat (for UNION)
 */
export class Multiset<T> {
  #entries: Iterable<Entry<T>>;
  constructor(entries: Iterable<Entry<T>>) {
    this.#entries = entries;
  }

  get entries() {
    return this.#entries;
  }

  negate(): Multiset<T> {
    return new Multiset(
      genMap(this.entries, ([value, multiplicity]) => [value, -multiplicity]),
    );
  }

  map<R>(f: (value: T) => R): Multiset<R> {
    return new Multiset(
      genMap(this.entries, ([value, multiplicity]) => [f(value), multiplicity]),
    );
  }

  filter(f: (value: T) => boolean): Multiset<T> {
    return new Multiset(genFilter(this.entries, ([value, _]) => f(value)));
  }

  toString() {
    return this.entries.toString();
  }
}

function genMap<T, U>(s: Iterable<T>, cb: (x: T) => U) {
  function* gen() {
    for (const x of s) {
      yield cb(x);
    }
  }
  return {
    *[Symbol.iterator]() {
      yield* gen();
    },
  };
}

function genFilter<T>(s: Iterable<T>, cb: (x: T) => boolean) {
  function* gen() {
    for (const x of s) {
      if (cb(x)) {
        yield x;
      }
    }
  }
  return {
    *[Symbol.iterator]() {
      yield* gen();
    },
  };
}
