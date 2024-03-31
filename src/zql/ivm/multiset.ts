export type Entry<T> = readonly [T, Multiplicity];
export type Multiplicity = number;

export function genMap<T, U>(s: Iterable<T>, cb: (x: T) => U) {
  return {
    *[Symbol.iterator]() {
      for (const x of s) {
        yield cb(x);
      }
    },
  };
}

export function genFilter<T>(s: Iterable<T>, cb: (x: T) => boolean) {
  return {
    *[Symbol.iterator]() {
      for (const x of s) {
        if (cb(x)) {
          yield x;
        }
      }
    },
  };
}
