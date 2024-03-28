export function* mapIter<T, U>(
  iter: Iterable<T>,
  f: (t: T, index: number) => U,
): Iterable<U> {
  let index = 0;
  for (const t of iter) {
    yield f(t, index++);
  }
}

/**
 * Flat maps the items returned from the iterable.
 *
 * `iter` is a lambda that returns an iterable
 * so this function can return an `IterableIterator`
 */
export function flatMapIter<T, U>(
  iter: () => Iterable<T>,
  f: (t: T, index: number) => Iterable<U>,
) {
  return {
    *[Symbol.iterator]() {
      let index = 0;
      for (const t of iter()) {
        yield* f(t, index++);
      }
    },
  };
}
