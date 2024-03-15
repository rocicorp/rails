export function* mapIter<T, U>(
  iter: Iterable<T>,
  f: (t: T, index: number) => U,
): Iterable<U> {
  let index = 0;
  for (const t of iter) {
    yield f(t, index++);
  }
}

export function* flatMapIter<T, U>(
  iter: Iterable<T>,
  f: (t: T, index: number) => Iterable<U>,
): Iterable<U> {
  let index = 0;
  for (const t of iter) {
    yield* f(t, index++);
  }
}
