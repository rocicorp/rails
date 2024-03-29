export function* mapIter<T, U>(
  iter: Iterable<T>,
  f: (t: T, index: number) => U,
): Iterable<U> {
  let index = 0;
  for (const t of iter) {
    yield f(t, index++);
  }
}

export function filterIter<V, O extends V>(
  iter: Iterable<V>,
  p: (value: V, index: number) => value is O,
): Iterable<O>;
export function filterIter<V>(
  iter: Iterable<V>,
  p: (value: V, index: number) => boolean,
): Iterable<V>;
export function* filterIter<V>(
  iter: Iterable<V>,
  p: (value: V, index: number) => boolean,
): Iterable<V> {
  let index = 0;
  for (const value of iter) {
    if (p(value, index++)) {
      yield value;
    }
  }
}
