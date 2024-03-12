import {expect, test} from 'vitest';
import {Multiset} from './multiset.js';

type Ref<T> = {
  v: T;
};
test('operations against a multiset are lazy', () => {
  const set = new Multiset([
    [1, 1],
    [2, 1],
    [3, 1],
  ]);

  function check(
    op: (s: Multiset<number>, count: Ref<number>) => Multiset<number>,
    expectedCount: number,
  ) {
    const count = {
      v: 0,
    };
    const newSet = op(set, count);

    expect(count.v).toBe(0);

    // consume the set
    const arr1 = [...newSet.entries];
    expect(count.v).toBe(expectedCount);

    // Being lazy doesn't mean we can't restart the computation -- consume the set again
    const arr2 = [...newSet.entries];
    expect(count.v).toBe(expectedCount * 2);

    expect(arr1).toEqual(arr2);
  }

  check(
    (set: Multiset<number>, count: Ref<number>) =>
      set.map(v => {
        count.v++;
        return v * 2;
      }),
    3,
  );
  check(
    (set: Multiset<number>, count: Ref<number>) =>
      set.filter(_ => {
        count.v++;
        return false;
      }),
    3,
  );

  check(
    (set: Multiset<number>, count: Ref<number>) =>
      set
        .map(v => {
          count.v++;
          return v * 2;
        })
        .filter(_ => {
          count.v++;
          return true;
        })
        .negate(),
    6,
  );
});
