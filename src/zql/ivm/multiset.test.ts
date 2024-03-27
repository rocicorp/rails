import {expect, test} from 'vitest';
import {Multiset} from './multiset.js';

test('negate', () => {
  type Case = {
    name: string;
    input: [number, number][];
    expected: [number, number][];
  };

  const cases: Case[] = [
    {
      name: 'empty',
      input: [],
      expected: [],
    },
    {
      name: 'positive',
      input: [[1, 1]],
      expected: [[1, -1]],
    },
    {
      name: 'negative',
      input: [[1, -1]],
      expected: [[1, 1]],
    },
    {
      name: 'zero',
      input: [[1, 0]],
      expected: [[1, -0]],
    },
    {
      name: 'multiple',
      input: [
        [0, -0],
        [1, 1],
        [2, -2],
        [3, 3],
        [4, -4],
      ],
      expected: [
        [0, 0],
        [1, -1],
        [2, 2],
        [3, -3],
        [4, 4],
      ],
    },
  ];

  for (const c of cases) {
    const actual = [...new Multiset(c.input).negate().entries];
    expect(actual, c.name).toEqual(c.expected);
  }
});

test('map', () => {
  type Case = {
    name: string;
    input: [number, number][];
    expected: [number, number][];
  };

  const cases: Case[] = [
    {
      name: 'empty',
      input: [],
      expected: [],
    },
    {
      name: 'integers',
      input: [
        [1, 1],
        [2, 2],
        [3, -3],
      ],
      expected: [
        [2, 1],
        [4, 2],
        [6, -3],
      ],
    },
  ];

  for (const c of cases) {
    const actual = [...new Multiset(c.input).map(v => v * 2).entries];
    expect(actual, c.name).toEqual(c.expected);
  }
});

test('filter', () => {
  type Case = {
    name: string;
    input: [number, number][];
    expected: [number, number][];
  };

  const cases: Case[] = [
    {
      name: 'empty',
      input: [],
      expected: [],
    },
    {
      name: 'single',
      input: [[-1, 2]],
      expected: [],
    },
    {
      name: 'multiple',
      input: [
        [1, 1],
        [2, -2],
        [-3, 3],
      ],
      expected: [
        [1, 1],
        [2, -2],
      ],
    },
  ];

  for (const c of cases) {
    const actual = [...new Multiset(c.input).filter(v => v > 0).entries];
    expect(actual, c.name).toEqual(c.expected);
  }
});

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
