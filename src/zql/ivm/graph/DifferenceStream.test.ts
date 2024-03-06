import {expect, test} from 'vitest';
import {DifferenceStream} from './DifferenceStream.js';
import {Multiset} from '../Multiset.js';

test('map', () => {
  const s = new DifferenceStream<number>();
  let expectRan = 0;
  s.map(x => x * 2).effect(x => {
    expectRan++;
    expect(x).toBe(4);
  });

  s.queueData([
    1,
    new Multiset([
      [2, 1],
      [2, 1],
      [2, 1],
    ]),
  ]);
  s.notify(1);
  s.notifyCommitted(1);

  expect(expectRan).toBe(3);
});

test('filter', () => {
  const s = new DifferenceStream<number>();
  let expectRan = 0;
  s.filter(x => x % 2 === 0).effect(x => {
    expectRan++;
    expect(x).toBe(2);
  });

  s.queueData([
    1,
    new Multiset([
      [1, 1],
      [2, 1],
      [3, 1],
    ]),
  ]);
  s.notify(1);
  s.notifyCommitted(1);

  expect(expectRan).toBe(1);
});

test('linearCount', () => {
  const s = new DifferenceStream<number>();
  let expectRan = 0;
  let expectedCount = 3;
  s.linearCount().effect(x => {
    expectRan++;
    expect(x).toBe(expectedCount);
  });

  s.queueData([
    1,
    new Multiset([
      [1, 1],
      [2, 1],
      [3, 1],
    ]),
  ]);
  s.notify(1);
  s.notifyCommitted(1);

  expect(expectRan).toBe(1);

  s.queueData([
    2,
    new Multiset([
      [1, 1],
      [2, 1],
      [3, 1],
      [3, 1],
    ]),
  ]);
  expectedCount = 7;
  s.notify(2);
  s.notifyCommitted(2);

  expect(expectRan).toBe(2);
});

test('map, filter, linearCount', () => {
  const s = new DifferenceStream<number>();
  let expectRan = 0;
  let expectedCount = 1;
  s.map(x => x * 2)
    .filter(x => x % 2 === 0)
    .linearCount()
    .effect(x => {
      expectRan++;
      expect(x).toBe(expectedCount);
    });

  s.queueData([1, new Multiset([[1, 1]])]);
  s.notify(1);
  s.notifyCommitted(1);

  expect(expectRan).toBe(1);

  s.queueData([
    2,
    new Multiset([
      [1, 1],
      [2, 1],
    ]),
  ]);
  expectedCount = 3;
  s.notify(2);
  s.notifyCommitted(2);

  expect(expectRan).toBe(2);
});
