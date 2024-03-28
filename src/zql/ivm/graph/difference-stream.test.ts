import {expect, test} from 'vitest';
import {DifferenceStream} from './difference-stream.js';
import {Multiset} from '../multiset.js';
import {Materialite} from '../materialite.js';

type Elem = {x: number};
test('map', () => {
  const s = new DifferenceStream<Elem>();
  let expectRan = 0;
  s.map(x => ({
    x: x.x * 2,
  })).effect(x => {
    expectRan++;
    expect(x).toEqual({x: 4});
  });

  s.queueData([
    1,
    new Multiset([
      [{x: 2}, 1],
      [{x: 2}, 1],
      [{x: 2}, 1],
    ]),
  ]);
  s.notify(1);
  s.notifyCommitted(1);

  expect(expectRan).toBe(3);
});

test('filter', () => {
  const s = new DifferenceStream<Elem>();
  let expectRan = 0;
  s.filter(x => x.x % 2 === 0).effect(x => {
    expectRan++;
    expect(x).toEqual({x: 2});
  });

  s.queueData([
    1,
    new Multiset([
      [{x: 1}, 1],
      [{x: 2}, 1],
      [{x: 3}, 1],
    ]),
  ]);
  s.notify(1);
  s.notifyCommitted(1);

  expect(expectRan).toBe(1);
});

test('count', () => {
  const s = new DifferenceStream<Elem>();
  let expectRan = 0;
  let expectedCount = 3;
  s.count('count').effect((x, mult) => {
    if (mult > 0) {
      expectRan++;
      expect(x.count).toBe(expectedCount);
    }
  });

  s.queueData([
    1,
    new Multiset([
      [{x: 1}, 1],
      [{x: 2}, 1],
      [{x: 3}, 1],
    ]),
  ]);
  s.notify(1);
  s.notifyCommitted(1);

  expect(expectRan).toBe(1);

  s.queueData([
    2,
    new Multiset([
      [{x: 1}, 1],
      [{x: 2}, 1],
      [{x: 3}, 1],
      [{x: 3}, 1],
    ]),
  ]);
  expectedCount = 7;
  s.notify(2);
  s.notifyCommitted(2);

  expect(expectRan).toBe(2);
});

test('map, filter, linearCount', () => {
  const s = new DifferenceStream<Elem>();
  let expectRan = 0;
  let expectedCount = 1;
  s.map(x => ({
    x: x.x * 2,
  }))
    .filter(x => x.x % 2 === 0)
    .count('count')
    .effect((x, mult) => {
      if (mult > 0) {
        expectRan++;
        expect(x.count).toBe(expectedCount);
      }
    });

  s.queueData([1, new Multiset([[{x: 1}, 1]])]);
  s.notify(1);
  s.notifyCommitted(1);

  expect(expectRan).toBe(1);

  s.queueData([
    2,
    new Multiset([
      [{x: 1}, 1],
      [{x: 2}, 1],
    ]),
  ]);
  expectedCount = 3;
  s.notify(2);
  s.notifyCommitted(2);

  expect(expectRan).toBe(2);
});

test('cleaning up the only user of a stream cleans up the entire pipeline', () => {
  const materialite = new Materialite();
  const set = materialite.newSetSource<Elem>((l, r) => l.x - r.x);

  let notifyCount = 0;
  const final = set.stream
    .effect(_ => notifyCount++)
    .effect(_ => notifyCount++)
    .effect(_ => notifyCount++);

  set.add({x: 1});
  expect(notifyCount).toBe(3);
  final.destroy();
  set.add({x: 2});
  // stream was cleaned up, all the way to the root
  // so no more notifications.
  expect(notifyCount).toBe(3);
});

test('cleaning up the only user of a stream cleans up the entire pipeline but stops at a used fork', () => {
  const materialite = new Materialite();
  const set = materialite.newSetSource<Elem>((l, r) => l.x - r.x);

  let notifyCount = 0;
  const stream1 = set.stream.effect(_ => notifyCount++);
  const stream2 = stream1.effect(_ => notifyCount++);
  const stream3 = stream1.effect(_ => notifyCount++);
  // Forked stream which creates this graph:
  /*
      stream1
      /      \
  stream2   stream3
  */

  set.add({x: 1});
  expect(notifyCount).toBe(3);
  stream3.destroy();
  set.add({x: 2});
  // stream was cleaned up to fork, so still 2 notification
  expect(notifyCount).toBe(5);
  stream2.destroy();
  set.add({x: 3});
  // stream was cleaned up, all the way to the root
  // so no more notifications.
  expect(notifyCount).toBe(5);
});
