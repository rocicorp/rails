import {expect, test} from 'vitest';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {ReduceOperator} from './reduce-operator.js';
import {Multiset} from '../../multiset.js';
import {NoOp} from './operator.js';

type Thing = {
  id: string;
  a: number;
  b: string;
};

type Reduction = {
  id: string;
  sum: number;
};

test('collects all things with the same key', () => {
  const inputWriter = new DifferenceStreamWriter<Thing>();
  const inputReader = inputWriter.newReader();
  const output = new DifferenceStreamWriter<Reduction>();

  function getKey(t: Thing) {
    return t.b;
  }
  function getValueIdentity(t: Thing) {
    return t.id;
  }

  new ReduceOperator(
    inputReader,
    output,
    getValueIdentity,
    getKey,
    (group: Iterable<Thing>) => {
      let sum = 0;
      let id = '';
      for (const item of group) {
        id = item.b;
        sum += item.a;
      }

      return {
        id,
        sum,
      };
    },
  );

  const outReader = output.newReader();
  outReader.setOperator(new NoOp());

  inputWriter.queueData([
    1,
    new Multiset([
      [
        {
          id: 'a',
          a: 1,
          b: 'x',
        },
        1,
      ],
      [
        {
          id: 'b',
          a: 2,
          b: 'x',
        },
        2,
      ],
    ]),
  ]);
  check([[{id: 'x', sum: 5}, 1]]);

  // retract an item
  inputWriter.queueData([
    1,
    new Multiset([
      [
        {
          id: 'a',
          a: 1,
          b: 'x',
        },
        -1,
      ],
    ]),
  ]);
  check([
    [{id: 'x', sum: 5}, -1],
    [{id: 'x', sum: 4}, 1],
  ]);

  // fully retract items that constitue a grouping
  inputWriter.queueData([
    1,
    new Multiset([
      [
        {
          id: 'b',
          a: 2,
          b: 'x',
        },
        -2,
      ],
    ]),
  ]);
  check([[{id: 'x', sum: 4}, -1]]);

  // add more entries
  inputWriter.queueData([
    1,
    new Multiset([
      [
        {
          id: 'a',
          a: 1,
          b: 'c',
        },
        1,
      ],
    ]),
  ]);
  check([[{id: 'c', sum: 1}, 1]]);
  inputWriter.queueData([
    1,
    new Multiset([
      [
        {
          id: 'b',
          a: 2,
          b: 'c',
        },
        1,
      ],
    ]),
  ]);
  check([
    [{id: 'c', sum: 1}, -1],
    [{id: 'c', sum: 3}, 1],
  ]);

  inputWriter.queueData([
    1,
    new Multiset([
      [
        {
          id: 'a',
          a: 1,
          b: 'c',
        },
        -1,
      ],
      [
        {
          id: 'a',
          a: 2,
          b: 'c',
        },
        1,
      ],
    ]),
  ]);
  check([
    [{id: 'c', sum: 3}, -1],
    [{id: 'c', sum: 4}, 1],
  ]);

  function check(expected: [Reduction, number][]) {
    inputWriter.notify(1);
    inputWriter.notifyCommitted(1);
    const items = outReader.drain(1);
    expect(items.length).toBe(1);
    const entry = items[0];
    expect([...entry[1].entries]).toEqual(expected);
  }
});
