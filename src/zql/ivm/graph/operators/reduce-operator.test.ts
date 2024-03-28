import {expect, test} from 'vitest';
import {Multiset} from '../../multiset.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {NoOp} from './operator.js';
import {ReduceOperator} from './reduce-operator.js';

type Thing = {
  id: string;
  value: number;
  groupKey: string;
};

type Reduction = {
  id: string;
  sum: number;
};

test('collects all things with the same key', () => {
  const inputWriter = new DifferenceStreamWriter<Thing>();
  const inputReader = inputWriter.newReader();
  const output = new DifferenceStreamWriter<Reduction>();

  function getGroupKey(t: Thing) {
    return t.groupKey;
  }
  function getValueIdentity(t: Thing) {
    return t.id;
  }

  new ReduceOperator(
    inputReader,
    output,
    getValueIdentity,
    getGroupKey,
    (group: Iterable<Thing>) => {
      let sum = 0;
      let id = '';
      for (const item of group) {
        id = item.groupKey;
        sum += item.value;
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
          value: 1,
          groupKey: 'x',
        },
        1,
      ],
      [
        {
          id: 'b',
          value: 2,
          groupKey: 'x',
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
          value: 1,
          groupKey: 'x',
        },
        -1,
      ],
    ]),
  ]);
  check([
    [{id: 'x', sum: 5}, -1],
    [{id: 'x', sum: 4}, 1],
  ]);

  // fully retract items that constitute a grouping
  inputWriter.queueData([
    1,
    new Multiset([
      [
        {
          id: 'b',
          value: 2,
          groupKey: 'x',
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
          value: 1,
          groupKey: 'c',
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
          value: 2,
          groupKey: 'c',
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
          value: 1,
          groupKey: 'c',
        },
        -1,
      ],
      [
        {
          id: 'a',
          value: 2,
          groupKey: 'c',
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
    const entry = outReader.drain(1);
    expect([...entry![1].entries]).toEqual(expected);
  }
});
