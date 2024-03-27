import {expect, test} from 'vitest';
import {Multiset} from '../../multiset.js';
import {NoOp} from './operator.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {
  FullAvgOperator,
  FullCountOperator,
  FullSumOperator,
} from './full-agg-operators.js';

test('count', () => {
  const inputWriter = new DifferenceStreamWriter<{x: string}>();
  const inputReader = inputWriter.newReader();
  const output = new DifferenceStreamWriter<{x: string; count: number}>();

  new FullCountOperator(inputReader, output, 'count');

  const outReader = output.newReader();
  outReader.setOperator(new NoOp());

  // does not count things that do not exist
  inputWriter.queueData([
    1,
    new Multiset([
      [
        {
          x: 'foo',
        },
        0,
      ],
    ]),
  ]);
  check(1, [[{x: 'foo', count: 0}, 1]]);

  // counts multiplicity of 1
  inputWriter.queueData([
    2,
    new Multiset([
      [
        {
          x: 'foo',
        },
        1,
      ],
    ]),
  ]);
  check(2, [
    [{x: 'foo', count: 0}, -1],
    [{x: 'foo', count: 1}, 1],
  ]);

  // decrements if an item is removed
  inputWriter.queueData([
    3,
    new Multiset([
      [
        {
          x: 'foo',
        },
        -1,
      ],
    ]),
  ]);
  check(3, [
    [{x: 'foo', count: 1}, -1],
    [{x: 'foo', count: 0}, 1],
  ]);

  // double counts doubly present items
  inputWriter.queueData([
    4,
    new Multiset([
      [
        {
          x: 'foo',
        },
        2,
      ],
    ]),
  ]);
  check(4, [
    [{x: 'foo', count: 0}, -1],
    [{x: 'foo', count: 2}, 1],
  ]);

  function check(
    version: number,
    expected: [{x: string; count: number}, number][],
  ) {
    inputWriter.notify(version);
    inputWriter.notifyCommitted(version);

    const items = outReader.drain(version);
    expect([...items![1].entries]).toEqual(expected);
  }
});

test('average', () => {
  const inputWriter = new DifferenceStreamWriter<{x: number}>();
  const inputReader = inputWriter.newReader();
  const output = new DifferenceStreamWriter<{x: number}>();

  new FullAvgOperator(inputReader, output, 'x', 'x');

  const outReader = output.newReader();
  outReader.setOperator(new NoOp());

  // does not avg things that do not exist
  inputWriter.queueData([
    1,
    new Multiset([
      [
        {
          x: 1,
        },
        0,
      ],
    ]),
  ]);
  check(1, [[{x: 0}, 1]]);

  // averages things that exist
  inputWriter.queueData([
    1,
    new Multiset([
      [
        {
          x: 1,
        },
        1,
      ],
      [
        {
          x: 2,
        },
        1,
      ],
      [
        {
          x: 3,
        },
        1,
      ],
    ]),
  ]);
  check(1, [
    [{x: 0}, -1],
    [{x: 2}, 1],
  ]);

  // updates the average when new items enter
  inputWriter.queueData([
    1,
    new Multiset([
      [
        {
          x: 4,
        },
        1,
      ],
      [
        {
          x: 5,
        },
        1,
      ],
    ]),
  ]);
  check(1, [
    [{x: 2}, -1],
    [{x: 3}, 1],
  ]);

  // updates the average when items leave
  inputWriter.queueData([
    1,
    new Multiset([
      [
        {
          x: 4,
        },
        -1,
      ],
      [
        {
          x: 5,
        },
        -1,
      ],
    ]),
  ]);
  check(1, [
    [{x: 3}, -1],
    [{x: 2}, 1],
  ]);

  function check(version: number, expected: [{x: number}, number][]) {
    inputWriter.notify(version);
    inputWriter.notifyCommitted(version);

    const items = outReader.drain(version);
    expect([...items![1].entries]).toEqual(expected);
  }
});

test('sum', () => {
  const inputWriter = new DifferenceStreamWriter<{x: number}>();
  const inputReader = inputWriter.newReader();
  const output = new DifferenceStreamWriter<{x: number}>();

  new FullSumOperator(inputReader, output, 'x', 'x');

  const outReader = output.newReader();
  outReader.setOperator(new NoOp());

  // does not sum things that do not exist
  inputWriter.queueData([
    1,
    new Multiset([
      [
        {
          x: 1,
        },
        0,
      ],
    ]),
  ]);
  check(1, [[{x: 0}, 1]]);

  // sums things that exist
  inputWriter.queueData([
    1,
    new Multiset([
      [
        {
          x: 1,
        },
        1,
      ],
      [
        {
          x: 2,
        },
        1,
      ],
      [
        {
          x: 3,
        },
        1,
      ],
    ]),
  ]);
  check(1, [
    [{x: 0}, -1],
    [{x: 6}, 1],
  ]);

  // updates the sum when new items enter
  inputWriter.queueData([
    1,
    new Multiset([
      [
        {
          x: 4,
        },
        1,
      ],
      [
        {
          x: 5,
        },
        1,
      ],
    ]),
  ]);
  check(1, [
    [{x: 6}, -1],
    [{x: 15}, 1],
  ]);

  // updates the sum when items leave
  inputWriter.queueData([
    1,
    new Multiset([
      [
        {
          x: 4,
        },
        -1,
      ],
      [
        {
          x: 5,
        },
        -1,
      ],
    ]),
  ]);
  check(1, [
    [{x: 15}, -1],
    [{x: 6}, 1],
  ]);

  function check(version: number, expected: [{x: number}, number][]) {
    inputWriter.notify(version);
    inputWriter.notifyCommitted(version);

    const items = outReader.drain(version);
    expect([...items![1].entries]).toEqual(expected);
  }
});
