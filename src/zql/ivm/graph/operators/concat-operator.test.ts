import {expect, test} from 'vitest';
import {Multiset} from '../../multiset.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {ConcatOperator} from './concat-operator.js';
import {NoOp} from './operator.js';

test('All branches gets notified', () => {
  const inputWriters = [
    new DifferenceStreamWriter<number>(),
    new DifferenceStreamWriter<number>(),
    new DifferenceStreamWriter<number>(),
  ];
  const inputReaders = inputWriters.map(i => i.newReader());
  const output = new DifferenceStreamWriter<number>();

  new ConcatOperator(inputReaders, output);

  const outReader = output.newReader();
  outReader.setOperator(new NoOp());

  const version = 1;

  inputWriters[0].queueData([
    version,
    new Multiset([
      [1, 1],
      [2, 2],
      [1, -1],
      [2, -2],
    ]),
  ]);
  inputWriters[0].notify(version);
  inputWriters[0].notifyCommitted(version);

  const items = outReader.drain(version);
  expect(items.length).toBe(1);
  const entries = [...items[0][1].entries];
  expect(entries).toEqual([
    [1, 1],
    [2, 2],
    [1, -1],
    [2, -2],
  ]);
});
