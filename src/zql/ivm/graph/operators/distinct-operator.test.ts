import {expect, test} from 'vitest';
import {Multiset} from '../../multiset.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {DistinctOperator} from './distinct-operator.js';
import {NoOp} from './operator.js';

test('calls effect with raw difference events', () => {
  const inputWriter = new DifferenceStreamWriter<string>();
  const inputReader = inputWriter.newReader();
  const output = new DifferenceStreamWriter<string>();
  const outputReader = output.newReader();
  outputReader.setOperator(new NoOp());
  new DistinctOperator(inputReader, output);

  let version = 1;
  inputWriter.queueData([
    version,
    new Multiset([
      ['a', 1],
      ['b', 2],
      ['a', -1],
      ['c', 1],
    ]),
  ]);
  inputWriter.notify(version);
  inputWriter.notifyCommitted(version);
  expect(outputReader.drain(version)).toEqual([
    [
      version,
      new Multiset([
        ['b', 1],
        ['c', 1],
      ]),
    ],
  ]);

  version++;
  inputWriter.queueData([
    version,
    new Multiset([
      ['a', -1],
      ['b', 2],
      ['a', 1],
      ['c', -1],
    ]),
  ]);
  inputWriter.queueData([version, new Multiset([['a', 1]])]);
  inputWriter.queueData([version, new Multiset([['b', -2]])]);
  inputWriter.notify(version);
  inputWriter.notifyCommitted(version);

  const items = outputReader.drain(version);
  expect(items.length).toBe(1);
  const entries = [...items[0][1].entries];
  expect(entries).toEqual([
    ['a', 1],
    ['c', -1],
  ]);
});
