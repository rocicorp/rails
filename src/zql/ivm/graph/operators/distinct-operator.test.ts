import {expect, test} from 'vitest';
import {Multiset} from '../../multiset.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {DistinctOperator} from './distinct-operator.js';
import {NoOp} from './operator.js';

test('calls effect with raw difference events', () => {
  type E = {
    id: string;
  };
  const inputWriter = new DifferenceStreamWriter<E>();
  const inputReader = inputWriter.newReader();
  const output = new DifferenceStreamWriter<E>();
  const outputReader = output.newReader();
  outputReader.setOperator(new NoOp());
  new DistinctOperator(inputReader, output);

  let version = 1;
  inputWriter.queueData([
    version,
    new Multiset([
      [{id: 'a'}, 1],
      [{id: 'b'}, 2],
      [{id: 'a'}, -1],
      [{id: 'c'}, 1],
    ]),
  ]);
  inputWriter.notify(version);
  inputWriter.notifyCommitted(version);
  expect(outputReader.drain(version)).toEqual([
    [
      version,
      new Multiset([
        [{id: 'b'}, 1],
        [{id: 'c'}, 1],
      ]),
    ],
  ]);

  version++;
  inputWriter.queueData([
    version,
    new Multiset([
      [{id: 'a'}, -1],
      [{id: 'b'}, 2],
      [{id: 'a'}, 1],
      [{id: 'c'}, -1],
    ]),
  ]);
  inputWriter.queueData([version, new Multiset([[{id: 'a'}, 1]])]);
  inputWriter.queueData([version, new Multiset([[{id: 'b'}, -2]])]);
  inputWriter.notify(version);
  inputWriter.notifyCommitted(version);

  const items = outputReader.drain(version);
  expect(items.length).toBe(1);
  const entries = [...items[0][1].entries];
  expect(entries).toEqual([
    [{id: 'a'}, 1],
    [{id: 'c'}, -1],
  ]);
});
