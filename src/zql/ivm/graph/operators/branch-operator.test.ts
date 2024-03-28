import {expect, test} from 'vitest';
import {Multiset} from '../../multiset.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {BranchOperator} from './branch-operator.js';
import {NoOp} from './operator.js';

test('All branches gets notified', () => {
  const inputWriter = new DifferenceStreamWriter<number>();
  const inputReader = inputWriter.newReader();
  const outputs = [
    new DifferenceStreamWriter<number>(),
    new DifferenceStreamWriter<number>(),
    new DifferenceStreamWriter<number>(),
  ];

  new BranchOperator(inputReader, outputs);

  const outReaders = outputs.map(o => o.newReader());
  outReaders.forEach(o => o.setOperator(new NoOp()));

  const version = 1;

  inputWriter.queueData([
    version,
    new Multiset([
      [1, 1],
      [2, 2],
      [1, -1],
      [2, -2],
    ]),
  ]);
  inputWriter.notify(version);
  inputWriter.notifyCommitted(version);

  for (const outReader of outReaders) {
    const items = outReader.drain(version);

    expect(items.length).toBe(1);
    const entries = [...items[0][1].entries];
    expect(entries).toEqual([
      [1, 1],
      [2, 2],
      [1, -1],
      [2, -2],
    ]);
  }
});
