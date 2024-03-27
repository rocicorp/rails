import {expect, test} from 'vitest';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {LinearCountOperator} from './full-agg-operators.js';
import {Multiset} from '../../multiset.js';
import {NoOp} from './operator.js';

test('summing a difference stream', () => {
  const inputWriter = new DifferenceStreamWriter<number>();
  const inputReader = inputWriter.newReader();
  const output = new DifferenceStreamWriter<number>();

  new LinearCountOperator(inputReader, output);

  const outputReader = output.newReader();
  outputReader.setOperator(new NoOp());

  inputWriter.queueData([
    1,
    new Multiset([
      [1, 1],
      [1, -1],
      [2, 1],
      [3, 1],
      [3, -1],
    ]),
  ]);
  inputWriter.notify(1);

  const final = outputReader.drain(1);
  expect([...final![1].entries]).toEqual([[1, 1]]);
});
