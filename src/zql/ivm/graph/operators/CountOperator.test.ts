import {expect, test} from 'vitest';
import {DifferenceStreamWriter} from '../DifferenceStreamWriter.js';
import {LinearCountOperator} from './CountOperator.js';
import {Multiset} from '../../Multiset.js';
import {NoOp} from './Operator.js';

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
  expect(final.length).toBe(1);
  expect([...final[0].entries][0][0]).toBe(1);
});
