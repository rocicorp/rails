import {expect, test} from 'vitest';
import {Multiset} from '../../multiset.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {DifferenceEffectOperator} from './difference-effect-operator.js';

test('calls effect with raw difference events', () => {
  const inputWriter = new DifferenceStreamWriter<number>();
  const inputReader = inputWriter.newReader();
  const output = new DifferenceStreamWriter<number>();

  let called = false;
  let value = 0;
  let multiplicity = 0;
  new DifferenceEffectOperator(inputReader, output, (v, m) => {
    called = true;
    value = v;
    multiplicity = m;
  });

  inputWriter.queueData([1, new Multiset([[1, 1]])]);
  inputWriter.notify(1);

  // effect not run until commit
  expect(called).toBe(false);

  inputWriter.notifyCommitted(1);
  expect(called).toBe(true);
  expect(value).toBe(1);
  expect(multiplicity).toBe(1);

  called = false;
  value = 0;
  multiplicity = 0;
  inputWriter.queueData([2, new Multiset([[1, -1]])]);
  inputWriter.notify(2);

  // effect not run until commit
  expect(called).toBe(false);

  inputWriter.notifyCommitted(2);
  expect(called).toBe(true);
  expect(value).toBe(1);
  expect(multiplicity).toBe(-1);
});
