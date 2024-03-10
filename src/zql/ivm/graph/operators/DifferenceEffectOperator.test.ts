import {expect, test} from 'vitest';
import {DifferenceStreamWriter} from '../DifferenceStreamWriter.js';
import {DifferenceEffectOperator} from './DifferenceEffectOperator.js';
import {Multiset} from '../../Multiset.js';

test('calls effect with raw difference events', () => {
  const inputWriter = new DifferenceStreamWriter<number>();
  const inputReader = inputWriter.newReader();
  const output = new DifferenceStreamWriter<number>();

  let called = false;
  let value = 0;
  let mult = 0;
  new DifferenceEffectOperator(inputReader, output, (v, m) => {
    called = true;
    value = v;
    mult = m;
  });

  inputWriter.queueData([1, new Multiset([[1, 1]])]);
  inputWriter.notify(1);

  // effect not run until commit
  expect(called).toBe(false);

  inputWriter.notifyCommitted(1);
  expect(called).toBe(true);
  expect(value).toBe(1);
  expect(mult).toBe(1);

  called = false;
  value = 0;
  mult = 0;
  inputWriter.queueData([2, new Multiset([[1, -1]])]);
  inputWriter.notify(2);

  // effect not run until commit
  expect(called).toBe(false);

  inputWriter.notifyCommitted(2);
  expect(called).toBe(true);
  expect(value).toBe(1);
  expect(mult).toBe(-1);
});
