import {expect, test} from 'vitest';
import {FilterOperator} from './filter-operator.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {Multiset} from '../../multiset.js';
import {NoOp} from './operator.js';

test('does not emit any rows that fail the filter', () => {
  const inputWriter = new DifferenceStreamWriter<number>();
  const inputReader = inputWriter.newReader();
  const output = new DifferenceStreamWriter<number>();

  new FilterOperator(inputReader, output, _ => false);

  const outReader = output.newReader();
  outReader.setOperator(new NoOp());

  inputWriter.queueData([
    1,
    new Multiset([
      [1, 1],
      [2, 2],
      [1, -1],
      [2, -2],
    ]),
  ]);
  inputWriter.notify(1);
  inputWriter.notifyCommitted(1);
  const items = outReader.drain(1);

  expect([...items![1].entries].length).toBe(0);
});

test('emits all rows that pass the filter (including deletes / retractions)', () => {
  const inputWriter = new DifferenceStreamWriter<number>();
  const inputReader = inputWriter.newReader();
  const output = new DifferenceStreamWriter<number>();

  new FilterOperator(inputReader, output, _ => true);

  const outReader = output.newReader();
  outReader.setOperator(new NoOp());

  inputWriter.queueData([
    1,
    new Multiset([
      [1, 1],
      [2, 2],
      [1, -1],
      [2, -2],
    ]),
  ]);
  inputWriter.notify(1);
  inputWriter.notifyCommitted(1);
  const items = outReader.drain(1);

  const entries = [...items![1].entries];
  expect(entries).toEqual([
    [1, 1],
    [2, 2],
    [1, -1],
    [2, -2],
  ]);
});

test('test that filter is lazy / the filter is not actually run until we pull on it', () => {
  const inputWriter = new DifferenceStreamWriter<number>();
  const inputReader = inputWriter.newReader();
  const output = new DifferenceStreamWriter<number>();

  let called = false;
  new FilterOperator(inputReader, output, _ => {
    called = true;
    return true;
  });

  const outReader = output.newReader();
  outReader.setOperator(new NoOp());

  inputWriter.queueData([
    1,
    new Multiset([
      [1, 1],
      [2, 2],
      [1, -1],
      [2, -2],
    ]),
  ]);
  inputWriter.notify(1);
  inputWriter.notifyCommitted(1);

  // we run the graph but the filter is not run until we pull on it
  expect(called).toBe(false);

  const items = outReader.drain(1);
  // consume all the rows
  [...items![1].entries];
  expect(called).toBe(true);
});
