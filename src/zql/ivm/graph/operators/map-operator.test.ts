import {expect, test} from 'vitest';
import {MapOperator} from './map-operator.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {NoOp} from './operator.js';
import {Multiset} from '../../multiset.js';

test('lazy', () => {
  const inputWriter = new DifferenceStreamWriter<number>();
  const inputReader = inputWriter.newReader();
  const output = new DifferenceStreamWriter<number>();

  let called = false;
  new MapOperator(inputReader, output, x => {
    called = true;
    return x;
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

  // we run the graph but the mapper is not run until we pull on it
  expect(called).toBe(false);

  const items = outReader.drain(1);
  // consume all the rows
  [...items[0][1].entries];
  expect(called).toBe(true);
});

test('applies to rows', () => {
  const inputWriter = new DifferenceStreamWriter<number>();
  const inputReader = inputWriter.newReader();
  const output = new DifferenceStreamWriter<number>();

  new MapOperator(inputReader, output, x => x * 2);

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

  expect(items.length).toBe(1);
  const entries = [...items[0][1].entries];
  expect(entries).toMatchObject([
    [2, 1],
    [4, 2],
    [2, -1],
    [4, -2],
  ]);
});
