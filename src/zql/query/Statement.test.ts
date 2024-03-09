import {expect, test} from 'vitest';
import {makeTestContext} from '../context/context.js';
import {z} from 'zod';
import {EntityQuery} from './EntityQuery.js';

const e1 = z.object({
  id: z.string(),
  n: z.number(),
  optStr: z.string().optional(),
});
type E1 = z.infer<typeof e1>;
test('basic materialization', () => {
  const context = makeTestContext();
  const q = new EntityQuery<{fields: E1}>(context, 'e1');

  const view = q.select('id', 'n').where('n', '>', 100).prepare().materialize();

  let callCount = 0;
  view.on(data => {
    ++callCount;
    expect(data).toEqual(expected);
  });

  const items = [
    {id: 'a', n: 1},
    {id: 'b', n: 101},
    {id: 'c', n: 102},
  ] as const;
  const expected: E1[] = [];

  context.getSource('e1').add(items[0]);

  expected.push(items[1]);
  context.getSource('e1').add(items[1]);

  expected.unshift(items[2]);
  context.getSource('e1').add(items[2]);
  expect(callCount).toBe(2);
});

test('sorted materialization', () => {
  const context = makeTestContext();
  type E1 = z.infer<typeof e1>;
  const q = new EntityQuery<{fields: E1}>(context, 'e1');
  const ascView = q.select('id').asc('n').prepare().materialize();
  const descView = q.select('id').desc('n').prepare().materialize();

  context.getSource<E1>('e1').add({
    id: 'a',
    n: 3,
  });
  context.getSource<E1>('e1').add({
    id: 'b',
    n: 2,
  });
  context.getSource<E1>('e1').add({
    id: 'c',
    n: 1,
  });

  expect(ascView.value).toEqual([{id: 'c'}, {id: 'b'}, {id: 'a'}]);
  expect(descView.value).toEqual([{id: 'a'}, {id: 'b'}, {id: 'c'}]);
});

test('sorting is stable via suffixing the primary key to the order', () => {
  const context = makeTestContext();
  type E1 = z.infer<typeof e1>;
  const q = new EntityQuery<{fields: E1}>(context, 'e1');

  const ascView = q.select('id').asc('n').prepare().materialize();
  const descView = q.select('id').desc('n').prepare().materialize();

  context.getSource<E1>('e1').add({
    id: 'a',
    n: 1,
  });
  context.getSource<E1>('e1').add({
    id: 'b',
    n: 1,
  });
  context.getSource<E1>('e1').add({
    id: 'c',
    n: 1,
  });
  expect(ascView.value).toEqual([{id: 'a'}, {id: 'b'}, {id: 'c'}]);
  expect(descView.value).toEqual([{id: 'c'}, {id: 'b'}, {id: 'a'}]);
});

test('can materialize with a LIMIT', () => {
  expect(true).toBe(false);
});
test('materialization pulls historical data from sources with memory', () => {
  expect(true).toBe(false);
});

// TODO: after X
// The type of `after` must match the ordering type.

test('count', () => {
  expect(true).toBe(false);
});

// default ordering
// ordering on fields
// de-dupe via id

test('onDifference', () => {
  expect(true).toBe(false);
});
test('destroy', () => {
  expect(true).toBe(false);
});
