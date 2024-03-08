import {expect, test} from 'vitest';
import {makeTestContext} from './context/contextProvider.js';
import {z} from 'zod';
import {EntityQuery} from './EntityQuery.js';

test('basic materialization', () => {
  const context = makeTestContext();

  const e1 = z.object({
    id: z.string(),
    n: z.number(),
    optStr: z.string().optional(),
  });
  type E1 = z.infer<typeof e1>;
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

  expected.push(items[2]);
  context.getSource('e1').add(items[2]);
  expect(callCount).toBe(2);
});

test('sorted materialization', () => {});

test('limited materialization', () => {});

test('desc', () => {});

test('count', () => {});

// default ordering
// ordering on fields
// de-dupe via id
// asc/desc

test('onDifference', () => {});
test('destroy', () => {});
