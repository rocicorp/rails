import {expect, test} from 'vitest';
import {z} from 'zod';
import {makeTestContext} from '../context/context.js';
import {Materialite} from '../ivm/materialite.js';
import {EntityQueryImpl, astForTesting as ast} from '../query/entity-query.js';
import {buildPipeline} from './pipeline-builder.js';

const e1 = z.object({
  id: z.string(),
  a: z.number(),
  b: z.number(),
  c: z.string().optional(),
  d: z.boolean(),
});
type E1 = z.infer<typeof e1>;

const context = makeTestContext();
test('A simple select', () => {
  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1');
  const m = new Materialite();
  let s = m.newStatelessSource<E1>();
  let pipeline = buildPipeline(
    () => s.stream,
    ast(q.select('id', 'a', 'b', 'c', 'd')),
  );

  let effectRunCount = 0;
  pipeline.effect(x => {
    expect(x).toEqual(expected[effectRunCount++]);
  });

  const expected = [
    {id: 'a', a: 1, b: 1, c: '', d: true},
    {id: 'b', a: 2, b: 2, d: false},
  ] as const;

  s.add(expected[0]);
  s.add(expected[1]);
  expect(effectRunCount).toBe(2);

  s = m.newStatelessSource();
  pipeline = buildPipeline(() => s.stream, ast(q.select('a', 'd')));
  const expected2 = [
    {a: 1, d: true},
    {a: 2, d: false},
  ];
  effectRunCount = 0;
  pipeline.effect(x => {
    expect(x).toEqual(expected2[effectRunCount++]);
  });

  s.add(expected[0]);
  s.add(expected[1]);
  expect(effectRunCount).toBe(2);
});

test('Count', () => {
  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1');
  const m = new Materialite();
  const s = m.newStatelessSource();
  const pipeline = buildPipeline(() => s.stream, ast(q.count()));

  let effectRunCount = 0;
  pipeline.effect(x => {
    expect(x).toBe(expected[effectRunCount++]);
  });
  const expected = [1, 2, 1, 0];

  s.add({});
  s.add({});
  s.delete({});
  s.delete({});
  expect(effectRunCount).toBe(4);
});

test('Where', () => {
  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1');
  const m = new Materialite();
  const s = m.newStatelessSource();
  const pipeline = buildPipeline(
    () => s.stream,
    ast(q.select('id').where('a', '>', 1).where('b', '<', 2)),
  );

  let effectRunCount = 0;
  pipeline.effect(x => {
    expect(x).toEqual(expected[effectRunCount++]);
  });
  const expected = [{id: 'b'}];

  s.add({id: 'a', a: 1, b: 1n});
  s.add({id: 'b', a: 2, b: 1n});
  s.add({id: 'c', a: 1, b: 2n});
  s.add({id: 'd', a: 2, b: 2n});
  expect(effectRunCount).toBe(1);
});

// order-by and limit are properties of the materialize view
// and not a part of the pipeline.
