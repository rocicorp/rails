import {describe, expect, test} from 'vitest';
import {z} from 'zod';
import {Entity} from '../../generate.js';
import {AST} from '../ast/ast.js';
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

describe.only('OR', () => {
  type E = {
    id: string;
    a: number;
    b: number;
  };

  type Case = {
    name: string;
    where: AST['where'];
    values?: E[] | undefined;
    expected: string[];
  };

  const defaultValues = [
    {id: 'a', a: 1, b: 1},
    {id: 'b', a: 2, b: 1},
    {id: 'c', a: 1, b: 2},
    {id: 'd', a: 2, b: 2},
  ] as const;

  const cases: Case[] = [
    {
      name: 'basic (2) OR conditions',
      where: {
        op: 'OR',
        conditions: [
          {op: '=', field: 'a', value: {type: 'literal', value: 1}},
          {op: '=', field: 'b', value: {type: 'literal', value: 2}},
        ],
      },
      expected: ['a', 'c', 'd'],
    },
    {
      name: 'basic (1) OR condition',
      where: {
        op: 'OR',
        conditions: [{op: '=', field: 'a', value: {type: 'literal', value: 1}}],
      },
      expected: ['a', 'c'],
    },
    {
      name: 'basic (3) OR condition',
      where: {
        op: 'OR',
        conditions: [
          {op: '=', field: 'a', value: {type: 'literal', value: 1}},
          {op: '=', field: 'b', value: {type: 'literal', value: 2}},
          {op: '=', field: 'a', value: {type: 'literal', value: 2}},
        ],
      },
      values: [
        {id: 'a', a: 1, b: 1},
        {id: 'b', a: 2, b: 1},
        {id: 'c', a: 1, b: 2},
        {id: 'd', a: 3, b: 3},
      ],
      expected: ['a', 'c', 'b'],
    },
    {
      name: 'two branches with same condition',
      where: {
        op: 'OR',
        conditions: [
          {op: '=', field: 'a', value: {type: 'literal', value: 1}},
          {op: '=', field: 'a', value: {type: 'literal', value: 1}},
        ],
      },
      expected: ['a', 'c'],
    },
    {
      name: 'WHERE (a = 1 AND b = 1) OR (a = 2 AND b = 2)',
      where: {
        op: 'OR',
        conditions: [
          {
            op: 'AND',
            conditions: [
              {op: '=', field: 'a', value: {type: 'literal', value: 1}},
              {op: '=', field: 'b', value: {type: 'literal', value: 1}},
            ],
          },
          {
            op: 'AND',
            conditions: [
              {op: '=', field: 'a', value: {type: 'literal', value: 2}},
              {op: '=', field: 'b', value: {type: 'literal', value: 2}},
            ],
          },
        ],
      },
      expected: ['a', 'd'],
    },
    {
      name: 'WHERE (a = 1 OR b = 1) AND (a = 2 OR b = 2)',
      where: {
        op: 'AND',
        conditions: [
          {
            op: 'OR',
            conditions: [
              {op: '=', field: 'a', value: {type: 'literal', value: 1}},
              {op: '=', field: 'b', value: {type: 'literal', value: 1}},
            ],
          },
          {
            op: 'OR',
            conditions: [
              {op: '=', field: 'a', value: {type: 'literal', value: 2}},
              {op: '=', field: 'b', value: {type: 'literal', value: 2}},
            ],
          },
        ],
      },
      expected: ['b', 'c'],
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      const {values = defaultValues} = c;
      const m = new Materialite();
      const s = m.newStatelessSource();
      const ast: AST = {
        table: 'e1',
        select: ['id'],
        where: c.where,
        orderBy: [['id'], 'asc'],
      };

      const pipeline = buildPipeline(() => s.stream, ast);

      // console.log(pipeline.toString());

      const log: unknown[] = [];

      // why in the world is effect not notified but debug is?!?!?!?!?!
      // pipeline.effect(x => {
      //   console.log('WTF!');
      //   log.push((x as Entity).id);
      // });

      pipeline.debug(c => {
        for (const x of c[1].entries) {
          log.push((x[0] as unknown as Entity).id);
        }
      });

      m.tx(() => {
        s.addAll(values);
      });

      expect(log).toEqual(c.expected);
    });
  }
});

// order-by and limit are properties of the materialize view
// and not a part of the pipeline.
