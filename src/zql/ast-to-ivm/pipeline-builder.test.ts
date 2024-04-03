import {compareUTF8} from 'compare-utf8';
import {describe, expect, test} from 'vitest';
import {z} from 'zod';
import {Entity} from '../../generate.js';
import {AST, Condition} from '../ast/ast.js';
import {makeTestContext} from '../context/context.js';
import {DifferenceStream} from '../ivm/graph/difference-stream.js';
import {Materialite} from '../ivm/materialite.js';
import * as agg from '../query/agg.js';
import {EntityQuery, astForTesting as ast} from '../query/entity-query.js';
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
const comparator = (l: E1, r: E1) => compareUTF8(l.id, r.id);
test('A simple select', () => {
  const q = new EntityQuery<{fields: E1}>(context, 'e1');
  const m = new Materialite();
  let s = m.newSetSource<E1>(comparator);
  let pipeline = buildPipeline(
    () => s.stream as unknown as DifferenceStream<Entity>,
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

  s = m.newSetSource(comparator);
  pipeline = buildPipeline(
    () => s.stream as unknown as DifferenceStream<Entity>,
    ast(q.select('a', 'd')),
  );
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
  const q = new EntityQuery<{fields: E1}>(context, 'e1');
  const m = new Materialite();
  const s = m.newSetSource<E1>(comparator);
  const pipeline = buildPipeline(
    () => s.stream as unknown as DifferenceStream<Entity>,
    ast(q.select(agg.count())),
  );

  let effectRunCount = 0;
  pipeline.effect((x, mult) => {
    if (mult > 0) {
      expect(x).toEqual(expected[effectRunCount++]);
    }
  });
  const expected = [1, 2, 1, 0].map(x => ({
    a: 1,
    b: 1,
    count: x,
    d: false,
    id: '1',
  }));

  s.add({id: '1', a: 1, b: 1, d: false});
  s.add({id: '2', a: 1, b: 1, d: false});
  s.delete({id: '1', a: 1, b: 1, d: false});
  s.delete({id: '2', a: 1, b: 1, d: false});
  expect(effectRunCount).toBe(4);
});

test('Where', () => {
  const q = new EntityQuery<{fields: E1}>(context, 'e1');
  const m = new Materialite();
  const s = m.newSetSource<E1>(comparator);
  const pipeline = buildPipeline(
    () => s.stream as unknown as DifferenceStream<Entity>,
    ast(q.select('id').where('a', '>', 1).where('b', '<', 2)),
  );

  let effectRunCount = 0;
  pipeline.effect(x => {
    expect(x).toEqual(expected[effectRunCount++]);
  });
  const expected = [{id: 'b'}];

  s.add({id: 'a', a: 1, b: 1, d: false});
  s.add({id: 'b', a: 2, b: 1, d: false});
  s.add({id: 'c', a: 1, b: 2, d: false});
  s.add({id: 'd', a: 2, b: 2, d: false});
  expect(effectRunCount).toBe(1);
});

// order-by and limit are properties of the materialize view
// and not a part of the pipeline.

function conditionToString(c: Condition, paren = false): string {
  if (c.op === 'AND' || c.op === 'OR') {
    let s = '';
    if (paren) {
      s += '(';
    }
    {
      const paren = c.op === 'AND' && c.conditions.length > 1;
      s += c.conditions.map(c => conditionToString(c, paren)).join(` ${c.op} `);
    }
    if (paren) {
      s += ')';
    }
    return s;
  }
  return `${(c as {field: string}).field} ${c.op} ${(c as {value: {value: unknown}}).value.value}`;
}

describe('OR', () => {
  type E = {
    id: string;
    a: number;
    b: number;
  };

  type DeleteE = {
    delete: E;
  };

  type NoUndefined<T> = T extends undefined ? never : T;

  type Case = {
    name?: string | undefined;
    where: NoUndefined<AST['where']>;
    values?: (E | DeleteE)[] | undefined;
    expected: (E | [v: E, multiplicity: number])[];
  };

  const defaultValues: (E | DeleteE)[] = [
    {id: 'a', a: 1, b: 1},
    {id: 'b', a: 2, b: 1},
    {id: 'c', a: 1, b: 2},
    {id: 'd', a: 2, b: 2},
  ];

  const cases: Case[] = [
    {
      where: {
        op: 'OR',
        conditions: [
          {op: '=', field: 'a', value: {type: 'literal', value: 1}},
          {op: '=', field: 'b', value: {type: 'literal', value: 2}},
        ],
      },
      expected: [
        {id: 'a', a: 1, b: 1},
        {id: 'c', a: 1, b: 2},
        {id: 'd', a: 2, b: 2},
      ],
    },
    {
      where: {
        op: 'OR',
        conditions: [{op: '=', field: 'a', value: {type: 'literal', value: 1}}],
      },
      expected: [
        {id: 'a', a: 1, b: 1},
        {id: 'c', a: 1, b: 2},
      ],
    },
    {
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
      expected: [
        {id: 'a', a: 1, b: 1},
        {id: 'b', a: 2, b: 1},
        {id: 'c', a: 1, b: 2},
      ],
    },
    {
      where: {
        op: 'OR',
        conditions: [
          {op: '=', field: 'a', value: {type: 'literal', value: 1}},
          {op: '=', field: 'a', value: {type: 'literal', value: 1}},
        ],
      },
      expected: [
        {id: 'a', a: 1, b: 1},
        {id: 'c', a: 1, b: 2},
      ],
    },
    {
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
      expected: [
        {id: 'a', a: 1, b: 1},
        {id: 'd', a: 2, b: 2},
      ],
    },

    {
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
      expected: [
        {id: 'b', a: 2, b: 1},
        {id: 'c', a: 1, b: 2},
      ],
    },

    {
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
              {op: '=', field: 'a', value: {type: 'literal', value: 1}},
              {op: '=', field: 'b', value: {type: 'literal', value: 1}},
            ],
          },
        ],
      },
      expected: [
        {id: 'a', a: 1, b: 1},
        {id: 'b', a: 2, b: 1},
        {id: 'c', a: 1, b: 2},
      ],
    },

    {
      name: 'Repeat identical conditions',
      where: {
        op: 'OR',
        conditions: [
          {op: '=', field: 'a', value: {type: 'literal', value: 1}},
          {op: '=', field: 'a', value: {type: 'literal', value: 1}},
          {op: '=', field: 'a', value: {type: 'literal', value: 1}},
        ],
      },
      expected: [
        {id: 'a', a: 1, b: 1},
        {id: 'c', a: 1, b: 2},
      ],
    },

    {
      where: {
        op: 'OR',
        conditions: [
          {op: '=', field: 'a', value: {type: 'literal', value: 3}},
          {op: '=', field: 'a', value: {type: 'literal', value: 4}},
          {op: '=', field: 'a', value: {type: 'literal', value: 5}},
        ],
      },
      expected: [],
    },

    {
      where: {
        op: 'AND',
        conditions: [
          {
            op: 'OR',
            conditions: [
              {op: '=', field: 'a', value: {type: 'literal', value: 1}},
              {op: '=', field: 'a', value: {type: 'literal', value: 2}},
              {op: '=', field: 'a', value: {type: 'literal', value: 3}},
            ],
          },
          {
            op: '=',
            field: 'b',
            value: {type: 'literal', value: 1},
          },
        ],
      },
      values: [
        {id: 'a', a: 1, b: 1},
        {id: 'b', a: 2, b: 2},
        {id: 'c', a: 3, b: 1},
        {id: 'd', a: 4, b: 1},
      ],
      expected: [
        {id: 'a', a: 1, b: 1},
        {id: 'c', a: 3, b: 1},
      ],
    },

    {
      name: 'With delete',
      where: {
        op: 'OR',
        conditions: [
          {op: '=', field: 'a', value: {type: 'literal', value: 1}},
          {op: '=', field: 'a', value: {type: 'literal', value: 2}},
        ],
      },
      values: [
        {id: 'a', a: 1, b: 1},
        // Even though it is really nonsensical to delete this entry since this
        // entry does not exist in the model it should still work.
        {delete: {id: 'a', a: 1, b: 3}},
        {id: 'a', a: 2, b: 2},
        {delete: {id: 'c', a: 3, b: 2}},
      ],
      expected: [
        {id: 'a', a: 1, b: 1},
        [{id: 'a', a: 1, b: 3}, -1],
        {id: 'a', a: 2, b: 2},
      ],
    },
  ];

  const comparator = (l: E, r: E) => compareUTF8(l.id, r.id);

  for (const c of cases) {
    test((c.name ? c.name + ': ' : '') + conditionToString(c.where), () => {
      const {values = defaultValues} = c;
      const m = new Materialite();
      const s = m.newSetSource<E>(comparator);

      const ast: AST = {
        table: 'items',
        select: ['id', 'a', 'b'],
        where: c.where,
        orderBy: [['id'], 'asc'],
      };

      const pipeline = buildPipeline(
        () => s.stream as unknown as DifferenceStream<Entity>,
        ast,
      );

      const log: unknown[] = [];
      pipeline.effect((value, multiplicity) => {
        if (multiplicity === 1) {
          log.push(value);
        } else {
          log.push([value, multiplicity]);
        }
      });

      for (const value of values) {
        if ('delete' in value) {
          s.delete(value.delete);
          continue;
        } else {
          s.add(value);
        }
      }

      expect(log).toEqual(c.expected);
    });
  }
});
