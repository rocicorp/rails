import {expect, expectTypeOf, test} from 'vitest';
import {z} from 'zod';
import {AST} from '../ast/ast.js';
import {makeTestContext} from '../context/context.js';
import {Misuse} from '../error/misuse.js';
import {
  EntityQueryImpl,
  and,
  astForTesting,
  expression,
  or,
} from './entity-query.js';

function ast(q: WeakKey): AST {
  const ast = astForTesting(q);
  return {
    ...ast,
    alias: 0,
  };
}

const context = makeTestContext();
test('query types', () => {
  const sym = Symbol('sym');
  type E1 = {
    id: string;
    str: string;
    optStr?: string | undefined;
    [sym]: boolean;
  };

  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1');

  // @ts-expect-error - selecting fields that do not exist in the schema is a type error
  q.select('does-not-exist');

  expectTypeOf(q.select).toBeCallableWith('id');
  expectTypeOf(q.select).toBeCallableWith('str', 'optStr');

  expectTypeOf(q.select('id', 'str').prepare().exec()).toMatchTypeOf<
    Promise<readonly {id: string; str: string}[]>
  >();
  expectTypeOf(q.select('id').prepare().exec()).toMatchTypeOf<
    Promise<readonly {id: string}[]>
  >();
  expectTypeOf(q.select('optStr').prepare().exec()).toMatchTypeOf<
    Promise<readonly {optStr?: string}[]>
  >();

  // where/order/limit do not change return type
  expectTypeOf(
    q
      .select('id', 'str')
      .where('id', '<', '123')
      .limit(1)
      .asc('id')
      .prepare()
      .exec(),
  ).toMatchTypeOf<Promise<readonly {id: string; str: string}[]>>();

  expectTypeOf(q.where).toBeCallableWith('id', '=', 'foo');
  expectTypeOf(q.where).toBeCallableWith('str', '<', 'foo');
  expectTypeOf(q.where).toBeCallableWith('optStr', '>', 'foo');

  // @ts-expect-error - comparing on missing fields is an error
  q.where('does-not-exist', '=', 'x');

  // @ts-expect-error - comparing with the wrong data type for the value is an error
  q.where('id', '=', 1);

  expectTypeOf(q.count().prepare().exec()).toMatchTypeOf<Promise<number>>();

  // @ts-expect-error - Argument of type 'unique symbol' is not assignable to parameter of type '"id" | "str" | "optStr"'.ts(2345)
  q.select(sym);

  // @ts-expect-error - Argument of type 'unique symbol' is not assignable to parameter of type 'FieldName<{ fields: E1; }>'.ts(2345)
  q.where(sym, '==', true);
});

const e1 = z.object({
  id: z.string(),
  a: z.number(),
  b: z.bigint(),
  c: z.string().optional(),
  d: z.boolean(),
});

type E1 = z.infer<typeof e1>;
const dummyObject: E1 = {
  id: 'a',
  a: 1,
  b: 1n,
  c: '',
  d: true,
};

test('ast: select', () => {
  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1');

  // each individual field is selectable on its own
  Object.keys(dummyObject).forEach(k => {
    const newq = q.select(k as keyof E1);
    expect(ast(newq).select).toEqual([k]);
  });

  // all fields are selectable together
  let newq = q.select(...(Object.keys(dummyObject) as (keyof E1)[]));
  expect(ast(newq).select).toEqual(Object.keys(dummyObject));

  // we can call select many times to build up the selection set
  newq = q;
  Object.keys(dummyObject).forEach(k => {
    newq = newq.select(k as keyof E1);
  });
  expect(ast(newq).select).toEqual(Object.keys(dummyObject));

  // we remove duplicates
  newq = q;
  Object.keys(dummyObject).forEach(k => {
    newq = newq.select(k as keyof E1);
  });
  Object.keys(dummyObject).forEach(k => {
    newq = newq.select(k as keyof E1);
  });
  expect(ast(newq).select).toEqual(Object.keys(dummyObject));
});

test('ast: count', () => {
  // Cannot select fields in addition to a count.
  // A query is one or the other: count query or selection query.
  expect(() =>
    new EntityQueryImpl<{fields: E1}>(context, 'e1').select('id').count(),
  ).toThrow(Misuse);
  expect(() =>
    new EntityQueryImpl<{fields: E1}>(context, 'e1').count().select('id'),
  ).toThrow(Misuse);

  // selection set is the literal `count`, not an array of fields
  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1').count();
  expect(ast(q).select).toEqual('count');
});

test('ast: where', () => {
  let q = new EntityQueryImpl<{fields: E1}>(context, 'e1');

  // where is applied
  q = q.where('id', '=', 'a');

  expect(ast(q)).toEqual({
    alias: 0,
    table: 'e1',
    orderBy: [['id'], 'asc'],
    where: {
      field: 'id',
      op: '=',
      value: {
        type: 'literal',
        value: 'a',
      },
    },
  });

  // additional wheres are anded
  q = q.where('a', '>', 0);

  expect(ast(q)).toEqual({
    alias: 0,
    table: 'e1',
    orderBy: [['id'], 'asc'],
    where: {
      op: 'AND',
      conditions: [
        {
          field: 'id',
          op: '=',
          value: {
            type: 'literal',
            value: 'a',
          },
        },
        {
          field: 'a',
          op: '>',
          value: {
            type: 'literal',
            value: 0,
          },
        },
      ],
    },
  });

  q = q.where('c', '=', 'foo');
  // multiple ANDs are flattened
  expect(ast(q)).toEqual({
    alias: 0,
    table: 'e1',
    orderBy: [['id'], 'asc'],
    where: {
      op: 'AND',
      conditions: [
        {
          field: 'id',
          op: '=',
          value: {
            type: 'literal',
            value: 'a',
          },
        },
        {
          field: 'a',
          op: '>',
          value: {
            type: 'literal',
            value: 0,
          },
        },
        {
          field: 'c',
          op: '=',
          value: {
            type: 'literal',
            value: 'foo',
          },
        },
      ],
    },
  });
});

test('ast: limit', () => {
  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1');
  expect(ast(q.limit(10))).toEqual({
    orderBy: [['id'], 'asc'],
    alias: 0,
    table: 'e1',
    limit: 10,
  });
});

test('ast: asc/desc', () => {
  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1');

  // order methods update the ast
  expect(ast(q.asc('id'))).toEqual({
    alias: 0,
    table: 'e1',
    orderBy: [['id'], 'asc'],
  });
  expect(ast(q.desc('id'))).toEqual({
    alias: 0,
    table: 'e1',
    orderBy: [['id'], 'desc'],
  });
  expect(ast(q.asc('id', 'a', 'b', 'c', 'd'))).toEqual({
    alias: 0,
    table: 'e1',
    orderBy: [['id', 'a', 'b', 'c', 'd'], 'asc'],
  });
});

test('ast: independent of method call order', () => {
  const base = new EntityQueryImpl<{fields: E1}>(context, 'e1');

  const calls = {
    select(q: typeof base) {
      return q.select('b');
    },
    where(q: typeof base) {
      return q.where('c', 'LIKE', 'foo');
    },
    limit(q: typeof base) {
      return q.limit(10);
    },
    asc(q: typeof base) {
      return q.asc('a');
    },
  };

  let q = base;
  for (const call of Object.values(calls)) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    q = call(q) as any;
  }
  const inOrderToAST = ast(q);

  q = base;
  for (const call of Object.values(calls).reverse()) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    q = call(q) as any;
  }
  const reverseToAST = ast(q);

  expect(inOrderToAST).toEqual({
    ...reverseToAST,
    alias: 0,
  });
});

test('ast: or', () => {
  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1');

  expect(
    ast(q.where(or(expression('a', '=', 123), expression('c', '=', 'abc')))),
  ).toEqual({
    alias: 0,
    table: 'e1',
    orderBy: [['id'], 'asc'],
    where: {
      op: 'OR',
      conditions: [
        {op: '=', field: 'a', value: {type: 'literal', value: 123}},
        {op: '=', field: 'c', value: {type: 'literal', value: 'abc'}},
      ],
    },
  });

  expect(
    ast(
      q.where(
        and(
          expression('a', '=', 1),
          or(expression('d', '=', true), expression('c', '=', 'hello')),
        ),
      ),
    ),
  ).toEqual({
    alias: 0,
    table: 'e1',
    orderBy: [['id'], 'asc'],
    where: {
      op: 'AND',
      conditions: [
        {op: '=', field: 'a', value: {type: 'literal', value: 1}},
        {
          op: 'OR',
          conditions: [
            {op: '=', field: 'd', value: {type: 'literal', value: true}},
            {op: '=', field: 'c', value: {type: 'literal', value: 'hello'}},
          ],
        },
      ],
    },
  });
});

test('ast flatten ands', () => {
  type S = {fields: {id: string; a: number; b: string; c: boolean; d: string}};

  expect(
    and<S>(
      expression('a', '=', 1),
      expression('b', '=', '2'),
      and<S>(expression('c', '=', true), expression('d', '=', '3')),
    ),
  ).toEqual(
    and<S>(
      expression('a', '=', 1),
      expression('b', '=', '2'),
      expression('c', '=', true),
      expression('d', '=', '3'),
    ),
  );

  expect(
    and<S>(
      expression('a', '=', 1),
      and<S>(expression('c', '=', true), expression('d', '=', '3')),
      expression('b', '=', '2'),
    ),
  ).toEqual(
    and<S>(
      expression('a', '=', 1),
      expression('c', '=', true),
      expression('d', '=', '3'),
      expression('b', '=', '2'),
    ),
  );

  expect(
    and<S>(
      and<S>(expression('c', '=', true), expression('d', '=', '3')),
      expression('a', '=', 1),
      expression('b', '=', '2'),
    ),
  ).toEqual(
    and<S>(
      expression('c', '=', true),
      expression('d', '=', '3'),
      expression('a', '=', 1),
      expression('b', '=', '2'),
    ),
  );

  expect(
    and<S>(
      and<S>(expression('c', '=', true), expression('d', '=', '3')),
      and<S>(expression('a', '=', 1), expression('b', '=', '2')),
    ),
  ).toEqual(
    and<S>(
      expression('c', '=', true),
      expression('d', '=', '3'),
      expression('a', '=', 1),
      expression('b', '=', '2'),
    ),
  );
});

test('ast flatten ors', () => {
  type S = {fields: {id: string; a: number; b: string; c: boolean; d: string}};

  expect(
    or<S>(
      expression('a', '=', 1),
      or<S>(expression('c', '=', true), expression('d', '=', '3')),
      expression('b', '=', '2'),
    ),
  ).toEqual(
    or<S>(
      expression('a', '=', 1),
      expression('c', '=', true),
      expression('d', '=', '3'),
      expression('b', '=', '2'),
    ),
  );
});

test('ast consecutive ands should be merged', () => {
  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1');

  expect(
    ast(
      q
        .where(and(expression('a', '=', 1), expression('a', '=', 2)))
        .where(and(expression('c', '=', 'a'), expression('c', '=', 'b'))),
    ).where,
  ).toEqual({
    op: 'AND',
    conditions: [
      {
        field: 'a',
        op: '=',
        value: {
          type: 'literal',
          value: 1,
        },
      },
      {
        field: 'a',
        op: '=',
        value: {
          type: 'literal',
          value: 2,
        },
      },
      {
        field: 'c',
        op: '=',
        value: {
          type: 'literal',
          value: 'a',
        },
      },
      {
        field: 'c',
        op: '=',
        value: {
          type: 'literal',
          value: 'b',
        },
      },
    ],
  });

  expect(
    ast(
      q
        .where(expression('a', '=', 123))
        .where(expression('c', '=', 'abc'))
        .where(expression('d', '=', true)),
    ).where,
  ).toEqual({
    op: 'AND',
    conditions: [
      {
        field: 'a',
        op: '=',
        value: {
          type: 'literal',
          value: 123,
        },
      },
      {
        field: 'c',
        op: '=',
        value: {
          type: 'literal',
          value: 'abc',
        },
      },
      {
        field: 'd',
        op: '=',
        value: {
          type: 'literal',
          value: true,
        },
      },
    ],
  });

  expect(
    ast(
      q
        .where(expression('a', '=', 123))
        .where(or(expression('c', '=', 'abc'), expression('c', '=', 'def')))
        .where(expression('d', '=', true)),
    ).where,
  ).toEqual({
    op: 'AND',
    conditions: [
      {
        field: 'a',
        op: '=',
        value: {
          type: 'literal',
          value: 123,
        },
      },
      {
        op: 'OR',
        conditions: [
          {
            field: 'c',
            op: '=',
            value: {
              type: 'literal',
              value: 'abc',
            },
          },
          {
            field: 'c',
            op: '=',
            value: {
              type: 'literal',
              value: 'def',
            },
          },
        ],
      },
      {
        field: 'd',
        op: '=',
        value: {
          type: 'literal',
          value: true,
        },
      },
    ],
  });
});

test('ast consecutive ors', () => {
  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1');

  expect(
    ast(q.where(or(expression('a', '=', 123), expression('a', '=', 456))))
      .where,
  ).toEqual({
    op: 'OR',
    conditions: [
      {
        field: 'a',
        op: '=',
        value: {
          type: 'literal',
          value: 123,
        },
      },
      {
        field: 'a',
        op: '=',
        value: {
          type: 'literal',
          value: 456,
        },
      },
    ],
  });

  expect(
    ast(
      q
        .where(or(expression('a', '=', 123), expression('a', '=', 456)))
        .where(or(expression('c', '=', 'abc'), expression('c', '=', 'def'))),
    ).where,
  ).toEqual({
    op: 'AND',
    conditions: [
      {
        op: 'OR',
        conditions: [
          {
            field: 'a',
            op: '=',
            value: {
              type: 'literal',
              value: 123,
            },
          },
          {
            field: 'a',
            op: '=',
            value: {
              type: 'literal',
              value: 456,
            },
          },
        ],
      },
      {
        op: 'OR',
        conditions: [
          {
            field: 'c',
            op: '=',
            value: {
              type: 'literal',
              value: 'abc',
            },
          },
          {
            field: 'c',
            op: '=',
            value: {
              type: 'literal',
              value: 'def',
            },
          },
        ],
      },
    ],
  });
});
