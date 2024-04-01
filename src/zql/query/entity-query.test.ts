import {expect, expectTypeOf, test} from 'vitest';
import {z} from 'zod';
import {makeTestContext} from '../context/context.js';
import {Misuse} from '../error/misuse.js';
import {EntityQuery, astForTesting as ast} from './entity-query.js';
import * as agg from './agg.js';

const context = makeTestContext();
test('query types', () => {
  const sym = Symbol('sym');
  type E1 = {
    id: string;
    str: string;
    optStr?: string | undefined;
    [sym]: boolean;
  };

  const q = new EntityQuery<{fields: E1}>(context, 'e1');

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
  expectTypeOf(q.where).toBeCallableWith('id', '=', 'foo');
  expectTypeOf(q.where).toBeCallableWith('str', '<', 'foo');
  expectTypeOf(q.where).toBeCallableWith('optStr', '>', 'foo');

  // @ts-expect-error - comparing on missing fields is an error
  q.where('does-not-exist', '=', 'x');

  // @ts-expect-error - comparing with the wrong data type for the value is an error
  q.where('id', '=', 1);

  expectTypeOf(q.select(agg.count()).prepare().exec()).toMatchTypeOf<
    Promise<number>
  >();

  // @ts-expect-error - Argument of type 'unique symbol' is not assignable to parameter of type '"id" | "str" | "optStr"'.ts(2345)
  q.select(sym);

  // @ts-expect-error - Argument of type 'unique symbol' is not assignable to parameter of type 'FieldName<{ fields: E1; }>'.ts(2345)
  q.where(sym, '==', true);

  // @ts-expect-error - 'x' is not a field that we can aggregate on
  q.select(agg.array('x')).groupBy('id');

  expectTypeOf(
    q.select('id', agg.array('str')).groupBy('optStr').prepare().exec(),
  ).toMatchTypeOf<Promise<readonly {id: string; str: readonly string[]}[]>>();

  expectTypeOf(
    q
      .select('id', agg.array('str', 'alias'))
      .groupBy('optStr')
      .prepare()
      .exec(),
  ).toMatchTypeOf<Promise<readonly {id: string; alias: readonly string[]}[]>>();
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
  const q = new EntityQuery<{fields: E1}>(context, 'e1');

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

test('ast: where', () => {
  let q = new EntityQuery<{fields: E1}>(context, 'e1');

  // where is applied
  q = q.where('id', '=', 'a');

  expect({...ast(q), alias: 0}).toEqual({
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

  expect({...ast(q), alias: 0}).toEqual({
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
  expect({...ast(q), alias: 0}).toEqual({
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
  const q = new EntityQuery<{fields: E1}>(context, 'e1');
  expect({...ast(q.limit(10)), alias: 0}).toEqual({
    orderBy: [['id'], 'asc'],
    alias: 0,
    table: 'e1',
    limit: 10,
  });
});

test('ast: asc/desc', () => {
  const q = new EntityQuery<{fields: E1}>(context, 'e1');

  // order methods update the ast
  expect({...ast(q.asc('id')), alias: 0}).toEqual({
    alias: 0,
    table: 'e1',
    orderBy: [['id'], 'asc'],
  });
  expect({...ast(q.desc('id')), alias: 0}).toEqual({
    alias: 0,
    table: 'e1',
    orderBy: [['id'], 'desc'],
  });
  expect({...ast(q.asc('id', 'a', 'b', 'c', 'd')), alias: 0}).toEqual({
    alias: 0,
    table: 'e1',
    orderBy: [['id', 'a', 'b', 'c', 'd'], 'asc'],
  });
});

test('ast: independent of method call order', () => {
  const base = new EntityQuery<{fields: E1}>(context, 'e1');

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

  expect({
    ...inOrderToAST,
    alias: 0,
  }).toEqual({
    ...reverseToAST,
    alias: 0,
  });
});
