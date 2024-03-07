import {expect, expectTypeOf, test} from 'vitest';
import {z} from 'zod';
import {EntityQuery} from './EntityQuery.js';
import {Misuse} from '../error/Misuse.js';
import {makeTestContext} from './context/contextProvider.js';

const context = makeTestContext();
test('query types', () => {
  const e1 = z.object({
    id: z.string(),
    str: z.string(),
    optStr: z.string().optional(),
  });

  type E1 = z.infer<typeof e1>;

  const q = new EntityQuery<{fields: E1}>(context, 'e1');

  // @ts-expect-error - selecting fields that do not exist in the schema is a type error
  q.select('does-not-exist');

  expectTypeOf(q.select).toBeCallableWith('id');
  expectTypeOf(q.select).toBeCallableWith('str', 'optStr');

  expectTypeOf(
    q.select('id', 'str').prepare().materialize().value,
  ).toMatchTypeOf<readonly {id: string; str: string}[]>();
  expectTypeOf(q.select('id').prepare().materialize().value).toMatchTypeOf<
    readonly {id: string}[]
  >();
  expectTypeOf(q.select('optStr').prepare().materialize().value).toMatchTypeOf<
    readonly {optStr?: string}[]
  >();

  // where/order/limit do not change return type
  expectTypeOf(
    q
      .select('id', 'str')
      .where('id', '<', '123')
      .limit(1)
      .asc('id')
      .prepare()
      .materialize().value,
  ).toMatchTypeOf<readonly {id: string; str: string}[]>();

  expectTypeOf(q.where).toBeCallableWith('id', '=', 'foo');
  expectTypeOf(q.where).toBeCallableWith('str', '<', 'foo');
  expectTypeOf(q.where).toBeCallableWith('optStr', '>', 'foo');

  // @ts-expect-error - comparing on missing fields is an error
  q.where('does-not-exist', '=', 'x');

  // @ts-expect-error - comparing with the wrong data type for the value is an error
  q.where('id', '=', 1);

  expectTypeOf(q.count().prepare().materialize().value).toMatchTypeOf<number>();
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
    expect(newq._ast.select).toEqual([k]);
  });

  // all fields are selectable together
  let newq = q.select(...(Object.keys(dummyObject) as (keyof E1)[]));
  expect(newq._ast.select).toEqual(Object.keys(dummyObject));

  // we can call select many times to build up the selection set
  newq = q;
  Object.keys(dummyObject).forEach(k => {
    newq = newq.select(k as keyof E1);
  });
  expect(newq._ast.select).toEqual(Object.keys(dummyObject));

  // we remove duplicates
  newq = q;
  Object.keys(dummyObject).forEach(k => {
    newq = newq.select(k as keyof E1);
  });
  Object.keys(dummyObject).forEach(k => {
    newq = newq.select(k as keyof E1);
  });
  expect(newq._ast.select).toEqual(Object.keys(dummyObject));
});

test('ast: count', () => {
  // Cannot select fields in addition to a count.
  // A query is one or the other: count query or selection query.
  expect(() =>
    new EntityQuery<{fields: E1}>(context, 'e1').select('id').count(),
  ).toThrow(Misuse);
  expect(() =>
    new EntityQuery<{fields: E1}>(context, 'e1').count().select('id'),
  ).toThrow(Misuse);

  // selection set is the literal `count`, not an array of fields
  const q = new EntityQuery<{fields: E1}>(context, 'e1').count();
  expect(q._ast.select).toEqual('count');
});

test('ast: where', () => {
  let q = new EntityQuery<{fields: E1}>(context, 'e1');

  // where is applied
  q = q.where('id', '=', 'a');

  expect({...q._ast, alias: 0}).toEqual({
    alias: 0,
    table: 'e1',
    where: [
      {
        field: 'id',
        op: '=',
        value: {
          type: 'literal',
          value: 'a',
        },
      },
    ],
  });

  // additional wheres are anded
  q = q.where('a', '>', 0);

  expect({...q._ast, alias: 0}).toEqual({
    alias: 0,
    table: 'e1',
    where: [
      {
        field: 'id',
        op: '=',
        value: {
          type: 'literal',
          value: 'a',
        },
      },
      'AND',
      {
        field: 'a',
        op: '>',
        value: {
          type: 'literal',
          value: 0,
        },
      },
    ],
  });
});

test('ast: limit', () => {
  const q = new EntityQuery<{fields: E1}>(context, 'e1');
  expect({...q.limit(10)._ast, alias: 0}).toEqual({
    alias: 0,
    table: 'e1',
    limit: 10,
  });
});

test('ast: asc/desc', () => {
  // can only order once
  const q = new EntityQuery<{fields: E1}>(context, 'e1');
  expect(() => q.asc('id').desc('id')).toThrow(Misuse);
  expect(() => q.asc('id').asc('id')).toThrow(Misuse);
  expect(() => q.desc('id').desc('id')).toThrow(Misuse);
  expect(() => q.asc('id').desc('a')).toThrow(Misuse);

  // order methods update the ast
  expect({...q.asc('id')._ast, alias: 0}).toEqual({
    alias: 0,
    table: 'e1',
    orderBy: [['id'], 'asc'],
  });
  expect({...q.desc('id')._ast, alias: 0}).toEqual({
    alias: 0,
    table: 'e1',
    orderBy: [['id'], 'desc'],
  });
  expect({...q.asc('id', 'a', 'b', 'c', 'd')._ast, alias: 0}).toEqual({
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
  const inOrderToAST = q._ast;

  q = base;
  for (const call of Object.values(calls).reverse()) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    q = call(q) as any;
  }
  const reverseToAST = q._ast;

  expect({
    ...inOrderToAST,
    alias: 0,
  }).toEqual({
    ...reverseToAST,
    alias: 0,
  });
});
