import {afterEach, expect, expectTypeOf, test, vi} from 'vitest';
import {z} from 'zod';
import {makeTestContext} from '../context/context.js';
import {Misuse} from '../error/misuse.js';
import {EntityQueryImpl} from './entity-query.js';

const context = makeTestContext();
test('query types', () => {
  const e1 = z.object({
    id: z.string(),
    str: z.string(),
    optStr: z.string().optional(),
  });

  type E1 = z.infer<typeof e1>;

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
});

const e1 = z.object({
  id: z.string(),
  a: z.number(),
  b: z.number(),
  c: z.string().optional(),
  d: z.boolean(),
});

type E1 = z.infer<typeof e1>;
const dummyObject: E1 = {
  a: 1,
  b: 1,
  c: '',
  d: true,
  id: 'a',
};

test('ast: select', () => {
  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1');

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
    new EntityQueryImpl<{fields: E1}>(context, 'e1').select('id').count(),
  ).toThrow(Misuse);
  expect(() =>
    new EntityQueryImpl<{fields: E1}>(context, 'e1').count().select('id'),
  ).toThrow(Misuse);

  // selection set is the literal `count`, not an array of fields
  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1').count();
  expect(q._ast.select).toEqual('count');
});

test('ast: where', () => {
  let q = new EntityQueryImpl<{fields: E1}>(context, 'e1');

  // where is applied
  q = q.where('id', '=', 'a');

  expect({...q._ast, alias: 0}).toEqual({
    alias: 0,
    table: 'e1',
    orderBy: [['id'], 'asc'],
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
    orderBy: [['id'], 'asc'],
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
  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1');
  expect({...q.limit(10)._ast, alias: 0}).toEqual({
    orderBy: [['id'], 'asc'],
    alias: 0,
    table: 'e1',
    limit: 10,
  });
});

test('ast: asc/desc', () => {
  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1');

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

test('reusing instances', () => {
  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1');
  const s1 = q.select('id');
  const s2 = q.select('id');
  expect(s1).toBe(s2);

  // We do not yet normalize the order of the select fields.
  const s3 = q.select('a', 'b');
  const s4 = q.select('b', 'a');
  expect(s3).not.toBe(s4);

  const s5 = q.select('a', 'b');
  const s6 = q.select('a').select('b');
  expect(s5).toBe(s6);

  const w1 = q.where('id', '=', 'a');
  const w2 = q.where('id', '=', 'a');
  expect(w1).toBe(w2);

  const w3 = q.where('id', '=', 'a');
  const w4 = q.where('id', '=', 'b');
  expect(w3).not.toBe(w4);

  const l1 = q.limit(10);
  const l2 = q.limit(10);
  expect(l1).toBe(l2);

  const l3 = q.limit(10);
  const l4 = q.limit(20);
  expect(l3).not.toBe(l4);

  const a1 = q.asc('id');
  const a2 = q.asc('id');
  expect(a1).toBe(a2);

  const a3 = q.asc('id');
  const a4 = q.desc('id');
  expect(a3).not.toBe(a4);

  const a5 = q.asc('id');
  const a6 = q.asc('a');
  expect(a5).not.toBe(a6);

  const a7 = q.asc('id');
  const a8 = q.asc('id', 'a');
  expect(a7).not.toBe(a8);

  const s7 = q.select('id').where('id', '=', 'a').limit(10).asc('id');
  const s8 = q.select('id').limit(10).asc('id').where('id', '=', 'a');
  expect(s7).toBe(s8);
});

afterEach(() => {
  vi.restoreAllMocks();
});

test('reusing with mocked weakref', () => {
  const weakRefs: MockWeakRef<WeakKey>[] = [];

  class MockWeakRef<T extends WeakKey> implements WeakRef<T> {
    readonly [Symbol.toStringTag] = 'WeakRef';
    value: T | undefined;
    constructor(value: T) {
      weakRefs.push(this);
      this.value = value;
    }

    deref(): T | undefined {
      return this.value;
    }
  }

  vi.stubGlobal('WeakRef', MockWeakRef);

  const q = new EntityQueryImpl<{fields: E1}>(context, 'e1');
  const s1 = q.select('id');
  expect(weakRefs.length).toBe(1);
  expect(weakRefs[0].value).toBe(s1);

  const s2 = q.select('id');
  expect(s1).toBe(s2);
  expect(weakRefs.length).toBe(1);
  expect(weakRefs[0].value).toBe(s2);

  weakRefs[0].value = undefined;

  const s3 = q.select('id');
  expect(s1).not.toBe(s3);
  expect(weakRefs.length).toBe(2);
  expect(weakRefs[1].value).toBe(s3);
});
