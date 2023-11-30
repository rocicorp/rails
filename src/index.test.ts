/* eslint-disable @typescript-eslint/naming-convention */
import type {OptionalLogger} from '@rocicorp/logger';
import {expect} from 'chai';
import {nanoid} from 'nanoid';
import {
  ReadonlyJSONObject,
  ReadonlyJSONValue,
  Replicache,
  TEST_LICENSE_KEY,
  WriteTransaction,
} from 'replicache';
import {ZodError, ZodTypeAny, z} from 'zod';
import {ListOptions, generate} from './index.js';

const e1 = z.object({
  id: z.string(),
  str: z.string(),
  optStr: z.string().optional(),
});

type E1 = z.infer<typeof e1>;

const {
  init: initE1,
  set: setE1,
  update: updateE1,
  delete: deleteE1,
  get: getE1,
  mustGet: mustGetE1,
  has: hasE1,
  list: listE1,
  listIDs: listIDsE1,
  listEntries: listEntriesE1,
} = generate<E1>('e1', e1.parse);

async function directWrite(
  tx: WriteTransaction,
  {key, val}: {key: string; val: ReadonlyJSONValue},
) {
  await tx.set(key, val);
}

const mutators = {
  initE1,
  setE1,
  getE1,
  updateE1,
  deleteE1,
  listE1,
  directWrite,
};

suite('set', () => {
  type Case = {
    name: string;
    preexisting: boolean;
    input: unknown;
    expectError?: ReadonlyJSONValue | undefined;
  };

  const id = 'id1';

  const cases: Case[] = [
    {
      name: 'null',
      preexisting: false,
      input: null,
      expectError: {_errors: ['Expected object, received null']},
    },
    {
      name: 'undefined',
      preexisting: false,
      input: undefined,
      expectError: {_errors: ['Required']},
    },
    {
      name: 'string',
      preexisting: false,
      input: 'foo',
      expectError: {_errors: ['Expected object, received string']},
    },
    {
      name: 'no-id',
      preexisting: false,
      input: {str: 'foo'},
      expectError: {_errors: [], id: {_errors: ['Required']}},
    },
    {
      name: 'no-str',
      preexisting: false,
      input: {id},
      expectError: {_errors: [], str: {_errors: ['Required']}},
    },
    {
      name: 'valid',
      preexisting: false,
      input: {id, str: 'foo'},
    },
    {
      name: 'with-opt-filed',
      preexisting: false,
      input: {id, str: 'foo', optStr: 'bar'},
    },
    {
      name: 'preexisting',
      preexisting: true,
      input: {id, str: 'foo'},
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const rep = new Replicache({
        name: nanoid(),
        mutators,
        licenseKey: TEST_LICENSE_KEY,
      });

      if (c.preexisting) {
        await rep.mutate.setE1({id, str: 'preexisting'});
      }

      let error = undefined;
      try {
        await rep.mutate.setE1(c.input as E1);
      } catch (e) {
        error = (e as ZodError).format();
      }

      const actual = await rep.query(tx => tx.get(`e1/${id}`));
      if (c.expectError !== undefined) {
        expect(error).deep.eq(c.expectError);
        expect(actual).undefined;
      } else {
        expect(error).undefined;
        expect(actual).deep.eq(c.input);
      }
    });
  }
});

suite('init', () => {
  type Case = {
    name: string;
    preexisting: boolean;
    input: unknown;
    expectError?: ReadonlyJSONValue;
    expectResult?: boolean;
  };

  const id = 'id1';

  const cases: Case[] = [
    {
      name: 'null',
      preexisting: false,
      input: null,
      expectError: {_errors: ['Expected object, received null']},
    },
    {
      name: 'undefined',
      preexisting: false,
      input: undefined,
      expectError: {_errors: ['Required']},
    },
    {
      name: 'string',
      preexisting: false,
      input: 'foo',
      expectError: {_errors: ['Expected object, received string']},
    },
    {
      name: 'no-id',
      preexisting: false,
      input: {str: 'foo'},
      expectError: {_errors: [], id: {_errors: ['Required']}},
    },
    {
      name: 'no-str',
      preexisting: false,
      input: {id},
      expectError: {_errors: [], str: {_errors: ['Required']}},
    },
    {
      name: 'valid',
      preexisting: false,
      input: {id, str: 'foo'},
      expectResult: true,
    },
    {
      name: 'with-opt-filed',
      preexisting: false,
      input: {id, str: 'foo', optStr: 'bar'},
      expectResult: true,
    },
    {
      name: 'preexisting',
      preexisting: true,
      input: {id, str: 'foo'},
      expectResult: false,
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const rep = new Replicache({
        name: nanoid(),
        mutators,
        licenseKey: TEST_LICENSE_KEY,
      });

      const preexisting = {id, str: 'preexisting'};
      if (c.preexisting) {
        await rep.mutate.setE1(preexisting);
      }

      let error = undefined;
      let result = undefined;
      try {
        result = await rep.mutate.initE1(c.input as E1);
      } catch (e) {
        error = (e as ZodError).format();
      }

      const actual = await rep.query(tx => tx.get(`e1/${id}`));
      if (c.expectError !== undefined) {
        expect(error).deep.eq(c.expectError);
        expect(actual).undefined;
        expect(result).undefined;
      } else {
        expect(error).undefined;
        expect(actual).deep.eq(c.preexisting ? preexisting : c.input);
        expect(result).eq(c.expectResult);
      }
    });
  }
});

suite('get', () => {
  type Case = {
    name: string;
    stored: unknown;
    expectError?: ReadonlyJSONValue;
  };

  const id = 'id1';

  const cases: Case[] = [
    {
      name: 'null',
      stored: null,
      expectError: {_errors: ['Expected object, received null']},
    },
    {
      name: 'undefined',
      stored: undefined,
    },
    {
      name: 'string',
      stored: 'foo',
      expectError: {_errors: ['Expected object, received string']},
    },
    {
      name: 'no-id',
      stored: {str: 'foo'},
      expectError: {_errors: [], id: {_errors: ['Required']}},
    },
    {
      name: 'no-str',
      stored: {id},
      expectError: {_errors: [], str: {_errors: ['Required']}},
    },
    {
      name: 'valid',
      stored: {id, str: 'foo'},
    },
    {
      name: 'with-opt-filed',
      stored: {id, str: 'foo', optStr: 'bar'},
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const rep = new Replicache({
        name: nanoid(),
        mutators,
        licenseKey: TEST_LICENSE_KEY,
      });

      if (c.stored !== undefined) {
        await rep.mutate.directWrite({key: `e1/${id}`, val: c.stored as E1});
      }
      const {actual, error} = await rep.query(async tx => {
        try {
          return {actual: await getE1(tx, id)};
        } catch (e) {
          return {error: (e as ZodError).format()};
        }
      });
      expect(error).deep.eq(c.expectError, c.name);
      expect(actual).deep.eq(c.expectError ? undefined : c.stored, c.name);
    });
  }
});

suite('mustGet', () => {
  type Case = {
    name: string;
    stored: unknown;
    expectError?: ReadonlyJSONValue;
  };

  const id = 'id1';

  const cases: Case[] = [
    {
      name: 'null',
      stored: null,
      expectError: {_errors: ['Expected object, received null']},
    },
    {
      name: 'undefined',
      stored: undefined,
      expectError: 'Error: no such entity id1',
    },
    {
      name: 'valid',
      stored: {id, str: 'foo'},
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const rep = new Replicache({
        name: nanoid(),
        mutators,
        licenseKey: TEST_LICENSE_KEY,
      });

      if (c.stored !== undefined) {
        await rep.mutate.directWrite({key: `e1/${id}`, val: c.stored as E1});
      }
      const {actual, error} = await rep.query(async tx => {
        try {
          return {actual: await mustGetE1(tx, id)};
        } catch (e) {
          if (e instanceof ZodError) {
            return {error: (e as ZodError).format()};
          }
          return {error: String(e)};
        }
      });
      expect(error).deep.eq(c.expectError, c.name);
      expect(actual).deep.eq(c.expectError ? undefined : c.stored, c.name);
    });
  }
});

suite('has', () => {
  type Case = {
    name: string;
    stored: unknown;
    expectHas: boolean;
  };

  const id = 'id1';

  const cases: Case[] = [
    {
      name: 'undefined',
      stored: undefined,
      expectHas: false,
    },
    {
      name: 'null',
      stored: null,
      expectHas: true,
    },
    {
      name: 'string',
      stored: 'foo',
      expectHas: true,
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const rep = new Replicache({
        name: nanoid(),
        mutators,
        licenseKey: TEST_LICENSE_KEY,
      });

      if (c.stored !== undefined) {
        await rep.mutate.directWrite({key: `e1/${id}`, val: c.stored as E1});
      }
      const has = await rep.query(tx => hasE1(tx, id));
      expect(has).eq(c.expectHas, c.name);
    });
  }
});

suite('update', () => {
  type Case = {
    name: string;
    prev?: unknown | undefined;
    update: ReadonlyJSONObject;
    expected?: unknown | undefined;
    expectError?: ReadonlyJSONValue | undefined;
  };

  const id = 'id1';

  const cases: Case[] = [
    {
      name: 'prev-invalid',
      prev: null,
      update: {},
      expected: undefined,
      expectError: {_errors: ['Expected object, received null']},
    },
    {
      name: 'not-existing-update-id',
      prev: {id, str: 'foo', optStr: 'bar'},
      update: {id: 'bonk', str: 'bar'},
      expected: {id, str: 'foo', optStr: 'bar'},
      expectError: undefined,
    },
    {
      name: 'invalid-update',
      prev: {id, str: 'foo', optStr: 'bar'},
      update: {id, str: 42},
      expected: {id, str: 'foo', optStr: 'bar'},
      expectError: {
        _errors: [],
        str: {_errors: ['Expected string, received number']},
      },
    },
    {
      name: 'valid-update',
      prev: {id, str: 'foo', optStr: 'bar'},
      update: {id, str: 'baz'},
      expected: {id, str: 'baz', optStr: 'bar'},
      expectError: undefined,
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const rep = new Replicache({
        name: nanoid(),
        mutators,
        licenseKey: TEST_LICENSE_KEY,
      });

      if (c.prev !== undefined) {
        await rep.mutate.directWrite({key: `e1/${id}`, val: c.prev as E1});
      }

      let error = undefined;
      let actual = undefined;
      try {
        await rep.mutate.updateE1(c.update as E1);
        actual = await rep.query(tx => getE1(tx, id));
      } catch (e) {
        if (e instanceof ZodError) {
          error = e.format();
        } else {
          error = e;
        }
      }
      expect(error).deep.eq(c.expectError, c.name);
      expect(actual).deep.eq(c.expectError ? undefined : c.expected, c.name);
    });
  }
});

suite('delete', () => {
  type Case = {
    name: string;
    prevExist: boolean;
  };

  const id = 'id1';

  const cases: Case[] = [
    {
      name: 'prev-exist',
      prevExist: true,
    },
    {
      name: 'prev-not-exist',
      prevExist: false,
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const rep = new Replicache({
        name: nanoid(),
        mutators,
        licenseKey: TEST_LICENSE_KEY,
      });

      if (c.prevExist) {
        await rep.mutate.directWrite({
          key: `e1/${id}`,
          val: {id, str: 'foo', optStr: 'bar'},
        });
      }
      await rep.mutate.directWrite({
        key: `e1/id2`,
        val: {id: 'id2', str: 'hot', optStr: 'dog'},
      });

      await rep.mutate.deleteE1(id);
      const actualE1 = await rep.query(tx => getE1(tx, id));
      const actualE12 = await rep.query(tx => getE1(tx, 'id2'));
      expect(actualE1).undefined;
      expect(actualE12).deep.eq({id: 'id2', str: 'hot', optStr: 'dog'});
    });
  }
});

suite('list', () => {
  type Case = {
    name: string;
    prefix: string;
    schema: ZodTypeAny;
    options?: ListOptions | undefined;
    expected?: ReadonlyJSONValue[] | undefined;
    expectError?: ReadonlyJSONValue | undefined;
  };

  const cases: Case[] = [
    {
      name: 'all',
      prefix: 'e1',
      schema: e1,
      expected: [
        {id: 'bar', str: 'barstr'},
        {id: 'baz', str: 'bazstr'},
        {id: 'foo', str: 'foostr'},
      ],
      expectError: undefined,
    },
    {
      name: 'keystart',
      prefix: 'e1',
      schema: e1,
      options: {
        startAtID: 'f',
      },
      expected: [{id: 'foo', str: 'foostr'}],
      expectError: undefined,
    },
    {
      name: 'keystart+limit',
      prefix: 'e1',
      schema: e1,
      options: {
        startAtID: 'bas',
        limit: 1,
      },
      expected: [{id: 'baz', str: 'bazstr'}],
      expectError: undefined,
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const rep = new Replicache({
        name: nanoid(),
        mutators,
        licenseKey: TEST_LICENSE_KEY,
      });

      await rep.mutate.directWrite({
        key: `e1/foo`,
        val: {id: 'foo', str: 'foostr'},
      });
      await rep.mutate.directWrite({
        key: `e1/bar`,
        val: {id: 'bar', str: 'barstr'},
      });
      await rep.mutate.directWrite({
        key: `e1/baz`,
        val: {id: 'baz', str: 'bazstr'},
      });

      let error = undefined;
      let actual = undefined;
      try {
        actual = await rep.query(tx => listE1(tx, c.options));
      } catch (e) {
        if (e instanceof ZodError) {
          error = e.format();
        } else {
          error = e;
        }
      }
      expect(error).deep.eq(c.expectError, c.name);
      expect(actual).deep.eq(c.expected, c.name);
    });
  }
});

suite('listIDs', () => {
  type Case = {
    name: string;
    prefix: string;
    options?: ListOptions | undefined;
    expected?: string[] | undefined;
    expectError?: ReadonlyJSONValue | undefined;
  };

  const cases: Case[] = [
    {
      name: 'all',
      prefix: 'e1',
      expected: ['bar', 'baz', 'foo'],
      expectError: undefined,
    },
    {
      name: 'keystart',
      prefix: 'e1',
      options: {
        startAtID: 'f',
      },
      expected: ['foo'],
      expectError: undefined,
    },
    {
      name: 'keystart+limit',
      prefix: 'e1',
      options: {
        startAtID: 'bas',
        limit: 1,
      },
      expected: ['baz'],
      expectError: undefined,
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const rep = new Replicache({
        name: nanoid(),
        mutators,
        licenseKey: TEST_LICENSE_KEY,
      });

      await rep.mutate.directWrite({
        key: `e1/foo`,
        val: {id: 'foo', str: 'foostr'},
      });
      await rep.mutate.directWrite({
        key: `e1/bar`,
        val: {id: 'bar', str: 'barstr'},
      });
      await rep.mutate.directWrite({
        key: `e1/baz`,
        val: {id: 'baz', str: 'bazstr'},
      });

      let error = undefined;
      let actual = undefined;
      try {
        actual = await rep.query(tx => listIDsE1(tx, c.options));
      } catch (e) {
        if (e instanceof ZodError) {
          error = e.format();
        } else {
          error = e;
        }
      }
      expect(error).deep.eq(c.expectError, c.name);
      expect(actual).deep.eq(c.expected, c.name);
    });
  }
});

suite('listEntries', () => {
  type Case = {
    name: string;
    prefix: string;
    schema: ZodTypeAny;
    options?: ListOptions | undefined;
    expected?: ReadonlyJSONValue[] | undefined;
    expectError?: ReadonlyJSONValue | undefined;
  };

  const cases: Case[] = [
    {
      name: 'all',
      prefix: 'e1',
      schema: e1,
      expected: [
        ['bar', {id: 'bar', str: 'barstr'}],
        ['baz', {id: 'baz', str: 'bazstr'}],
        ['foo', {id: 'foo', str: 'foostr'}],
      ],
      expectError: undefined,
    },
    {
      name: 'keystart',
      prefix: 'e1',
      schema: e1,
      options: {
        startAtID: 'f',
      },
      expected: [['foo', {id: 'foo', str: 'foostr'}]],
      expectError: undefined,
    },
    {
      name: 'keystart+limit',
      prefix: 'e1',
      schema: e1,
      options: {
        startAtID: 'bas',
        limit: 1,
      },
      expected: [['baz', {id: 'baz', str: 'bazstr'}]],
      expectError: undefined,
    },
  ];

  for (const c of cases) {
    test(c.name, async () => {
      const rep = new Replicache({
        name: nanoid(),
        mutators,
        licenseKey: TEST_LICENSE_KEY,
      });

      await rep.mutate.directWrite({
        key: `e1/foo`,
        val: {id: 'foo', str: 'foostr'},
      });
      await rep.mutate.directWrite({
        key: `e1/bar`,
        val: {id: 'bar', str: 'barstr'},
      });
      await rep.mutate.directWrite({
        key: `e1/baz`,
        val: {id: 'baz', str: 'bazstr'},
      });

      let error = undefined;
      let actual = undefined;
      try {
        actual = await rep.query(tx => listEntriesE1(tx, c.options));
      } catch (e) {
        if (e instanceof ZodError) {
          error = e.format();
        } else {
          error = e;
        }
      }
      expect(error).deep.eq(c.expectError, c.name);
      expect(actual).deep.eq(c.expected, c.name);
    });
  }
});

test('optionalLogger', async () => {
  type Case = {
    name: string;
    logger: OptionalLogger | undefined;
    expected: unknown[] | undefined;
  };

  let output: unknown[] | undefined = undefined;

  const cases: Case[] = [
    {
      name: 'undefined',
      logger: undefined,
      expected: undefined,
    },
    {
      name: 'empty',
      logger: {},
      expected: undefined,
    },
    {
      name: 'console',
      logger: console,
      expected: undefined,
    },
    {
      name: 'custom',
      logger: {
        debug: (...args: unknown[]) => {
          output = args;
        },
      },
      expected: ['no such entity foo, skipping update'],
    },
  ];

  for (const c of cases) {
    const {update: updateE1} = generate('e1', e1.parse, c.logger);
    output = undefined;

    const rep = new Replicache({
      name: nanoid(),
      mutators: {
        updateE1,
      },
      licenseKey: TEST_LICENSE_KEY,
    });

    await rep.mutate.updateE1({id: 'foo', str: 'bar'});
    expect(output, c.name).deep.equal(c.expected);
  }
});

test('undefined parse', async () => {
  globalThis.process = {
    env: {
      NODE_ENV: '',
    },
  } as unknown as NodeJS.Process;

  const generated = generate<E1>('e1');
  const {get, list, listIDs} = generated;

  const r = new Replicache({
    name: nanoid(),
    mutators: generated,
    licenseKey: TEST_LICENSE_KEY,
  });

  let v = await r.query(tx => get(tx, 'valid'));
  expect(v).eq(undefined);

  await r.mutate.set({id: 'valid', str: 'bar'});
  await r.mutate.set({id: 'invalid', bonk: 'baz'} as unknown as E1);

  v = await r.query(tx => get(tx, 'valid'));
  expect(v).deep.eq({id: 'valid', str: 'bar'});
  v = await r.query(tx => get(tx, 'invalid'));
  expect(v).deep.eq({id: 'invalid', bonk: 'baz'});

  const l = await r.query(tx => list(tx));
  expect(l).deep.eq([
    {id: 'invalid', bonk: 'baz'},
    {id: 'valid', str: 'bar'},
  ]);

  const l2 = await r.query(tx => listIDs(tx));
  expect(l2).deep.eq(['invalid', 'valid']);
});
