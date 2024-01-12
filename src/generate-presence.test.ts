/* eslint-disable @typescript-eslint/naming-convention */
import {OptionalLogger} from '@rocicorp/logger';
import {Reflect} from '@rocicorp/reflect/client';
import {expect} from 'chai';
import {nanoid} from 'nanoid';
import {MutatorDefs, Replicache, TEST_LICENSE_KEY} from 'replicache';
import {ZodError, ZodTypeAny, z} from 'zod';
import {
  ListOptionsForPresence,
  PresenceEntity,
  generatePresence,
  normalizeScanOptions,
  parseKeyToID,
} from './generate-presence.js';
import {ListOptionsWithLookupID, WriteTransaction} from './generate.js';
import {ReadonlyJSONObject, ReadonlyJSONValue} from './json.js';

const e1 = z.object({
  clientID: z.string(),
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
} = generatePresence<E1>('e1', e1.parse);

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

const factories = [
  <M extends MutatorDefs>(m: M) =>
    new Replicache({
      licenseKey: TEST_LICENSE_KEY,
      name: nanoid(),
      mutators: m,
    }),
  <M extends MutatorDefs>(m: M) =>
    new Reflect({
      roomID: nanoid(),
      userID: nanoid(),
      mutators: m,
    }),
];

suite('set', () => {
  type Case = {
    name: string;
    id: string;
    preexisting: boolean;
    input: (clientID: string) => unknown;
    written?: (clientID: string) => unknown;
    expectError?: (clientID: string) => unknown;
  };

  const id = 'id1';

  const cases: Case[] = [
    {
      name: 'normalize id',
      id: '',
      preexisting: false,
      input: clientID => ({clientID, str: 'foo'}),
      written: clientID => ({clientID, id: '', str: 'foo'}),
    },
    {
      name: 'normalize clientID',
      id: 'id2',
      preexisting: false,
      input: () => ({id: 'id2', str: 'foo'}),
      written: clientID => ({clientID, id: 'id2', str: 'foo'}),
    },
    {
      name: 'normalize clientID & id',
      id: '',
      preexisting: false,
      input: () => ({str: 'foo'}),
      written: clientID => ({clientID, id: '', str: 'foo'}),
    },
    {
      name: 'null',
      id,
      preexisting: false,
      input: () => null,
      expectError: () => 'TypeError: Expected object, received null',
    },
    {
      name: 'undefined',
      id,
      preexisting: false,
      input: () => undefined,
      expectError: () => 'TypeError: Expected object, received undefined',
    },
    {
      name: 'string',
      id,
      preexisting: false,
      input: () => 'foo',
      expectError: () => 'TypeError: Expected object, received string',
    },
    {
      name: 'no-str',
      id,
      preexisting: false,
      input: clientID => ({clientID, id}),
      expectError: () => ({_errors: [], str: {_errors: ['Required']}}),
    },
    {
      name: 'valid',
      id,
      preexisting: false,
      input: clientID => ({clientID, id, str: 'foo'}),
    },
    {
      name: 'with-opt-filed',
      id,
      preexisting: false,
      input: clientID => ({clientID, id, str: 'foo', optStr: 'bar'}),
    },
    {
      name: 'preexisting',
      id,
      preexisting: true,
      input: clientID => ({clientID, id, str: 'foo'}),
    },
    {
      name: 'setting with wrong clientID',
      id,
      preexisting: false,
      input: () => ({clientID: 'wrong', id, str: 'foo'}),
      expectError: clientID =>
        `Error: Can only mutate own entities. Expected clientID "${clientID}" but received "wrong"`,
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(c.name, async () => {
        const {written = c.input, id} = c;
        const r = f(mutators);
        const clientID = await r.clientID;
        if (c.preexisting) {
          await r.mutate.setE1({clientID, id, str: 'preexisting'});
        }

        let error = undefined;
        try {
          await r.mutate.setE1(c.input(clientID) as E1);
        } catch (e) {
          if (e instanceof ZodError) {
            error = e.format();
          } else {
            error = String(e);
          }
        }

        const key = `-/p/${clientID}/e1/${id}`;

        const actual = await r.query(tx => tx.get(key));

        if (c.expectError !== undefined) {
          expect(error).deep.equal(c.expectError?.(clientID));
          expect(actual).undefined;
        } else {
          expect(error).undefined;
          expect(actual).deep.equal(written(clientID));
        }
      });
    }
  }
});

suite('init', () => {
  type Case = {
    name: string;
    id: string;
    preexisting: boolean;
    input: (clientID: string) => unknown;
    written?: (clientID: string) => unknown;
    expectError?: (clientID: string) => unknown;
    expectResult?: boolean;
  };

  const id = 'id1';

  const cases: Case[] = [
    {
      name: 'null',
      id,
      preexisting: false,
      input: () => null,
      expectError: () => 'TypeError: Expected object, received null',
    },
    {
      name: 'undefined',
      id,
      preexisting: false,
      input: () => undefined,
      expectError: () => 'TypeError: Expected object, received undefined',
    },
    {
      name: 'string',
      id,
      preexisting: false,
      input: () => 'foo',
      expectError: () => 'TypeError: Expected object, received string',
    },
    {
      name: 'no-clientID, no-id',
      id: '',
      preexisting: false,
      input: () => ({str: 'foo'}),
      written: clientID => ({clientID, id: '', str: 'foo'}),
      expectResult: true,
    },
    {
      name: 'no-clientID',
      id,
      preexisting: false,
      input: () => ({id, str: 'foo'}),
      written: clientID => ({clientID, id, str: 'foo'}),
      expectResult: true,
    },

    {
      name: 'no-id',
      id: '',
      preexisting: false,
      input: clientID => ({clientID, str: 'foo'}),
      written: clientID => ({clientID, id: '', str: 'foo'}),
      expectResult: true,
    },
    {
      name: 'no-str',
      id,
      preexisting: false,
      input: clientID => ({clientID, id}),
      expectError: () => ({_errors: [], str: {_errors: ['Required']}}),
    },
    {
      name: 'valid',
      id,
      preexisting: false,
      input: clientID => ({clientID, id, str: 'foo'}),
      expectResult: true,
    },
    {
      name: 'with-opt-filed',
      id,
      preexisting: false,
      input: clientID => ({clientID, id, str: 'foo', optStr: 'bar'}),
      expectResult: true,
    },
    {
      name: 'preexisting',
      id,
      preexisting: true,
      input: clientID => ({clientID, id, str: 'foo'}),
      expectResult: false,
    },
    {
      name: 'setting with wrong clientID',
      id,
      preexisting: false,
      input: () => ({clientID: 'wrong', id, str: 'foo'}),
      expectError: clientID =>
        `Error: Can only mutate own entities. Expected clientID "${clientID}" but received "wrong"`,
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(c.name, async () => {
        const {id, input, written = input} = c;
        const r = f(mutators);
        const clientID = await r.clientID;

        const preexisting = {clientID, id, str: 'preexisting'};
        if (c.preexisting) {
          await r.mutate.setE1(preexisting);
        }

        let error = undefined;
        let result = undefined;
        try {
          result = await r.mutate.initE1(input(clientID) as E1);
        } catch (e) {
          if (e instanceof ZodError) {
            error = e.format();
          } else {
            error = String(e);
          }
        }

        const actual = await r.query(tx => tx.get(`-/p/${clientID}/e1/${id}`));
        if (c.expectError !== undefined) {
          expect(error).deep.equal(c.expectError(clientID));
          expect(actual).undefined;
          expect(result).undefined;
        } else {
          expect(error).undefined;
          expect(actual).deep.equal(
            c.preexisting ? preexisting : written(clientID),
          );
          expect(result).eq(c.expectResult);
        }
      });
    }
  }
});

suite('get', () => {
  type Case = {
    name: string;
    stored: ((clientID: string) => unknown) | undefined;
    id: string;
    lookupID?: (
      clientID: string,
    ) => Partial<{clientID: string; id: string}> | undefined;
    expectError?: ReadonlyJSONObject;
  };

  const id = 'id1';

  const cases: Case[] = [
    {
      name: 'null',
      id,
      stored: () => null,
      expectError: {_errors: ['Expected object, received null']},
    },
    {
      name: 'undefined',
      id,
      stored: undefined,
    },
    {
      name: 'string',
      id,
      stored: () => 'foo',
      expectError: {_errors: ['Expected object, received string']},
    },
    {
      name: 'no-clientID, no-id in stored',
      id,
      stored: () => ({str: 'foo'}),
      expectError: {
        _errors: [],
        clientID: {_errors: ['Required']},
        id: {_errors: ['Required']},
      },
    },
    {
      name: 'no-clientID in stored',
      id,
      stored: () => ({id, str: 'foo'}),
      expectError: {_errors: [], clientID: {_errors: ['Required']}},
    },
    {
      name: 'no-id in stored',
      id,
      stored: clientID => ({clientID, str: 'foo'}),
      expectError: {_errors: [], id: {_errors: ['Required']}},
    },
    {
      name: 'no-clientID, no-id in lookup',
      id: '',
      stored: clientID => ({clientID, id: '', str: 'foo'}),
      lookupID: () => ({}),
    },
    {
      name: 'undefined in lookup',
      id: '',
      stored: clientID => ({clientID, id: '', str: 'foo'}),
      lookupID: () => undefined,
    },
    {
      name: 'no-clientID in lookup',
      id,
      stored: clientID => ({clientID, id, str: 'foo'}),
      lookupID: () => ({id}),
    },
    {
      name: 'no-id in lookup',
      id: '',
      stored: clientID => ({clientID, id: '', str: 'foo'}),
      lookupID: clientID => ({clientID}),
    },
    {
      name: 'no-str',
      id,
      stored: clientID => ({clientID, id}),
      expectError: {_errors: [], str: {_errors: ['Required']}},
    },
    {
      name: 'valid',
      id,
      stored: clientID => ({clientID, id, str: 'foo'}),
    },
    {
      name: 'with-opt-filed',
      id,
      stored: clientID => ({clientID, id, str: 'foo', optStr: 'bar'}),
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(c.name, async () => {
        const r = f(mutators);
        const clientID = await r.clientID;
        const {id, lookupID = (clientID: string) => ({clientID, id})} = c;

        if (c.stored !== undefined) {
          await r.mutate.directWrite({
            key: `-/p/${clientID}/e1/${id}`,
            val: c.stored(clientID) as E1,
          });
        }
        const {actual, error} = await r.query(
          async (
            tx,
          ): Promise<
            | {
                actual: E1 | undefined;
                error?: undefined;
              }
            | {error: {_errors: string[]}; actual?: undefined}
          > => {
            try {
              return {actual: await getE1(tx, lookupID(clientID))};
            } catch (e) {
              return {error: (e as ZodError).format()};
            }
          },
        );
        expect(error).deep.equal(c.expectError, c.name);
        expect(actual).deep.equal(
          c.expectError ? undefined : c.stored?.(clientID),
          c.name,
        );
      });
    }
  }
});

suite('mustGet', () => {
  type Case = {
    name: string;
    id: string;
    stored: ((clientID: string) => unknown) | undefined;
    lookupID?: (
      clientID: string,
    ) => Partial<{clientID: string; id: string}> | undefined;
    expectError?: (clientID: string) => unknown;
  };

  const id = 'id1';

  const cases: Case[] = [
    {
      name: 'null',
      id,
      stored: () => null,
      expectError: () => ({_errors: ['Expected object, received null']}),
    },
    {
      name: 'undefined',
      id,
      stored: undefined,
      expectError: clientID =>
        `Error: no such entity {"clientID":"${clientID}","id":"${id}"}`,
    },
    {
      name: 'valid',
      id,
      stored: clientID => ({clientID, id, str: 'foo'}),
    },
    {
      name: 'no-clientID, no-id in stored',
      id,
      stored: () => ({str: 'foo'}),
      expectError: () => ({
        _errors: [],
        clientID: {_errors: ['Required']},
        id: {_errors: ['Required']},
      }),
    },
    {
      name: 'no-clientID in stored',
      id,
      stored: () => ({id, str: 'foo'}),
      expectError: () => ({_errors: [], clientID: {_errors: ['Required']}}),
    },
    {
      name: 'no-id in stored',
      id,
      stored: clientID => ({clientID, str: 'foo'}),
      expectError: () => ({_errors: [], id: {_errors: ['Required']}}),
    },
    {
      name: 'no-clientID, no-id in lookup',
      id: '',
      stored: clientID => ({clientID, id: '', str: 'foo'}),
      lookupID: () => ({}),
    },
    {
      name: 'undefined in lookup',
      id: '',
      stored: clientID => ({clientID, id: '', str: 'foo'}),
      lookupID: () => undefined,
    },
    {
      name: 'no-clientID in lookup',
      id,
      stored: clientID => ({clientID, id, str: 'foo'}),
      lookupID: () => ({id}),
    },
    {
      name: 'no-id in lookup',
      id: '',
      stored: clientID => ({clientID, id: '', str: 'foo'}),
      lookupID: clientID => ({clientID}),
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(c.name, async () => {
        const r = f(mutators);
        const {id, lookupID = (clientID: string) => ({clientID, id})} = c;
        const clientID = await r.clientID;

        if (c.stored !== undefined) {
          await r.mutate.directWrite({
            key: `-/p/${clientID}/e1/${id}`,
            val: c.stored(clientID) as E1,
          });
        }
        const {actual, error} = await r.query(async tx => {
          try {
            return {actual: await mustGetE1(tx, lookupID(clientID))};
          } catch (e) {
            if (e instanceof ZodError) {
              return {error: (e as ZodError).format()};
            }
            return {error: String(e)};
          }
        });
        expect(error).deep.equal(c.expectError?.(clientID), c.name);
        expect(actual).deep.equal(
          c.expectError ? undefined : c.stored?.(clientID),
          c.name,
        );
      });
    }
  }
});

suite('has', () => {
  type Case = {
    name: string;
    id: string;
    lookupID?: (
      clientID: string,
    ) => Partial<{clientID: string; id: string}> | undefined;
    stored: ((clientID: string) => unknown) | undefined;
    expectHas: boolean;
  };

  const id = 'id1';

  const cases: Case[] = [
    {
      name: 'undefined',
      id,
      stored: undefined,
      expectHas: false,
    },
    {
      name: 'null',
      id,
      stored: () => null,
      expectHas: true,
    },
    {
      name: 'string',
      id,
      stored: () => 'foo',
      expectHas: true,
    },
    {
      name: 'no-clientID, no-id in lookup',
      id: '',
      stored: clientID => ({clientID, id: '', str: 'foo'}),
      lookupID: () => ({}),
      expectHas: true,
    },
    {
      name: 'undefined in lookup',
      id: '',
      stored: clientID => ({clientID, id: '', str: 'foo'}),
      lookupID: () => undefined,
      expectHas: true,
    },
    {
      name: 'no-clientID in lookup',
      id,
      stored: clientID => ({clientID, id, str: 'foo'}),
      lookupID: () => ({id}),
      expectHas: true,
    },
    {
      name: 'no-id in lookup',
      id: '',
      stored: clientID => ({clientID, id: '', str: 'foo'}),
      lookupID: clientID => ({clientID}),
      expectHas: true,
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(c.name, async () => {
        const r = f(mutators);
        const clientID = await r.clientID;
        const {id, lookupID = (clientID: string) => ({clientID, id})} = c;

        if (c.stored !== undefined) {
          await r.mutate.directWrite({
            key: `-/p/${clientID}/e1/${id}`,
            val: c.stored(clientID) as E1,
          });
        }
        const has = await r.query(tx => hasE1(tx, lookupID(clientID)));
        expect(has).eq(c.expectHas, c.name);
      });
    }
  }
});

suite('update', () => {
  type Case = {
    name: string;
    id: string;
    prev?: (clientID: string) => unknown;
    update: (clientID: string) => ReadonlyJSONObject;
    expected?: (clientID: string) => unknown;
    expectError?: (clientID: string) => unknown;
  };

  const id = 'id1';

  const cases: Case[] = [
    {
      name: 'prev-invalid',
      id,
      prev: () => null,
      update: () => ({}),
      expectError: () => ({_errors: ['Expected object, received null']}),
    },
    {
      name: 'not-existing-update-clientID',
      id,
      prev: clientID => ({clientID, id, str: 'foo', optStr: 'bar'}),
      update: clientID => ({clientID, id, str: 'baz'}),
      expected: clientID => ({clientID, id, str: 'baz', optStr: 'bar'}),
    },
    {
      name: "not-existing-update-clientID different id doesn't change old",
      id: 'a',
      prev: clientID => ({clientID, id: 'a', str: 'foo', optStr: 'bar'}),
      update: clientID => ({clientID, id: 'b', str: 'baz'}),
      expected: clientID => ({clientID, id: 'a', str: 'foo', optStr: 'bar'}),
    },
    {
      name: 'not-existing-update-clientID different id sets new',
      id: 'b',
      prev: clientID => ({clientID, id: 'a', str: 'foo', optStr: 'bar'}),
      update: clientID => ({clientID, id: 'b', str: 'baz'}),
      expected: clientID => ({clientID, id: 'b', str: 'baz', optStr: 'bar'}),
    },
    {
      name: 'not-existing-update no clientID, no id',
      id: '',
      prev: clientID => ({clientID, id: '', str: 'foo', optStr: 'bar'}),
      update: () => ({str: 'baz'}),
      expected: clientID => ({clientID, id: '', str: 'baz', optStr: 'bar'}),
    },
    {
      name: 'not-existing-update no clientID',
      id,
      prev: clientID => ({clientID, id, str: 'foo', optStr: 'bar'}),
      update: () => ({id, str: 'baz'}),
      expected: clientID => ({clientID, id, str: 'baz', optStr: 'bar'}),
    },
    {
      name: 'not-existing-update no id',
      id: '',
      prev: clientID => ({clientID, id: '', str: 'foo', optStr: 'bar'}),
      update: clientID => ({clientID, str: 'baz'}),
      expected: clientID => ({clientID, id: '', str: 'baz', optStr: 'bar'}),
    },
    {
      name: 'invalid-update',
      id,
      prev: clientID => ({clientID, id, str: 'foo', optStr: 'bar'}),
      update: clientID => ({clientID, id, str: 42}),
      expected: clientID => ({clientID, id, str: 'foo', optStr: 'bar'}),
      expectError: () => ({
        _errors: [],
        str: {_errors: ['Expected string, received number']},
      }),
    },
    {
      name: 'valid-update',
      id,
      prev: clientID => ({clientID, id, str: 'foo', optStr: 'bar'}),
      update: clientID => ({clientID, id, str: 'baz'}),
      expected: clientID => ({clientID, id, str: 'baz', optStr: 'bar'}),
      expectError: undefined,
    },

    {
      name: 'update with wrong clientID',
      id,
      prev: clientID => ({clientID, id, str: 'foo', optStr: 'bar'}),
      update: () => ({clientID: 'wrong', id, str: 'baz'}),
      expectError: clientID =>
        `Error: Can only mutate own entities. Expected clientID "${clientID}" but received "wrong"`,
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(c.name, async () => {
        const r = f(mutators);
        const clientID = await r.clientID;
        const {id} = c;

        if (c.prev !== undefined) {
          await r.mutate.directWrite({
            key: `-/p/${clientID}/e1/${id}`,
            val: c.prev(clientID) as E1,
          });
        }

        let error = undefined;
        let actual = undefined;
        try {
          await r.mutate.updateE1(c.update(clientID) as E1);
          actual = await r.query(tx => getE1(tx, {clientID, id}));
        } catch (e) {
          if (e instanceof ZodError) {
            error = e.format();
          } else {
            error = String(e);
          }
        }
        expect(error).deep.equal(c.expectError?.(clientID), c.name);
        expect(actual).deep.equal(
          c.expectError ? undefined : c.expected?.(clientID),
          c.name,
        );
      });
    }
  }
});

suite('delete', () => {
  type Case = {
    name: string;
    id: string;
    deleteID?: (
      clientID: string,
    ) => Partial<{clientID: string; id: string}> | undefined;
    lookupID?: (clientID: string) => Partial<{clientID: string; id: string}>;
    stored?: (clientID: string) => unknown;
    expectedValue?: (clientID: string) => unknown;
    expectError?: (clientID: string) => unknown;
  };

  const id = 'id1';

  const cases: Case[] = [
    {
      name: 'prev-exist',
      id,
      stored: clientID => ({clientID, id, str: 'foo', optStr: 'bar'}),
    },
    {
      name: 'prev-exist no-clientID, no-id',
      id: '',
      stored: clientID => ({clientID, id: '', str: 'foo', optStr: 'bar'}),
      deleteID: () => ({}),
    },
    {
      name: 'prev-exist undefined deleteID',
      id: '',
      stored: clientID => ({clientID, id: '', str: 'foo', optStr: 'bar'}),
      deleteID: () => undefined,
    },
    {
      name: 'prev-exist no-clientID',
      id,
      stored: clientID => ({clientID, id, str: 'foo', optStr: 'bar'}),
      deleteID: () => ({id}),
    },
    {
      name: 'prev-exist no-id',
      id: '',
      stored: clientID => ({clientID, id: '', str: 'foo', optStr: 'bar'}),
      deleteID: clientID => ({clientID}),
    },
    {
      name: 'prev-not-exist',
      id,
    },
    {
      name: 'prev-exist different id',
      id,
      stored: clientID => ({clientID, id: 'a', str: 'foo', optStr: 'bar'}),
    },
    {
      name: 'different id',
      id: 'a',
      stored: clientID => ({clientID, id: 'a', str: 'foo', optStr: 'bar'}),
      deleteID: clientID => ({clientID, id: 'b'}),
      lookupID: clientID => ({clientID, id: 'a'}),
      expectedValue: clientID => ({
        clientID,
        id: 'a',
        str: 'foo',
        optStr: 'bar',
      }),
    },

    {
      name: 'deleting with wrong clientID',
      id,
      stored: clientID => ({clientID, id, str: 'foo', optStr: 'bar'}),
      deleteID: () => ({clientID: 'wrong', id}),
      expectError: clientID =>
        `Error: Can only mutate own entities. Expected clientID "${clientID}" but received "wrong"`,
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(c.name, async () => {
        const r = f(mutators);
        const clientID = await r.clientID;
        const {
          id,
          deleteID = (clientID: string) => ({
            clientID,
            id,
          }),
          lookupID = deleteID,
        } = c;

        if (c.stored) {
          await r.mutate.directWrite({
            key: `-/p/${clientID}/e1/${id}`,
            val: c.stored(clientID) as E1,
          });
        }

        let error;
        try {
          await r.mutate.deleteE1(deleteID(clientID));
        } catch (e) {
          error = String(e);
        }

        const actual = await r.query(tx => getE1(tx, lookupID(clientID)));

        expect(actual).deep.equal(c.expectedValue?.(clientID));
        expect(error).deep.equal(c.expectError?.(clientID));
      });
    }
  }
});

suite('list', () => {
  type Case = {
    name: string;
    schema: ZodTypeAny;
    options?: ListOptionsForPresence | undefined;
    expected?: ReadonlyJSONObject[] | undefined;
    expectError?: ReadonlyJSONObject | undefined;
  };

  const cases: Case[] = [
    {
      name: 'all',
      schema: e1,
      expected: [
        {clientID: 'bar', id: '', str: 'bar--str'},
        {clientID: 'bar', id: 'c', str: 'bar-c-str'},
        {clientID: 'baz', id: 'e', str: 'baz-e-str'},
        {clientID: 'baz', id: 'g', str: 'baz-g-str'},
        {clientID: 'foo', id: 'a', str: 'foo-a-str'},
      ],
      expectError: undefined,
    },
    {
      name: 'keystart, clientID',
      schema: e1,
      options: {
        startAtID: {clientID: 'f'},
      },
      expected: [{clientID: 'foo', id: 'a', str: 'foo-a-str'}],
      expectError: undefined,
    },
    {
      name: 'keystart, clientID baz',
      schema: e1,
      options: {
        startAtID: {clientID: 'baz'},
      },
      expected: [
        {clientID: 'baz', id: 'e', str: 'baz-e-str'},
        {clientID: 'baz', id: 'g', str: 'baz-g-str'},
        {clientID: 'foo', id: 'a', str: 'foo-a-str'},
      ],
      expectError: undefined,
    },
    {
      name: 'keystart, clientID and id',
      schema: e1,
      options: {
        startAtID: {clientID: 'baz', id: 'g'},
      },
      expected: [
        {clientID: 'baz', id: 'g', str: 'baz-g-str'},
        {clientID: 'foo', id: 'a', str: 'foo-a-str'},
      ],
      expectError: undefined,
    },
    {
      name: 'keystart+limit',
      schema: e1,
      options: {
        startAtID: {clientID: 'bas'},
        limit: 1,
      },
      expected: [{clientID: 'baz', id: 'e', str: 'baz-e-str'}],
      expectError: undefined,
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(c.name, async () => {
        const r = f(mutators);

        for (const [clientID, id] of [
          ['foo', 'a'],
          ['bar', ''],
          ['bar', 'c'],
          ['baz', 'e'],
          ['baz', 'g'],
        ] as const) {
          await r.mutate.directWrite({
            key: `-/p/${clientID}/e1/${id}`,
            val: {clientID, id, str: `${clientID}-${id}-str`},
          });
        }

        await r.mutate.directWrite({
          key: `-/p/ignore/me`,
          val: 'data that should be ignored',
        });
        await r.mutate.directWrite({
          key: `-/p/foo`,
          val: 'data that should be ignored',
        });

        let error = undefined;
        let actual = undefined;
        try {
          actual = await r.query(tx => listE1(tx, c.options));
        } catch (e) {
          if (e instanceof ZodError) {
            error = e.format();
          } else {
            error = e;
          }
        }
        expect(error).deep.equal(c.expectError, c.name);
        expect(actual).deep.equal(c.expected, c.name);
      });
    }
  }
});

suite('listIDs', () => {
  type Case = {
    name: string;
    prefix: string;
    options?: ListOptionsForPresence | undefined;
    expected?: PresenceEntity[] | undefined;
    expectError?: ReadonlyJSONObject | undefined;
  };

  const cases: Case[] = [
    {
      name: 'all',
      prefix: 'e1',
      expected: [
        {clientID: 'bar', id: 'c'},
        {clientID: 'baz', id: 'e'},
        {clientID: 'baz', id: 'g'},
        {clientID: 'foo', id: 'a'},
      ],
      expectError: undefined,
    },
    {
      name: 'keystart',
      prefix: 'e1',
      options: {
        startAtID: {clientID: 'f', id: ''},
      },
      expected: [{clientID: 'foo', id: 'a'}],
      expectError: undefined,
    },
    {
      name: 'keystart',
      prefix: 'e1',
      options: {
        startAtID: {clientID: 'baz', id: 'e'},
      },
      expected: [
        {clientID: 'baz', id: 'e'},
        {clientID: 'baz', id: 'g'},
        {clientID: 'foo', id: 'a'},
      ],
      expectError: undefined,
    },
    {
      name: 'keystart+limit',
      prefix: 'e1',
      options: {
        startAtID: {clientID: 'bas', id: ''},
        limit: 1,
      },
      expected: [{clientID: 'baz', id: 'e'}],
      expectError: undefined,
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(c.name, async () => {
        const r = f(mutators);

        for (const [clientID, id] of [
          ['foo', 'a'],
          ['bar', 'c'],
          ['baz', 'e'],
          ['baz', 'g'],
        ] as const) {
          await r.mutate.directWrite({
            key: `-/p/${clientID}/e1/${id}`,
            val: {clientID, id, str: `${clientID}-${id}-str`},
          });
        }

        await r.mutate.directWrite({
          key: `-/p/ignore/me`,
          val: 'data that should be ignored',
        });
        await r.mutate.directWrite({
          key: `-/p/foo`,
          val: 'data that should be ignored',
        });

        let error = undefined;
        let actual = undefined;
        try {
          actual = await r.query(tx => listIDsE1(tx, c.options));
        } catch (e) {
          if (e instanceof ZodError) {
            error = e.format();
          } else {
            error = e;
          }
        }
        expect(error).deep.equal(c.expectError, c.name);
        expect(actual).deep.equal(c.expected, c.name);
      });
    }
  }
});

suite('listEntries', () => {
  type Case = {
    name: string;
    prefix: string;
    schema: ZodTypeAny;
    options?: ListOptionsWithLookupID<PresenceEntity> | undefined;
    expected?: ReadonlyJSONObject[][] | undefined;
    expectError?: ReadonlyJSONObject | undefined;
  };

  const cases: Case[] = [
    {
      name: 'all',
      prefix: 'e1',
      schema: e1,
      expected: [
        [
          {clientID: 'bar', id: 'c'},
          {clientID: 'bar', id: 'c', str: 'bar-c-str'},
        ],
        [
          {clientID: 'baz', id: 'e'},
          {clientID: 'baz', id: 'e', str: 'baz-e-str'},
        ],
        [
          {clientID: 'baz', id: 'g'},
          {clientID: 'baz', id: 'g', str: 'baz-g-str'},
        ],
        [
          {clientID: 'foo', id: 'a'},
          {clientID: 'foo', id: 'a', str: 'foo-a-str'},
        ],
      ],
      expectError: undefined,
    },
    {
      name: 'keystart',
      prefix: 'e1',
      schema: e1,
      options: {
        startAtID: {clientID: 'f', id: ''},
      },
      expected: [
        [
          {clientID: 'foo', id: 'a'},
          {clientID: 'foo', id: 'a', str: 'foo-a-str'},
        ],
      ],
      expectError: undefined,
    },
    {
      name: 'keystart+limit',
      prefix: 'e1',
      schema: e1,
      options: {
        startAtID: {clientID: 'bas', id: ''},
        limit: 1,
      },
      expected: [
        [
          {clientID: 'baz', id: 'e'},
          {clientID: 'baz', id: 'e', str: 'baz-e-str'},
        ],
      ],
      expectError: undefined,
    },
  ];

  for (const f of factories) {
    for (const c of cases) {
      test(c.name, async () => {
        const r = f(mutators);

        for (const [clientID, id] of [
          ['foo', 'a'],
          ['bar', 'c'],
          ['baz', 'e'],
          ['baz', 'g'],
        ] as const) {
          await r.mutate.directWrite({
            key: `-/p/${clientID}/e1/${id}`,
            val: {clientID, id, str: `${clientID}-${id}-str`},
          });
        }

        await r.mutate.directWrite({
          key: `-/p/ignore/me`,
          val: 'data that should be ignored',
        });
        await r.mutate.directWrite({
          key: `-/p/foo`,
          val: 'data that should be ignored',
        });

        let error = undefined;
        let actual = undefined;
        try {
          actual = await r.query(tx => listEntriesE1(tx, c.options));
        } catch (e) {
          if (e instanceof ZodError) {
            error = e.format();
          } else {
            error = e;
          }
        }
        expect(error).deep.equal(c.expectError, c.name);
        expect(actual).deep.equal(c.expected, c.name);
      });
    }
  }
});

test('optionalLogger', async () => {
  type Case = {
    name: string;
    logger: OptionalLogger | undefined;
    expected: ((clientID: string) => unknown[]) | undefined;
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
      expected: clientID => [
        `no such entity {"clientID":"${clientID}","id":"a"}, skipping update`,
      ],
    },
  ];

  const name = 'nnnn';

  for (const f of factories) {
    for (const c of cases) {
      const {update: updateE1} = generatePresence(name, e1.parse, c.logger);
      output = undefined;

      const r = f({updateE1});
      const clientID = await r.clientID;

      await r.mutate.updateE1({clientID, id: 'a', str: 'bar'});
      expect(output, c.name).deep.equal(c.expected?.(clientID));
    }
  }
});

test('undefined parse', async () => {
  globalThis.process = {
    env: {
      NODE_ENV: '',
    },
  } as unknown as NodeJS.Process;

  const name = 'nnn';
  const generated = generatePresence<E1>(name);
  const {get, list, listIDs} = generated;

  const r = new Replicache({
    name: nanoid(),
    mutators: generated,
    licenseKey: TEST_LICENSE_KEY,
  });
  const clientID = await r.clientID;

  let v = await r.query(tx => get(tx, {clientID, id: 'id1'}));
  expect(v).eq(undefined);

  await r.mutate.set({clientID, id: 'id1', str: 'bar'});
  await r.mutate.set({
    clientID,
    id: 'id2',
    bonk: 'baz',
  } as unknown as E1);

  v = await r.query(tx => get(tx, {clientID, id: 'id1'}));
  expect(v).deep.equal({clientID, id: 'id1', str: 'bar'});
  v = await r.query(tx => get(tx, {clientID, id: 'id2'}));
  expect(v).deep.equal({clientID, id: 'id2', bonk: 'baz'});

  const l = await r.query(tx => list(tx));
  expect(l).deep.equal([
    {clientID, id: 'id1', str: 'bar'},
    {clientID, id: 'id2', bonk: 'baz'},
  ]);

  const l2 = await r.query(tx => listIDs(tx));
  expect(l2).deep.equal([
    {clientID, id: 'id1'},
    {clientID, id: 'id2'},
  ]);
});

test('parse key', () => {
  const name = 'foo';
  expect(parseKeyToID(name, '-/p/clientID1/foo/id1')).deep.equals({
    clientID: 'clientID1',
    id: 'id1',
  });
  expect(parseKeyToID(name, '-/p/clientID1/bar/id1')).equals(undefined);
  expect(parseKeyToID(name, '-/p/clientID1/foo/id1/')).equals(undefined);
  expect(parseKeyToID(name, '-/p/clientID1/foo/id1/more')).equals(undefined);
  expect(parseKeyToID(name, '-/p/clientID1/foo/')).deep.equals({
    clientID: 'clientID1',
    id: '',
  });
  expect(parseKeyToID(name, '-/p/clientID1/foo')).equals(undefined);
  expect(parseKeyToID(name, '-/p/clientID1/')).equals(undefined);
  expect(parseKeyToID(name, '-/p/clientID1')).equals(undefined);
  expect(parseKeyToID(name, '-/p/')).equals(undefined);
  expect(parseKeyToID(name, '-/p')).equals(undefined);
  expect(parseKeyToID(name, '-/')).equals(undefined);
  expect(parseKeyToID(name, '-')).equals(undefined);
  expect(parseKeyToID(name, '')).equals(undefined);
  expect(parseKeyToID(name, 'baz')).equals(undefined);
});

test('normalizeScanOptions', () => {
  expect(normalizeScanOptions(undefined)).undefined;
  expect(normalizeScanOptions({})).deep.equal({
    startAtID: undefined,
    limit: undefined,
  });
  expect(normalizeScanOptions({limit: 123})).deep.equal({
    startAtID: undefined,
    limit: 123,
  });
  expect(normalizeScanOptions({startAtID: {}})).deep.equal({
    startAtID: {clientID: '', id: ''},
    limit: undefined,
  });
  expect(normalizeScanOptions({startAtID: {id: 'a'}})).deep.equal({
    startAtID: {clientID: '', id: 'a'},
    limit: undefined,
  });
  expect(normalizeScanOptions({startAtID: {clientID: 'b'}})).deep.equal({
    startAtID: {clientID: 'b', id: ''},
    limit: undefined,
  });
});
