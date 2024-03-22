/* eslint-disable @typescript-eslint/naming-convention */
import {nanoid} from 'nanoid';
import {MutatorDefs, Replicache, TEST_LICENSE_KEY} from 'replicache';
import {test} from 'vitest';
import {z} from 'zod';
import {generateZQL} from './generate-zql.js';
import {WriteTransaction, generate} from './generate.js';
import {ReadonlyJSONValue} from './json.js';
import {Console} from './types/lib/dom/console.js';

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
  // mustGet: mustGetE1,
  // has: hasE1,
  list: listE1,
  // listIDs: listIDsE1,
  // listEntries: listEntriesE1,
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

const factory = <M extends MutatorDefs>(m: M) =>
  new Replicache({
    licenseKey: TEST_LICENSE_KEY,
    name: nanoid(),
    mutators: m,
  });

// function wrap<V>(o: {on: (v: V) => (() => void)}): Promise<V> {
//   return new Promise<V>(res => {
//     const c = o.on((v: V) => {
//       res(v);
//       c();
//     });
//   });
// }

declare const console: Console;

test('generate', async () => {
  const rep = factory(mutators);
  const zql = generateZQL<E1>(rep, 'e1');

  const selected = zql.select();
  const statement = selected.prepare();
  const view = statement.view();

  view.on(v => {
    console.log('on', v);
  });

  console.log(view.value);

  await rep.mutate.setE1({id: 'a', str: 'a'});

  console.log(view.value);

  // const res = await new Promise(res => {
  //   zql
  //     .select('id')
  //     .prepare()
  //     .materialize()
  //     .on(val => res(val));
  // });
  // console.log(res);
});
