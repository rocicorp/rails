import type {
  ReadonlyJSONValue,
  ReadTransaction,
  WriteTransaction,
} from 'replicache';
import {z, ZodType} from 'zod';
import type {OptionalLogger} from '@rocicorp/logger';

export type Update<T> = Entity & Partial<T>;

export function parseIfDebug<T>(schema: ZodType<T>, val: ReadonlyJSONValue): T {
  if (globalThis.process?.env?.NODE_ENV !== 'production') {
    return schema.parse(val);
  }
  return val as T;
}

export type GenerateResult<T extends Entity> = {
  put: (tx: WriteTransaction, value: T) => Promise<void>;
  has: (tx: ReadTransaction, id: string) => Promise<boolean>;
  get: (tx: ReadTransaction, id: string) => Promise<T | undefined>;
  mustGet: (tx: ReadTransaction, id: string) => Promise<T>;
  update: (tx: WriteTransaction, value: Update<T>) => Promise<void>;
  del: (tx: WriteTransaction, id: string) => Promise<void>;
  list: (tx: ReadTransaction, options?: ListOptions) => Promise<Array<T>>;
};

export function generate<T extends Entity>(
  prefix: string,
  schema: ZodType<T>,
  logger: OptionalLogger = console,
): GenerateResult<T> {
  return {
    put: (tx: WriteTransaction, value: T) => putImpl(prefix, schema, tx, value),
    has: (tx: ReadTransaction, id: string) => hasImpl(prefix, tx, id),
    get: (tx: ReadTransaction, id: string) => getImpl(prefix, schema, tx, id),
    mustGet: (tx: ReadTransaction, id: string) =>
      mustGetImpl(prefix, schema, tx, id),
    update: (tx: WriteTransaction, update: Update<T>) =>
      updateImpl(prefix, schema, tx, update, logger),
    del: (tx: WriteTransaction, id: string) => deleteImpl(prefix, tx, id),
    list: (tx: ReadTransaction, options?: ListOptions) =>
      listImpl(prefix, schema, tx, options),
  };
}

export const entitySchema = z.object({
  id: z.string(),
});
export type Entity = z.TypeOf<typeof entitySchema>;

function key(prefix: string, id: string) {
  return `${prefix}/${id}`;
}

async function putImpl<T extends Entity>(
  prefix: string,
  schema: ZodType<T>,
  tx: WriteTransaction,
  initial: ReadonlyJSONValue,
) {
  const val = parseIfDebug(schema, initial);
  await tx.put(key(prefix, val.id), val);
}

async function hasImpl(prefix: string, tx: ReadTransaction, id: string) {
  return await tx.has(key(prefix, id));
}

async function getImpl<T extends Entity>(
  prefix: string,
  schema: ZodType<T>,
  tx: ReadTransaction,
  id: string,
) {
  return await getInternal(schema, tx, key(prefix, id));
}

async function mustGetImpl<T extends Entity>(
  prefix: string,
  schema: ZodType<T>,
  tx: ReadTransaction,
  id: string,
) {
  const v = await getInternal(schema, tx, key(prefix, id));
  if (v === undefined) {
    throw new Error(`no such entity ${id}`);
  }
  return v;
}

async function updateImpl<T extends Entity>(
  prefix: string,
  schema: ZodType<T>,
  tx: WriteTransaction,
  update: Update<T>,
  logger: OptionalLogger,
) {
  const {id} = update;
  const k = key(prefix, id);
  const prev = await getInternal(schema, tx, k);
  if (prev === undefined) {
    logger.debug?.(`no such entity ${id}, skipping update`);
    return;
  }
  const next = {...prev, ...update};
  const parsed = parseIfDebug(schema, next);
  await tx.put(k, parsed);
}

async function deleteImpl(prefix: string, tx: WriteTransaction, id: string) {
  await tx.del(key(prefix, id));
}

export type ListOptions = {
  startAtID?: string;
  limit?: number;
};

async function listImpl<T extends Entity>(
  prefix: string,
  schema: ZodType<T>,
  tx: ReadTransaction,
  options?: ListOptions,
) {
  const {startAtID, limit} = options ?? {};
  const result = [];
  for await (const v of tx
    .scan({
      prefix: key(prefix, ''),
      start: {
        key: key(prefix, startAtID ?? ''),
      },
      limit,
    })
    .values()) {
    const parsed = parseIfDebug(schema, v);
    result.push(parsed);
  }
  return result;
}

async function getInternal<T extends Entity>(
  schema: ZodType<T>,
  tx: ReadTransaction,
  key: string,
) {
  const val = await tx.get(key);
  if (val === undefined) {
    return val;
  }
  // TODO: parse only in debug mode
  const parsed = parseIfDebug(schema, val);
  return parsed;
}
