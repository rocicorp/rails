import type {OptionalLogger} from '@rocicorp/logger';
import type {
  ReadonlyJSONValue,
  ReadTransaction,
  WriteTransaction,
} from 'replicache';
import {z, ZodType} from 'zod';

export type Update<T> = Entity & Partial<T>;

export function parseIfDebug<T>(schema: ZodType<T>, val: ReadonlyJSONValue): T {
  if (process.env.NODE_ENV === 'production') {
    return val as T;
  }
  // Don't return the clone. It's useful to be able to cache by identity.
  schema.parse(val);
  return val as T;
}

export type GenerateResult<T extends Entity> = {
  /** Write `value`, overwriting any previous version of same value. */
  put: (tx: WriteTransaction, value: T) => Promise<void>;
  /** Write `value` only if no previous version of this value exists. */
  init: (tx: WriteTransaction, value: T) => Promise<boolean>;
  /** Update existing value with new fields. */
  update: (tx: WriteTransaction, value: Update<T>) => Promise<void>;
  /** Delete any existing value or do nothing if none exist. */
  delete: (tx: WriteTransaction, id: string) => Promise<void>;
  /** Return true if specified value exists, false otherwise. */
  has: (tx: ReadTransaction, id: string) => Promise<boolean>;
  /** Get value by ID, or return undefined if none exists. */
  get: (tx: ReadTransaction, id: string) => Promise<T | undefined>;
  /** Get value by ID, or throw if none exists. */
  mustGet: (tx: ReadTransaction, id: string) => Promise<T>;
  /** List values matching criteria. */
  list: (tx: ReadTransaction, options?: ListOptions) => Promise<Array<T>>;
  /** List ids matching criteria. */
  listIDs: (tx: ReadTransaction, options?: ListOptions) => Promise<string[]>;
};

export function generate<T extends Entity>(
  prefix: string,
  schema: ZodType<T>,
  logger: OptionalLogger = console,
): GenerateResult<T> {
  return {
    put: (tx: WriteTransaction, value: T) => putImpl(prefix, schema, tx, value),
    init: (tx: WriteTransaction, value: T) =>
      initImpl(prefix, schema, tx, value),
    update: (tx: WriteTransaction, update: Update<T>) =>
      updateImpl(prefix, schema, tx, update, logger),
    delete: (tx: WriteTransaction, id: string) => deleteImpl(prefix, tx, id),
    has: (tx: ReadTransaction, id: string) => hasImpl(prefix, tx, id),
    get: (tx: ReadTransaction, id: string) => getImpl(prefix, schema, tx, id),
    mustGet: (tx: ReadTransaction, id: string) =>
      mustGetImpl(prefix, schema, tx, id),
    list: (tx: ReadTransaction, options?: ListOptions) =>
      listImpl(prefix, schema, tx, options),
    listIDs: (tx: ReadTransaction, options?: ListOptions) =>
      listIDsImpl(prefix, tx, options),
  };
}

export const entitySchema = z.object({
  id: z.string(),
});
export type Entity = z.TypeOf<typeof entitySchema>;

function key(prefix: string, id: string) {
  return `${prefix}/${id}`;
}

function id(prefix: string, key: string) {
  return key.substring(prefix.length + 1);
}

async function initImpl<T extends Entity>(
  prefix: string,
  schema: ZodType<T>,
  tx: WriteTransaction,
  initial: ReadonlyJSONValue,
) {
  const val = parseIfDebug(schema, initial);
  const k = key(prefix, val.id);
  if (await tx.has(k)) {
    return false;
  }
  await tx.put(k, val);
  return true;
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

function hasImpl(prefix: string, tx: ReadTransaction, id: string) {
  return tx.has(key(prefix, id));
}

function getImpl<T extends Entity>(
  prefix: string,
  schema: ZodType<T>,
  tx: ReadTransaction,
  id: string,
) {
  return getInternal(schema, tx, key(prefix, id));
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
    result.push(parseIfDebug(schema, v));
  }
  return result;
}

async function listIDsImpl(
  prefix: string,
  tx: ReadTransaction,
  options?: ListOptions,
) {
  const {startAtID, limit} = options ?? {};
  const result = [];
  for await (const k of tx
    .scan({
      prefix: key(prefix, ''),
      start: {
        key: key(prefix, startAtID ?? ''),
      },
      limit,
    })
    .keys()) {
    result.push(id(prefix, k));
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
  return parseIfDebug(schema, val);
}
