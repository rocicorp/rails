import type {OptionalLogger} from '@rocicorp/logger';
import type {ReadonlyJSONValue} from './json.js';

export type Update<T> = Entity & Partial<T>;

export type Parse<T> = (val: ReadonlyJSONValue) => T;

export function maybeParse<T>(
  parse: Parse<T> | undefined,
  val: ReadonlyJSONValue,
): T {
  if (parse === undefined) {
    return val as T;
  }
  return parse(val);
}

type ScanOptions = {
  prefix?: string | undefined;
  start?:
    | {
        key?: string | undefined;
      }
    | undefined;
  limit?: number | undefined;
};

type ScanResult = {
  values(): AsyncIterable<ReadonlyJSONValue>;
  keys(): AsyncIterable<string>;
  entries(): AsyncIterable<Readonly<[string, ReadonlyJSONValue]>>;
};

export type ReadTransaction = {
  has(key: string): Promise<boolean>;
  get(key: string): Promise<ReadonlyJSONValue | undefined>;
  scan(options?: ScanOptions): ScanResult;
};

export type WriteTransaction = ReadTransaction & {
  set(key: string, value: ReadonlyJSONValue): Promise<void>;
  del(key: string): Promise<boolean>;
};

export type GenerateResult<T extends Entity> = {
  /** Write `value`, overwriting any previous version of same value. */
  set: (tx: WriteTransaction, value: T) => Promise<void>;
  /**
   * Write `value`, overwriting any previous version of same value.
   * @deprecated Use `set` instead.
   */
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
  list: (tx: ReadTransaction, options?: ListOptions) => Promise<T[]>;
  /** List ids matching criteria. */
  listIDs: (tx: ReadTransaction, options?: ListOptions) => Promise<string[]>;
  /** List [id, value] entries matching criteria. */
  listEntries: (
    tx: ReadTransaction,
    options?: ListOptions,
  ) => Promise<[string, T][]>;
};

export function generate<T extends Entity>(
  prefix: string,
  parse: Parse<T> | undefined = undefined,
  logger: OptionalLogger = console,
): GenerateResult<T> {
  return {
    set: (tx: WriteTransaction, value: T) => setImpl(prefix, parse, tx, value),
    put: (tx: WriteTransaction, value: T) => setImpl(prefix, parse, tx, value),
    init: (tx: WriteTransaction, value: T) =>
      initImpl(prefix, parse, tx, value),
    update: (tx: WriteTransaction, update: Update<T>) =>
      updateImpl(prefix, parse, tx, update, logger),
    delete: (tx: WriteTransaction, id: string) => deleteImpl(prefix, tx, id),
    has: (tx: ReadTransaction, id: string) => hasImpl(prefix, tx, id),
    get: (tx: ReadTransaction, id: string) => getImpl(prefix, parse, tx, id),
    mustGet: (tx: ReadTransaction, id: string) =>
      mustGetImpl(prefix, parse, tx, id),
    list: (tx: ReadTransaction, options?: ListOptions) =>
      listImpl(prefix, parse, tx, options),
    listIDs: (tx: ReadTransaction, options?: ListOptions) =>
      listIDsImpl(prefix, tx, options),
    listEntries: (tx: ReadTransaction, options?: ListOptions) =>
      listEntriesImpl(prefix, parse, tx, options),
  };
}

export type Entity = {
  id: string;
};

function key(prefix: string, id: string) {
  return `${prefix}/${id}`;
}

function id(prefix: string, key: string) {
  return key.substring(prefix.length + 1);
}

async function initImpl<T extends Entity>(
  prefix: string,
  parse: Parse<T> | undefined,
  tx: WriteTransaction,
  initial: ReadonlyJSONValue,
) {
  const val = maybeParse(parse, initial);
  const k = key(prefix, val.id);
  if (await tx.has(k)) {
    return false;
  }
  await tx.set(k, val);
  return true;
}

async function setImpl<T extends Entity>(
  prefix: string,
  parse: Parse<T> | undefined,
  tx: WriteTransaction,
  initial: ReadonlyJSONValue,
) {
  const val = maybeParse(parse, initial);
  await tx.set(key(prefix, val.id), val);
}

function hasImpl(prefix: string, tx: ReadTransaction, id: string) {
  return tx.has(key(prefix, id));
}

function getImpl<T extends Entity>(
  prefix: string,
  parse: Parse<T> | undefined,
  tx: ReadTransaction,
  id: string,
) {
  return getInternal(parse, tx, key(prefix, id));
}

async function mustGetImpl<T extends Entity>(
  prefix: string,
  parse: Parse<T> | undefined,
  tx: ReadTransaction,
  id: string,
) {
  const v = await getInternal(parse, tx, key(prefix, id));
  if (v === undefined) {
    throw new Error(`no such entity ${id}`);
  }
  return v;
}

async function updateImpl<T extends Entity>(
  prefix: string,
  parse: Parse<T> | undefined,
  tx: WriteTransaction,
  update: Update<T>,
  logger: OptionalLogger,
) {
  const {id} = update;
  const k = key(prefix, id);
  const prev = await getInternal(parse, tx, k);
  if (prev === undefined) {
    logger.debug?.(`no such entity ${id}, skipping update`);
    return;
  }
  const next = {...prev, ...update};
  const parsed = maybeParse(parse, next);
  await tx.set(k, parsed);
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
  parse: Parse<T> | undefined,
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
    result.push(maybeParse(parse, v));
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

async function listEntriesImpl<T extends Entity>(
  prefix: string,
  parse: Parse<T> | undefined,
  tx: ReadTransaction,
  options?: ListOptions,
): Promise<[string, T][]> {
  const {startAtID, limit} = options ?? {};
  const result: [string, T][] = [];
  for await (const [k, v] of tx
    .scan({
      prefix: key(prefix, ''),
      start: {
        key: key(prefix, startAtID ?? ''),
      },
      limit,
    })
    .entries()) {
    result.push([id(prefix, k), maybeParse(parse, v)]);
  }
  return result;
}

async function getInternal<T extends Entity>(
  parse: Parse<T> | undefined,
  tx: ReadTransaction,
  key: string,
) {
  const val = await tx.get(key);
  if (val === undefined) {
    return val;
  }
  return maybeParse(parse, val);
}
