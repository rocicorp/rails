import type {OptionalLogger} from '@rocicorp/logger';
import type {ReadonlyJSONObject, ReadonlyJSONValue} from './json.js';

export type Entity = {
  id: string;
};

export type Update<Entity, T> = Entity & Partial<T>;

export type Parse<T> = (val: ReadonlyJSONValue) => T;

export type ParseInternal<T> = (
  tx: ReadTransaction,
  val: ReadonlyJSONValue,
) => T;

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
  readonly clientID: string;
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
  update: (tx: WriteTransaction, value: Update<Entity, T>) => Promise<void>;
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
  const keyFromEntity: KeyFromEntityFunc<Entity> = (_tx, entity) =>
    key(prefix, entity.id);
  const keyFromID: KeyFromLookupIDFunc<string> = id => key(prefix, id);
  const keyToID = (key: string) => id(prefix, key);
  const idFromEntity: IDFromEntityFunc<Entity, string> = (_tx, entity) =>
    entity.id;
  const firstKey = () => prefix;

  const parseInternal: ParseInternal<T> = (_, val) => maybeParse(parse, val);

  return {
    set: (tx: WriteTransaction, value: T) =>
      setImpl(keyFromEntity, parseInternal, tx, value),
    put: (tx: WriteTransaction, value: T) =>
      setImpl(keyFromEntity, parseInternal, tx, value),
    init: (tx: WriteTransaction, value: T) =>
      initImpl(keyFromEntity, parseInternal, tx, value),
    update: (tx: WriteTransaction, update: Update<Entity, T>) =>
      updateImpl(
        keyFromEntity,
        idFromEntity,
        parseInternal,
        tx,
        update,
        logger,
      ),
    delete: (tx: WriteTransaction, id: string) =>
      deleteImpl(keyFromID, noop, tx, id),
    has: (tx: ReadTransaction, id: string) => hasImpl(keyFromID, tx, id),
    get: (tx: ReadTransaction, id: string) =>
      getImpl(keyFromID, parseInternal, tx, id),
    mustGet: (tx: ReadTransaction, id: string) =>
      mustGetImpl(keyFromID, parseInternal, tx, id),
    list: (tx: ReadTransaction, options?: ListOptions) =>
      listImpl(keyFromID, keyToID, firstKey, parse, tx, options),
    listIDs: (tx: ReadTransaction, options?: ListOptions) =>
      listIDsImpl(keyFromID, keyToID, firstKey, tx, options),
    listEntries: (tx: ReadTransaction, options?: ListOptions) =>
      listEntriesImpl(keyFromID, keyToID, firstKey, parse, tx, options),
  };
}

function key(prefix: string, id: string) {
  return `${prefix}/${id}`;
}

function id(prefix: string, key: string) {
  return key.substring(prefix.length + 1);
}

export async function initImpl<
  V extends ReadonlyJSONValue,
  E extends ReadonlyJSONObject,
>(
  keyFunc: KeyFromEntityFunc<E>,
  parse: ParseInternal<E>,
  tx: WriteTransaction,
  initial: V,
) {
  const val = parse(tx, initial);
  const k = keyFunc(tx, val);
  if (await tx.has(k)) {
    return false;
  }
  await tx.set(k, val);
  return true;
}

export type KeyFromEntityFunc<T extends ReadonlyJSONObject> = (
  tx: ReadTransaction,
  id: T,
) => string;

export type IDFromEntityFunc<T extends ReadonlyJSONObject, ID> = (
  tx: ReadTransaction,
  entity: T,
) => ID;

export async function setImpl<
  V extends ReadonlyJSONObject,
  E extends ReadonlyJSONObject,
>(
  keyFromEntity: KeyFromEntityFunc<E>,
  parse: ParseInternal<E>,
  tx: WriteTransaction,
  initial: V,
): Promise<void> {
  const val = parse(tx, initial);
  await tx.set(keyFromEntity(tx, val), val);
}

export function hasImpl<LookupID>(
  keyFromID: KeyFromLookupIDFunc<LookupID>,
  tx: ReadTransaction,
  id: LookupID,
) {
  return tx.has(keyFromID(id));
}

export type KeyFromLookupIDFunc<LookupID> = (id: LookupID) => string;

export type ValidateMutateFunc<LookupID> = (
  tx: {clientID: string},
  id: LookupID,
) => void;

export type KeyToIDFunc<ID> = (key: string) => ID | undefined;

export type FirstKeyFunc = () => string;

export function getImpl<T extends ReadonlyJSONObject, LookupID>(
  keyFromID: KeyFromLookupIDFunc<LookupID>,
  parse: ParseInternal<T>,
  tx: ReadTransaction,
  id: LookupID,
): Promise<T | undefined> {
  return getInternal(parse, tx, keyFromID(id));
}

export async function mustGetImpl<LookupID, T extends ReadonlyJSONObject>(
  keyFromID: KeyFromLookupIDFunc<LookupID>,
  parse: ParseInternal<T>,
  tx: ReadTransaction,
  id: LookupID,
) {
  const v = await getInternal(parse, tx, keyFromID(id));
  if (v === undefined) {
    throw new Error(`no such entity ${JSON.stringify(id)}`);
  }
  return v;
}

export async function updateImpl<
  Entity extends ReadonlyJSONObject,
  T extends Entity,
  ID,
>(
  keyFromEntity: KeyFromEntityFunc<Entity>,
  idFromEntity: IDFromEntityFunc<Entity, ID>,
  parse: ParseInternal<T>,
  tx: WriteTransaction,
  update: Update<Entity, T>,
  logger: OptionalLogger,
) {
  const k = keyFromEntity(tx, update);
  const prev = await getInternal(parse, tx, k);
  if (prev === undefined) {
    const id = idFromEntity(tx, update);
    logger.debug?.(`no such entity ${JSON.stringify(id)}, skipping update`);
    return;
  }
  const next = {...prev, ...update};
  const parsed = parse(tx, next);
  await tx.set(k, parsed);
}

export async function deleteImpl<LookupID>(
  keyFromLookupID: KeyFromLookupIDFunc<LookupID>,
  validateMutate: ValidateMutateFunc<LookupID>,
  tx: WriteTransaction,
  id: LookupID,
) {
  validateMutate(tx, id);
  await tx.del(keyFromLookupID(id));
}

export type ListOptions = {
  startAtID?: string;
  limit?: number;
};

async function* scan<ID, LookupID>(
  keyFromLookupID: KeyFromLookupIDFunc<LookupID>,
  keyToID: KeyToIDFunc<ID>,
  firstKey: FirstKeyFunc,
  tx: ReadTransaction,
  options?: ListOptionsWithLookupID<LookupID>,
): AsyncIterable<Readonly<[ID, ReadonlyJSONValue]>> {
  const {startAtID, limit} = options ?? {};
  const fk = firstKey();
  for await (const [k, v] of tx
    .scan({
      prefix: fk,
      start: {
        key: startAtID === undefined ? fk : keyFromLookupID(startAtID),
      },
      limit,
    })
    .entries()) {
    const id = keyToID(k);
    if (id !== undefined) {
      yield [id, v];
    }
  }
}

export type ListOptionsWithLookupID<ID> = {
  startAtID?: ID;
  limit?: number;
};

export async function listImpl<T extends ReadonlyJSONObject, ID>(
  keyFromID: KeyFromLookupIDFunc<ID>,
  keyToID: KeyToIDFunc<ID>,
  firstKey: FirstKeyFunc,
  parse: Parse<T> | undefined,
  tx: ReadTransaction,
  options?: ListOptionsWithLookupID<ID>,
) {
  const result = [];
  for await (const [, v] of scan(keyFromID, keyToID, firstKey, tx, options)) {
    result.push(maybeParse(parse, v));
  }
  return result;
}

export async function listIDsImpl<ID>(
  keyFromID: KeyFromLookupIDFunc<ID>,
  keyToID: KeyToIDFunc<ID>,
  firstKey: FirstKeyFunc,
  tx: ReadTransaction,
  options?: ListOptionsWithLookupID<ID>,
): Promise<ID[]> {
  const result: ID[] = [];
  for await (const [k] of scan(keyFromID, keyToID, firstKey, tx, options)) {
    result.push(k);
  }
  return result;
}

export async function listEntriesImpl<
  T extends ReadonlyJSONObject,
  LookupID,
  ID,
>(
  keyFromID: KeyFromLookupIDFunc<LookupID>,
  keyToID: KeyToIDFunc<ID>,
  firstKey: FirstKeyFunc,
  parse: Parse<T> | undefined,
  tx: ReadTransaction,
  options?: ListOptionsWithLookupID<LookupID>,
): Promise<[ID, T][]> {
  const result: [ID, T][] = [];
  for await (const [k, v] of scan(keyFromID, keyToID, firstKey, tx, options)) {
    result.push([k, maybeParse(parse, v)]);
  }
  return result;
}

async function getInternal<T extends ReadonlyJSONValue>(
  parse: ParseInternal<T>,
  tx: ReadTransaction,
  key: string,
): Promise<T | undefined> {
  const val = await tx.get(key);
  if (val === undefined) {
    return val;
  }
  return parse(tx, val);
}

function noop(): void {
  // intentionally empty
}
