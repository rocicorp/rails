import type {OptionalLogger} from '@rocicorp/logger';
import type {ReadonlyJSONObject, ReadonlyJSONValue} from './json.js';

/**
 * An entity is something that can be read or written by Rails.
 */
export type Entity = {
  id: string;
};

export type Update<T> = Entity & Partial<T>;

type UpdateWith<Entity, T> = Entity & Partial<T>;

/**
 * A function that can parse a JSON value into a specific type.
 * Parse should throw an error if the value cannot be parsed.
 */
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
/**
 * The subset of the Replicache/Reflect ScanOptions type that Rails depends on.
 */
export type ScanOptions = {
  prefix?: string | undefined;
  start?:
    | {
        key?: string | undefined;
      }
    | undefined;
  limit?: number | undefined;
};

/**
 * The subset of the Replicache/Reflect ScanResult type that Rails depends on.
 */
export type ScanResult = {
  values(): AsyncIterable<ReadonlyJSONValue>;
  keys(): AsyncIterable<string>;
  entries(): AsyncIterable<Readonly<[string, ReadonlyJSONValue]>>;
};

/**
 * The subset of the Replicache/Reflect ReadTransaction type that Rails depends
 * on.
 */
export type ReadTransaction = {
  readonly clientID: string;
  has(key: string): Promise<boolean>;
  get(key: string): Promise<ReadonlyJSONValue | undefined>;
  scan(options?: ScanOptions): ScanResult;
};

/**
 * The subset of the Replicache/Reflect WriteTransaction type that Rails depends
 * on.
 */
export type WriteTransaction = ReadTransaction & {
  set(key: string, value: ReadonlyJSONValue): Promise<void>;
  del(key: string): Promise<boolean>;
};

export type GenerateResult<T extends Entity> = {
  /** Write `value`, overwriting any previous version of same value. */
  set: (
    tx: WriteTransaction,
    value: T,
    options?: CommonOptions | undefined,
  ) => Promise<void>;
  /**
   * Write `value`, overwriting any previous version of same value.
   * @deprecated Use `set` instead.
   */
  put: (
    tx: WriteTransaction,
    value: T,
    options?: CommonOptions | undefined,
  ) => Promise<void>;
  /** Write `value` only if no previous version of this value exists. */
  init: (
    tx: WriteTransaction,
    value: T,
    options?: CommonOptions | undefined,
  ) => Promise<boolean>;
  /** Update existing value with new fields. */
  update: (
    tx: WriteTransaction,
    value: Update<T>,
    options?: CommonOptions | undefined,
  ) => Promise<void>;
  /** Delete any existing value or do nothing if none exist. */
  delete: (
    tx: WriteTransaction,
    id: string,
    options?: CommonOptions | undefined,
  ) => Promise<void>;
  /** Return true if specified value exists, false otherwise. */
  has: (
    tx: ReadTransaction,
    id: string,
    options?: CommonOptions | undefined,
  ) => Promise<boolean>;
  /** Get value by ID, or return undefined if none exists. */
  get: (
    tx: ReadTransaction,
    id: string,
    options?: CommonOptions | undefined,
  ) => Promise<T | undefined>;
  /** Get value by ID, or throw if none exists. */
  mustGet: (
    tx: ReadTransaction,
    id: string,
    options?: CommonOptions | undefined,
  ) => Promise<T>;
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

/**
 * Generates strongly-typed CRUD-style functions for interacting with Reflect
 * from an Entity.
 */
export function generate<T extends Entity>(
  prefix: string,
  parse: Parse<T> | undefined = undefined,
  logger: OptionalLogger = console,
): GenerateResult<T> {
  const keyFromEntity: KeyFromEntityFunc<Entity> = (
    _tx,
    entity,
    category: string | undefined,
  ) => key(prefix, entity.id, category);
  const keyFromID: KeyFromLookupIDFunc<string> = (
    id,
    category: string | undefined,
  ) => key(prefix, id, category);
  const keyToID = (key: string, category: string | undefined) =>
    id(prefix, key, category);
  const idFromEntity: IDFromEntityFunc<Entity, string> = (_tx, entity) =>
    entity.id;
  const firstKey = () => key(prefix, '');
  const parseInternal: ParseInternal<T> = (_, val) => maybeParse(parse, val);
  const set: GenerateResult<T>['set'] = (tx, value, options?: CommonOptions) =>
    setImpl(keyFromEntity, parseInternal, tx, value, options);

  return {
    set,
    put: set,
    init: (tx, value, options) =>
      initImpl(keyFromEntity, parseInternal, tx, value, options),
    update: (tx, update, options) =>
      updateImpl(
        keyFromEntity,
        idFromEntity,
        parseInternal,
        parseInternal,
        tx,
        update,
        logger,
        options,
      ),
    delete: (tx, id, options) => deleteImpl(keyFromID, noop, tx, id, options),
    has: (tx, id, options) => hasImpl(keyFromID, tx, id, options),
    get: (tx, id, options) =>
      getImpl(keyFromID, parseInternal, tx, id, options),
    mustGet: (tx, id, options) =>
      mustGetImpl(keyFromID, parseInternal, tx, id, options),
    list: (tx, options?) =>
      listImpl(keyFromID, keyToID, firstKey, parse, tx, options),
    listIDs: (tx, options?) =>
      listIDsImpl(keyFromID, keyToID, firstKey, tx, options),
    listEntries: (tx, options?) =>
      listEntriesImpl(keyFromID, keyToID, firstKey, parse, tx, options),
  };
}

function key(prefix: string, id: string, category?: string | undefined) {
  return `${category ?? ''}/${prefix}/${id}`;
}

function id(prefix: string, key: string, category: string | undefined) {
  return key.substring((category ?? '').length + 1 + prefix.length + 1);
}

export async function initImpl<
  V extends ReadonlyJSONValue,
  E extends ReadonlyJSONObject,
>(
  keyFunc: KeyFromEntityFunc<E>,
  parse: ParseInternal<E>,
  tx: WriteTransaction,
  initial: V,
  options?: CommonOptions | undefined,
) {
  const val = parse(tx, initial);
  const k = keyFunc(tx, val, options?.category);
  if (await tx.has(k)) {
    return false;
  }
  await tx.set(k, val);
  return true;
}

export type KeyFromEntityFunc<T extends ReadonlyJSONObject> = (
  tx: ReadTransaction,
  id: T,
  category: string | undefined,
) => string;

export type IDFromEntityFunc<T extends ReadonlyJSONObject, ID> = (
  tx: ReadTransaction,
  entity: T,
  category: string | undefined,
) => ID;

export async function setImpl<
  V extends ReadonlyJSONObject,
  E extends ReadonlyJSONObject,
>(
  keyFromEntity: KeyFromEntityFunc<E>,
  parse: ParseInternal<E>,
  tx: WriteTransaction,
  initial: V,
  options?: CommonOptions,
): Promise<void> {
  const val = parse(tx, initial);
  await tx.set(keyFromEntity(tx, val, options?.category), val);
}

export function hasImpl<LookupID>(
  keyFromID: KeyFromLookupIDFunc<LookupID>,
  tx: ReadTransaction,
  id: LookupID,
  options: CommonOptions | undefined,
) {
  return tx.has(keyFromID(id, options?.category));
}

export type KeyFromLookupIDFunc<LookupID> = (
  id: LookupID,
  category: string | undefined,
) => string;

export type ValidateMutateFunc<LookupID> = (
  tx: {clientID: string},
  id: LookupID,
) => void;

export type KeyToIDFunc<ID> = (
  key: string,
  category: string | undefined,
) => ID | undefined;

export type FirstKeyFunc = () => string;

export function getImpl<T extends ReadonlyJSONObject, LookupID>(
  keyFromID: KeyFromLookupIDFunc<LookupID>,
  parse: ParseInternal<T>,
  tx: ReadTransaction,
  id: LookupID,
  options: CommonOptions | undefined,
): Promise<T | undefined> {
  return getInternal(parse, tx, keyFromID(id, options?.category));
}

export async function mustGetImpl<LookupID, T extends ReadonlyJSONObject>(
  keyFromID: KeyFromLookupIDFunc<LookupID>,
  parse: ParseInternal<T>,
  tx: ReadTransaction,
  id: LookupID,
  options: CommonOptions | undefined,
) {
  const v = await getInternal(parse, tx, keyFromID(id, options?.category));
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
  parseExisting: ParseInternal<Entity>,
  parseNew: ParseInternal<Entity>,
  tx: WriteTransaction,
  update: UpdateWith<Entity, T>,
  logger: OptionalLogger,
  options: CommonOptions | undefined,
) {
  const k = keyFromEntity(tx, update, options?.category);
  const prev = await getInternal(parseExisting, tx, k);
  if (prev === undefined) {
    const id = idFromEntity(tx, update, options?.category);
    logger.debug?.(`no such entity ${JSON.stringify(id)}, skipping update`);
    return;
  }
  const next = {...prev, ...update};
  const parsed = parseNew(tx, next);
  await tx.set(k, parsed);
}

export async function deleteImpl<LookupID>(
  keyFromLookupID: KeyFromLookupIDFunc<LookupID>,
  validateMutate: ValidateMutateFunc<LookupID>,
  tx: WriteTransaction,
  id: LookupID,
  options?: CommonOptions | undefined,
) {
  validateMutate(tx, id);
  await tx.del(keyFromLookupID(id, options?.category));
}

export type CommonOptions = {
  category?: string | undefined;
};

export type ListOptions = ListOptionsWith<string>;

export async function* scan<ID, LookupID>(
  keyFromLookupID: KeyFromLookupIDFunc<LookupID>,
  keyToID: KeyToIDFunc<ID>,
  firstKey: FirstKeyFunc,
  tx: ReadTransaction,
  options?: ListOptionsWith<LookupID>,
): AsyncIterable<Readonly<[ID, ReadonlyJSONValue]>> {
  const {startAtID, limit} = options ?? {};
  const fk = firstKey();
  for await (const [k, v] of tx
    .scan({
      prefix: fk,
      start: {
        key:
          startAtID === undefined
            ? fk
            : keyFromLookupID(startAtID, options?.category),
      },
      limit,
    })
    .entries()) {
    const id = keyToID(k, options?.category);
    if (id !== undefined) {
      yield [id, v];
    }
  }
}

export type ListOptionsWith<ID> = CommonOptions & {
  startAtID?: ID;
  limit?: number;
};

export async function listImpl<T extends ReadonlyJSONObject, ID>(
  keyFromID: KeyFromLookupIDFunc<ID>,
  keyToID: KeyToIDFunc<ID>,
  firstKey: FirstKeyFunc,
  parse: Parse<T> | undefined,
  tx: ReadTransaction,
  options?: ListOptionsWith<ID>,
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
  options?: ListOptionsWith<ID>,
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
  options?: ListOptionsWith<LookupID>,
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
