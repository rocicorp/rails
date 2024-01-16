import {OptionalLogger} from '@rocicorp/logger';
import {
  IDFromEntityFunc,
  KeyFromEntityFunc,
  KeyFromLookupIDFunc,
  KeyToIDFunc,
  ListOptionsWithLookupID,
  Parse,
  ParseInternal,
  ReadTransaction,
  Update,
  WriteTransaction,
  deleteImpl,
  getImpl,
  hasImpl,
  initImpl,
  listEntriesImpl,
  listIDsImpl,
  listImpl,
  maybeParse,
  mustGetImpl,
  setImpl,
  updateImpl,
} from './generate.js';

export type PresenceEntity = {
  clientID: string;
  id: string;
};

/**
 * Like {@link PresenceEntity}, but with the fields optional. This is used when
 * doing get, has and delete operations where the clientID and id fields are
 * optional.
 */
export type PresenceID = Partial<PresenceEntity>;

/**
 * When mutating an entity, you can omit the `clientID` and `id` fields. This
 * type marks those fields as optional.
 */
export type OptionalIDs<T extends PresenceEntity> = PresenceID &
  Omit<T, keyof PresenceEntity>;

export type ListOptionsForPresence = ListOptionsWithLookupID<PresenceID>;

export type GeneratePresenceResult<T extends PresenceEntity> = {
  /** Write `value`, overwriting any previous version of same value. */
  set: (tx: WriteTransaction, value: OptionalIDs<T>) => Promise<void>;
  /**
   * Write `value`, overwriting any previous version of same value.
   * @deprecated Use `set` instead.
   */
  put: (tx: WriteTransaction, value: OptionalIDs<T>) => Promise<void>;
  /** Write `value` only if no previous version of this value exists. */
  init: (tx: WriteTransaction, value: OptionalIDs<T>) => Promise<boolean>;
  /** Update existing value with new fields. */
  update: (tx: WriteTransaction, value: Update<PresenceID, T>) => Promise<void>;
  /** Delete any existing value or do nothing if none exist. */
  delete: (tx: WriteTransaction, id?: PresenceID) => Promise<void>;
  /** Return true if specified value exists, false otherwise. */
  has: (tx: ReadTransaction, id?: PresenceID) => Promise<boolean>;
  /** Get value by ID, or return undefined if none exists. */
  get: (tx: ReadTransaction, id?: PresenceID) => Promise<T | undefined>;
  /** Get value by ID, or throw if none exists. */
  mustGet: (tx: ReadTransaction, id?: PresenceID) => Promise<T>;
  /** List values matching criteria. */
  list: (tx: ReadTransaction, options?: ListOptionsForPresence) => Promise<T[]>;
  /** List ids matching criteria. */
  listIDs: (
    tx: ReadTransaction,
    options?: ListOptionsForPresence,
  ) => Promise<PresenceEntity[]>;
  /** List [id, value] entries matching criteria. */
  listEntries: (
    tx: ReadTransaction,
    options?: ListOptionsForPresence,
  ) => Promise<[PresenceEntity, T][]>;
};

const presencePrefix = '-/p/';

function keyFromID(name: string, entity: PresenceEntity) {
  const {clientID, id} = entity;
  return `${presencePrefix}${clientID}/${name}/${id}`;
}

export function parseKeyToID(
  name: string,
  key: string,
): PresenceEntity | undefined {
  const parts = key.split('/');
  if (
    parts.length !== 5 ||
    parts[0] !== '-' ||
    parts[1] !== 'p' ||
    parts[2] === '' || // clientID
    parts[3] !== name
  ) {
    return undefined;
  }
  return {clientID: parts[2], id: parts[4]};
}

const idFromEntity: IDFromEntityFunc<PresenceEntity, PresenceEntity> = (
  _tx: ReadTransaction,
  entity: PresenceEntity,
) => ({clientID: entity.clientID, id: entity.id});

function normalizePresenceID(
  tx: {clientID: string},
  base: Partial<PresenceEntity> | undefined,
) {
  if (base === undefined) {
    return {clientID: tx.clientID, id: ''};
  }
  return {
    clientID: base.clientID ?? tx.clientID,
    id: base.id ?? '',
  };
}

function normalizeUpdate<T extends {id?: string | undefined}>(
  tx: {clientID: string},
  update: T,
): T & PresenceEntity {
  validateMutate(tx, update);
  return {
    ...update,
    clientID: tx.clientID,
    id: update.id ?? '',
  };
}

function normalizeForSet<V extends PresenceID>(
  tx: {clientID: string},
  v: V,
): V & PresenceEntity {
  if (v === null) {
    throw new TypeError('Expected object, received null');
  }
  if (typeof v !== 'object') {
    throw new TypeError(`Expected object, received ${typeof v}`);
  }

  validateMutate(tx, v);

  type R = V & PresenceEntity;

  if (v.clientID === undefined && v.id === undefined) {
    return {...v, clientID: tx.clientID, id: ''};
  }
  if (v.id === undefined) {
    return {...v, id: ''} as R;
  }
  if (v.clientID === undefined) {
    return {...v, clientID: tx.clientID} as R;
  }

  return v as R;
}

export function normalizeScanOptions(options?: ListOptionsForPresence) {
  if (!options) {
    return options;
  }
  const {startAtID, limit} = options;
  return {
    startAtID: startAtID && normalizePresenceID({clientID: ''}, startAtID),
    limit,
  };
}

function validateMutate(tx: {clientID: string}, id: PresenceID): void {
  if (id.clientID && id.clientID !== tx.clientID) {
    throw new Error(
      `Can only mutate own entities. Expected clientID "${tx.clientID}" but received "${id.clientID}"`,
    );
  }
}

export function generatePresence<T extends PresenceEntity>(
  name: string,
  parse: Parse<T> | undefined = undefined,
  logger: OptionalLogger = console,
): GeneratePresenceResult<T> {
  if (name === '' || name.includes('/')) {
    throw new Error(`Invalid name: ${name}. Must not be empty or include '/'`);
  }

  const keyFromEntityLocal: KeyFromEntityFunc<PresenceEntity> = (_tx, entity) =>
    keyFromID(name, entity);
  const keyFromIDLocal: KeyFromLookupIDFunc<PresenceEntity> = id =>
    keyFromID(name, id);
  const parseKeyToIDLocal: KeyToIDFunc<PresenceEntity> = (key: string) =>
    parseKeyToID(name, key);
  const firstKey = () => presencePrefix;
  const parseInternal: ParseInternal<T> = (_, v) => maybeParse(parse, v);
  const parseAndValidateClientIDForMutate: ParseInternal<T> = (tx, v) =>
    parseInternal(tx, normalizeForSet(tx, v as OptionalIDs<PresenceEntity>));
  const set: GeneratePresenceResult<T>['set'] = (tx, value) =>
    setImpl(keyFromEntityLocal, parseAndValidateClientIDForMutate, tx, value);

  return {
    set,
    put: set,
    init: (tx, value) =>
      initImpl(
        keyFromEntityLocal,
        parseAndValidateClientIDForMutate,
        tx,
        value,
      ),
    update: (tx, update) =>
      updateImpl(
        keyFromEntityLocal,
        idFromEntity,
        parseAndValidateClientIDForMutate,
        tx,
        normalizeUpdate(tx, update),
        logger,
      ),
    delete: (tx, id) =>
      deleteImpl(
        keyFromIDLocal,
        validateMutate,
        tx,
        normalizePresenceID(tx, id),
      ),
    has: (tx, id) => hasImpl(keyFromIDLocal, tx, normalizePresenceID(tx, id)),
    get: (tx, id) =>
      getImpl(keyFromIDLocal, parseInternal, tx, normalizePresenceID(tx, id)),
    mustGet: (tx, id) =>
      mustGetImpl(
        keyFromIDLocal,
        parseInternal,
        tx,
        normalizePresenceID(tx, id),
      ),
    list: (tx, options?) =>
      listImpl(
        keyFromIDLocal,
        parseKeyToIDLocal,
        firstKey,
        parse,
        tx,
        normalizeScanOptions(options),
      ),
    listIDs: (tx, options?) =>
      listIDsImpl(
        keyFromIDLocal,
        parseKeyToIDLocal,
        firstKey,
        tx,
        normalizeScanOptions(options),
      ),
    listEntries: (tx, options?) =>
      listEntriesImpl(
        keyFromIDLocal,
        parseKeyToIDLocal,
        firstKey,
        parse,
        tx,
        normalizeScanOptions(options),
      ),
  };
}
