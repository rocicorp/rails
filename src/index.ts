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

export type GenerateResult<T extends Entity> = [
  create: (tx: WriteTransaction, value: T) => Promise<void>,
  get: (tx: ReadTransaction, id: string) => Promise<T | undefined>,
  update: (tx: WriteTransaction, value: Update<T>) => Promise<void>,
  del: (tx: WriteTransaction, id: string) => Promise<void>,
  list: (tx: ReadTransaction, options?: ListOptions) => Promise<Array<T>>,
];

export function generate<T extends Entity>(
  prefix: string,
  schema: ZodType<T>,
  logger: OptionalLogger = console,
): GenerateResult<T> {
  return [
    (tx: WriteTransaction, value: T) => createImpl(prefix, schema, tx, value),
    (tx: ReadTransaction, id: string) => getImpl(prefix, schema, tx, id),
    (tx: WriteTransaction, update: Update<T>) =>
      updateImpl(prefix, schema, tx, update, logger),
    (tx: WriteTransaction, id: string) => deleteImpl(prefix, tx, id),
    (tx: ReadTransaction, options?: ListOptions) =>
      listImpl(prefix, schema, tx, options),
  ];
}

export const entitySchema = z.object({
  id: z.string(),
});
export type Entity = z.TypeOf<typeof entitySchema>;

function key(prefix: string, id: string) {
  return `${prefix}/${id}`;
}

async function createImpl<T extends Entity>(
  prefix: string,
  schema: ZodType<T>,
  tx: WriteTransaction,
  initial: ReadonlyJSONValue,
) {
  const val = parseIfDebug(schema, initial);
  await tx.put(key(prefix, val.id), val);
}

async function getImpl<T extends Entity>(
  prefix: string,
  schema: ZodType<T>,
  tx: ReadTransaction,
  id: string,
) {
  const val = await tx.get(key(prefix, id));
  if (val === undefined) {
    return val;
  }
  // TODO: parse only in debug mode
  const parsed = parseIfDebug(schema, val);
  return parsed;
}

async function updateImpl<T extends Entity>(
  prefix: string,
  schema: ZodType<T>,
  tx: WriteTransaction,
  update: Update<T>,
  logger: OptionalLogger,
) {
  const {id} = update;
  const prev = await getImpl(prefix, schema, tx, id);
  if (prev === undefined) {
    logger.debug?.(`no such entity ${id}, skipping update`);
    return;
  }
  const next = {...prev, ...update};
  const parsed = parseIfDebug(schema, next);
  // TODO: share duplicate key() call
  await tx.put(key(prefix, id), parsed);
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
