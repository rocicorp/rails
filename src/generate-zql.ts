import {OptionalLogger} from '@rocicorp/logger';
import {Entity, Parse} from './generate.js';
import {ReplicacheLike} from './replicache-like.js';
import {Console} from './types/lib/dom/console.js';
import {makeReplicacheContext} from './zql/context/replicache-context.js';
import {EntityQueryImpl, type EntityQuery} from './zql/query/entity-query.js';

declare const console: Console;

export function generateZQL<E extends Entity>(
  r: ReplicacheLike,
  tableName: string,
  _parse: Parse<E> | undefined = undefined,
  _logger: OptionalLogger = console,
): EntityQuery<{fields: E}> {
  const c = makeReplicacheContext(r);
  return new EntityQueryImpl<{fields: E}>(c, tableName);
}
