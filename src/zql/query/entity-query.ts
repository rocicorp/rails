import {
  AST,
  Condition,
  Primitive,
  SimpleCondition,
  SimpleOperator,
} from '../ast/ast.js';
import {Context} from '../context/context.js';
import {must} from '../error/asserts.js';
import {Misuse} from '../error/misuse.js';
import {EntitySchema} from '../schema/entity-schema.js';
import {Statement} from './statement.js';

type FieldValue<
  S extends EntitySchema,
  K extends Selectable<S>,
> = S['fields'][K] extends Primitive | undefined ? S['fields'][K] : never;

export type SelectedFields<
  S extends EntitySchema,
  Fields extends Selectable<EntitySchema>[],
> = Pick<
  S['fields'],
  Fields[number] extends keyof S['fields'] ? Fields[number] : never
>;

type AsString<T> = T extends string ? T : never;

export type Selectable<S extends EntitySchema> =
  | AsString<keyof S['fields']>
  | 'id';

/**
 * Have you ever noticed that when you hover over Types in TypeScript, it shows
 * Pick<Omit<T, K>, K>? Rather than the final object structure after picking and omitting?
 * Or any time you use a type alias.
 *
 * MakeHumanReadable collapses the type aliases into their final form.
 */
// eslint-disable-next-line @typescript-eslint/ban-types
export type MakeHumanReadable<T> = {} & {
  readonly [P in keyof T]: T[P] extends string ? T[P] : MakeHumanReadable<T[P]>;
};

export interface EntityQuery<Schema extends EntitySchema, Return = []> {
  readonly select: <Fields extends Selectable<Schema>[]>(
    ...x: Fields
  ) => EntityQuery<Schema, SelectedFields<Schema, Fields>[]>;
  readonly count: () => EntityQuery<Schema, number>;
  readonly where: <Key extends Selectable<Schema>>(
    f: Key,
    op: SimpleOperator,
    value: FieldValue<Schema, Key>,
  ) => EntityQuery<Schema, Return>;
  readonly limit: (n: number) => EntityQuery<Schema, Return>;
  readonly asc: (...x: Selectable<Schema>[]) => EntityQuery<Schema, Return>;
  readonly desc: (...x: Selectable<Schema>[]) => EntityQuery<Schema, Return>;

  // TODO: we can probably skip the `prepare` step and just have `materialize`
  // Although we'd need the prepare step in order to get a stmt to change bindings.
  readonly prepare: () => Statement<Return>;
}

let aliasCount = 0;

export class EntityQueryImpl<S extends EntitySchema, Return = []>
  implements EntityQuery<S, Return>
{
  readonly #ast: AST;
  readonly #name: string;
  readonly #context: Context;

  constructor(context: Context, tableName: string, ast?: AST) {
    this.#ast = ast ?? {
      table: tableName,
      alias: aliasCount++,
      orderBy: [['id'], 'asc'],
    };
    this.#name = tableName;
    this.#context = context;

    // TODO(arv): Guard this with TESTING once we have the infrastructure.
    astWeakMap.set(this, this.#ast);
  }

  select<Fields extends Selectable<S>[]>(...x: Fields) {
    if (this.#ast.select === 'count') {
      throw new Misuse(
        'A query can either return fields or a count, not both.',
      );
    }
    const select = new Set(this.#ast.select);
    for (const more of x) {
      select.add(more);
    }

    return new EntityQueryImpl<S, SelectedFields<S, Fields>[]>(
      this.#context,
      this.#name,
      {
        ...this.#ast,
        select: [...select],
      },
    );
  }

  where<K extends Selectable<S>>(
    field: K,
    op: SimpleOperator,
    value: FieldValue<S, K>,
  ) {
    const leaf: SimpleCondition = {
      field,
      op,
      value: {
        type: 'literal',
        value: value as Primitive,
      },
    };

    let cond: Condition;
    if (!this.#ast.where) {
      cond = leaf;
    } else if (this.#ast.where.op === 'AND') {
      cond = {
        op: 'AND',
        conditions: [...this.#ast.where.conditions, leaf],
      };
    } else {
      cond = {
        op: 'AND',
        conditions: [this.#ast.where, leaf],
      };
    }

    return new EntityQueryImpl<S, Return>(this.#context, this.#name, {
      ...this.#ast,
      where: cond,
    });
  }

  limit(n: number) {
    if (this.#ast.limit !== undefined) {
      throw new Misuse('Limit already set');
    }

    return new EntityQueryImpl<S, Return>(this.#context, this.#name, {
      ...this.#ast,
      limit: n,
    });
  }

  asc(...x: Selectable<S>[]) {
    if (!x.includes('id')) {
      x.push('id');
    }

    return new EntityQueryImpl<S, Return>(this.#context, this.#name, {
      ...this.#ast,
      orderBy: [x, 'asc'],
    });
  }

  desc(...x: Selectable<S>[]) {
    if (!x.includes('id')) {
      x.push('id');
    }

    return new EntityQueryImpl<S, Return>(this.#context, this.#name, {
      ...this.#ast,
      orderBy: [x, 'desc'],
    });
  }

  count() {
    if (this.#ast.select !== undefined) {
      throw new Misuse(
        'Selection set already set. Will not change to a count query.',
      );
    }
    return new EntityQueryImpl<S, number>(this.#context, this.#name, {
      ...this.#ast,
      select: 'count',
    });
  }

  prepare(): Statement<Return> {
    return new Statement<Return>(this.#context, this.#ast);
  }
}

const astWeakMap = new WeakMap<EntityQueryImpl<EntitySchema, unknown>, AST>();

export function astForTesting(q: EntityQueryImpl<EntitySchema, unknown>): AST {
  return must(astWeakMap.get(q));
}
