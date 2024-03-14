import {Statement} from './statement.js';
import {AST, Operator, Primitive} from '../ast/ast.js';
import {Context} from '../context/context.js';
import {Misuse} from '../error/misuse.js';
import {EntitySchema} from '../schema/entity-schema.js';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type SelectedFields<T, Fields extends Selectable<any>[]> = Pick<
  T,
  Fields[number] extends keyof T ? Fields[number] : never
>;

export type Selectable<T extends EntitySchema> = keyof T['fields'];

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
  ) => EntityQuery<Schema, SelectedFields<Schema['fields'], Fields>[]>;
  readonly count: () => EntityQuery<Schema, number>;
  readonly where: <K extends keyof Schema['fields']>(
    f: K,
    op: Operator,
    value: Schema['fields'][K],
  ) => EntityQuery<Schema, Return>;
  readonly limit: (n: number) => EntityQuery<Schema, Return>;
  readonly asc: (
    ...x: (keyof Schema['fields'])[]
  ) => EntityQuery<Schema, Return>;
  readonly desc: (
    ...x: (keyof Schema['fields'])[]
  ) => EntityQuery<Schema, Return>;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  readonly _ast: AST;

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
  }

  select<Fields extends Selectable<S>[]>(...x: Fields) {
    if (this.#ast.select === 'count') {
      throw new Misuse(
        'A query can either return fields or a count, not both.',
      );
    }
    const select = new Set(this.#ast.select);
    for (const more of x) {
      select.add(more as string);
    }

    return new EntityQueryImpl<S, SelectedFields<S['fields'], Fields>[]>(
      this.#context,
      this.#name,
      {
        ...this.#ast,
        select: [...select],
      },
    );
  }

  where<K extends keyof S['fields']>(
    field: K,
    op: Operator,
    value: S['fields'][K],
  ) {
    return new EntityQueryImpl<S, Return>(this.#context, this.#name, {
      ...this.#ast,
      where: [
        ...(this.#ast.where !== undefined
          ? [...this.#ast.where, 'AND' as const]
          : []),
        {
          field: field as string,
          op,
          value: {
            type: 'literal',
            value: value as Primitive,
          },
        },
      ],
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

  asc(...x: (keyof S['fields'])[]) {
    if (!x.includes('id')) {
      x.push('id');
    }

    return new EntityQueryImpl<S, Return>(this.#context, this.#name, {
      ...this.#ast,
      orderBy: [x as string[], 'asc'],
    });
  }

  desc(...x: (keyof S['fields'])[]) {
    if (!x.includes('id')) {
      x.push('id');
    }

    return new EntityQueryImpl<S, Return>(this.#context, this.#name, {
      ...this.#ast,
      orderBy: [x as string[], 'desc'],
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

  // eslint-disable-next-line @typescript-eslint/naming-convention
  get _ast() {
    return this.#ast;
  }

  prepare(): Statement<Return> {
    return new Statement<Return>(this.#context, this);
  }
}
