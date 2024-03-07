/* eslint-disable @typescript-eslint/no-explicit-any */
import {Misuse} from '../error/Misuse.js';
import {EntitySchema} from '../schema/EntitySchema.js';
import {IEntityQuery, Selectable, SelectedFields} from './IEntityQuery.js';
import {IStatement, Statement} from './Statement.js';
import {AST, Operator, Primitive} from './ZqlAst.js';
import {Context} from './context/contextProvider.js';

let aliasCount = 0;

export class EntityQuery<S extends EntitySchema, TReturn = []>
  implements IEntityQuery<S, TReturn>
{
  readonly #ast: AST;
  readonly #name: string;
  readonly #context: Context;

  constructor(context: Context, tableName: string, ast?: AST) {
    this.#ast = ast ?? {
      table: tableName,
      alias: aliasCount++,
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

    return new EntityQuery<S, SelectedFields<S['fields'], Fields>[]>(
      this.#context,
      this.#name,
      {
        ...this.#ast,
        select: [...new Set([...(this.#ast.select || []), ...x])] as any,
      },
    );
  }

  where<K extends keyof S['fields']>(
    field: K,
    op: Operator,
    value: S['fields'][K],
  ) {
    return new EntityQuery<S, TReturn>(this.#context, this.#name, {
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

    return new EntityQuery<S, TReturn>(this.#context, this.#name, {
      ...this.#ast,
      limit: n,
    });
  }

  asc(...x: (keyof S['fields'])[]) {
    if (this.#ast.orderBy !== undefined) {
      throw new Misuse('OrderBy already set');
    }

    return new EntityQuery<S, TReturn>(this.#context, this.#name, {
      ...this.#ast,
      orderBy: [x as string[], 'asc'],
    });
  }

  desc(...x: (keyof S['fields'])[]) {
    if (this.#ast.orderBy !== undefined) {
      throw new Misuse('OrderBy already set');
    }

    return new EntityQuery<S, TReturn>(this.#context, this.#name, {
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
    return new EntityQuery<S, number>(this.#context, this.#name, {
      ...this.#ast,
      select: 'count',
    });
  }

  // eslint-disable-next-line @typescript-eslint/naming-convention
  get _ast() {
    return this.#ast;
  }

  prepare(): IStatement<TReturn> {
    // TODO: build the IVM pipeline
    return new Statement<S, TReturn>(this.#context, this);
  }
}
