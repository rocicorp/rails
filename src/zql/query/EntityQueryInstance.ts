/* eslint-disable @typescript-eslint/no-explicit-any */
import {Misuse} from '../error/Misuse.js';
import {EntitySchema} from '../schema/EntitySchema.js';
import {
  MakeHumanReadable,
  QueryInstanceType,
  IStatement,
  Selectable,
  SelectedFields,
} from './EntityQueryType.js';
import {AST, Operator, Primitive} from './ZqlAst.js';

let aliasCount = 0;

export class QueryInstance<S extends EntitySchema, TReturn = []>
  implements QueryInstanceType<S, TReturn>
{
  readonly #ast: AST;
  readonly #name: string;

  constructor(tableName: string, ast?: AST) {
    this.#ast = ast ?? {
      table: tableName,
      alias: aliasCount++,
    };
    this.#name = tableName;
  }

  select<Fields extends Selectable<S>[]>(...x: Fields) {
    if (this.#ast.select === 'count') {
      throw new Misuse(
        'A query can either return fields or a count, not both.',
      );
    }

    return new QueryInstance<S, SelectedFields<S['fields'], Fields>[]>(
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
    return new QueryInstance<S, TReturn>(this.#name, {
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

    return new QueryInstance<S, TReturn>(this.#name, {...this.#ast, limit: n});
  }

  asc(...x: (keyof S['fields'])[]) {
    if (this.#ast.orderBy !== undefined) {
      throw new Misuse('OrderBy already set');
    }

    return new QueryInstance<S, TReturn>(this.#name, {
      ...this.#ast,
      orderBy: [x as string[], 'asc'],
    });
  }

  desc(...x: (keyof S['fields'])[]) {
    if (this.#ast.orderBy !== undefined) {
      throw new Misuse('OrderBy already set');
    }

    return new QueryInstance<S, TReturn>(this.#name, {
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
    return new QueryInstance<S, number>(this.#name, {
      ...this.#ast,
      select: 'count',
    });
  }

  // eslint-disable-next-line @typescript-eslint/naming-convention
  get _ast() {
    return this.#ast;
  }

  prepare() {
    // TODO: build the IVM pipeline
    return new Statement<TReturn>();
  }
}

class Statement<TReturn> implements IStatement<TReturn> {
  constructor() {}

  run(): MakeHumanReadable<TReturn> {
    // TODO run the query!
    return {} as TReturn;
  }
}
