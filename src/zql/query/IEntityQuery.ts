/* eslint-disable @typescript-eslint/ban-types */
/* eslint-disable @typescript-eslint/no-explicit-any */
import {EntitySchema} from '../schema/EntitySchema.js';
import {IStatement} from './Statement.js';
import {AST, Operator} from './ZqlAst.js';

export type SelectedFields<T, Fields extends Selectable<any>[]> = Pick<
  T,
  Fields[number] extends keyof T ? Fields[number] : never
>;

export type Selectable<T extends EntitySchema> = keyof T['fields'];

export type MakeHumanReadable<T> = {} & {
  readonly [P in keyof T]: T[P] extends string ? T[P] : MakeHumanReadable<T[P]>;
};

export interface IEntityQuery<TSchema extends EntitySchema, TReturn = []> {
  readonly select: <Fields extends Selectable<TSchema>[]>(
    ...x: Fields
  ) => IEntityQuery<TSchema, SelectedFields<TSchema['fields'], Fields>[]>;
  readonly count: () => IEntityQuery<TSchema, number>;
  readonly where: <K extends keyof TSchema['fields']>(
    f: K,
    op: Operator,
    value: TSchema['fields'][K],
  ) => IEntityQuery<TSchema, TReturn>;
  readonly limit: (n: number) => IEntityQuery<TSchema, TReturn>;
  readonly asc: (
    ...x: (keyof TSchema['fields'])[]
  ) => IEntityQuery<TSchema, TReturn>;
  readonly desc: (
    ...x: (keyof TSchema['fields'])[]
  ) => IEntityQuery<TSchema, TReturn>;
  // eslint-disable-next-line @typescript-eslint/naming-convention
  readonly _ast: AST;

  // TODO: we can probably skip the `prepare` step and just have `materialize`
  // Although we'd need the prepare step in order to get a stmt to change bindings.
  readonly prepare: () => IStatement<TReturn>;
}
