/* eslint-disable @typescript-eslint/ban-types */
/* eslint-disable @typescript-eslint/no-explicit-any */
import {EntitySchema} from '../schema/EntitySchema.js';
import {Operator} from './ZqlAst.js';

export type SelectedFields<T, Fields extends Selectable<any>[]> = Pick<
  T,
  Fields[number] extends keyof T ? Fields[number] : never
>;

export type Selectable<T extends EntitySchema> = keyof T['fields'];

export type MakeHumanReadable<T> = {} & {
  readonly [P in keyof T]: T[P] extends string ? T[P] : MakeHumanReadable<T[P]>;
};

export interface QueryInstanceType<TSchema extends EntitySchema, TReturn = []> {
  readonly select: <Fields extends Selectable<TSchema>[]>(
    ...x: Fields
  ) => QueryInstanceType<TSchema, SelectedFields<TSchema['fields'], Fields>[]>;
  readonly count: () => QueryInstanceType<TSchema, number>;
  readonly where: <K extends keyof TSchema['fields']>(
    f: K,
    op: Operator,
    value: TSchema['fields'][K],
  ) => QueryInstanceType<TSchema, TReturn>;
  readonly limit: (n: number) => QueryInstanceType<TSchema, TReturn>;
  readonly asc: (
    ...x: (keyof TSchema['fields'])[]
  ) => QueryInstanceType<TSchema, TReturn>;
  readonly desc: (
    ...x: (keyof TSchema['fields'])[]
  ) => QueryInstanceType<TSchema, TReturn>;

  readonly prepare: () => IStatement<TReturn>;
}

export interface IStatement<TReturn> {
  run: () => MakeHumanReadable<TReturn>;
}
