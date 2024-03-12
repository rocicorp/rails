import {buildPipeline, orderingProp} from '../ast-to-ivm/pipeline-builder.js';
import {View} from '../ivm/view/view.js';
import {PersistentTreeView} from '../ivm/view/tree-view.js';
import {EntitySchema} from '../schema/entity-schema.js';
import {MakeHumanReadable, EntityQuertType} from './entity-query-type.js';
import {Context} from '../context/context.js';
import {DifferenceStream} from '../ivm/graph/difference-stream.js';
import {ValueView} from '../ivm/view/primitive-view.js';
import {Primitive} from '../ast/ast.js';
import {Entity} from '../../generate.js';
import {invariant} from '../error/asserts.js';

export interface IStatement<TReturn> {
  materialize: () => View<MakeHumanReadable<TReturn>>;
  destroy: () => void;
}

export class Statement<TSchema extends EntitySchema, TReturn>
  implements IStatement<TReturn>
{
  readonly #pipeline;
  readonly #ast;
  readonly #context;
  #materialization: View<
    TReturn extends [] ? TReturn[number] : TReturn
  > | null = null;

  constructor(c: Context, q: EntityQuertType<TSchema, TReturn>) {
    this.#ast = q._ast;
    this.#pipeline = buildPipeline(
      <T extends Entity>(sourceName: string) =>
        c.getSource(sourceName, this.#ast.orderBy)
          .stream as DifferenceStream<T>,
      q._ast,
    );
    this.#context = c;
  }

  // run(): MakeHumanReadable<TReturn> {
  //   // TODO run the query!
  //   // 1. materialize the view
  //   // 2. if this is a 1-shot then we disconnect the view from updates?
  //   //   Disconnect the pipeline too?
  //   //
  //   // Our other options is to leave the view materialized.
  //   // Any future `run` would just immediately return.
  //   //
  //   // Nothing gets destroyed until the user `finalizes` the statement.
  //   return {} as TReturn;
  // }

  materialize(): View<MakeHumanReadable<TReturn>> {
    // TODO: invariants to throw if the statement is not completely bound before materialization.
    if (this.#materialization === null) {
      if (this.#ast.select === 'count') {
        // materialize primitive
        this.#materialization = new ValueView<number>(
          this.#context.materialite,
          this.#pipeline as DifferenceStream<number>,
          0,
        ) as unknown as View<TReturn extends [] ? TReturn[number] : TReturn>;
      } else {
        this.#materialization = new PersistentTreeView<
          TReturn extends [] ? TReturn[number] : never
        >(
          this.#context.materialite,
          this.#pipeline as DifferenceStream<
            TReturn extends [] ? TReturn[number] : never
          >,
          this.#ast.orderBy?.[1] === 'asc' ? ascComparator : descComparator,
          true, // TODO: since we're going to control everything we can make this so.
          this.#ast.limit,
        ) as unknown as View<TReturn extends [] ? TReturn[number] : TReturn>;
      }
    }

    return this.#materialization as View<MakeHumanReadable<TReturn>>;
  }

  // For savvy users that want to subscribe directly to diffs.
  onDifference() {}

  destroy() {
    // destroy the entire pipeline by disconnecting it from the source.
  }
}

export function ascComparator<T extends {[orderingProp]: Primitive[]}>(
  l: T,
  r: T,
): number {
  const leftVals = l[orderingProp];
  const rightVals = r[orderingProp];

  invariant(
    leftVals.length === rightVals.length,
    'orderingProp lengths must match',
  );

  for (let i = 0; i < leftVals.length; i++) {
    const leftVal = leftVals[i];
    const rightVal = rightVals[i];
    if (leftVal === rightVal) {
      continue;
    }
    if (leftVal === null) {
      return -1;
    }
    if (rightVal === null) {
      return 1;
    }
    if (leftVal < rightVal) {
      return -1;
    } else if (leftVal > rightVal) {
      return 1;
    }
  }

  return 0;
}

export function descComparator<T extends {[orderingProp]: Primitive[]}>(
  l: T,
  r: T,
): number {
  return ascComparator(r, l);
}
