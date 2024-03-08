import {buildPipeline, orderingProp} from '../ast-to-ivm/pipelineBuilder.js';
import {IView} from '../ivm/view/IView.js';
import {PersistentTreeView} from '../ivm/view/TreeView.js';
import {EntitySchema} from '../schema/EntitySchema.js';
import {MakeHumanReadable, IEntityQuery} from './IEntityQuery.js';
import {Context} from './context/contextProvider.js';
import {DifferenceStream} from '../ivm/graph/DifferenceStream.js';
import {ValueView} from '../ivm/view/PrimitiveView.js';
import {Primitive} from './ZqlAst.js';
import {Entity} from '../../generate.js';

export interface IStatement<TReturn> {
  materialize: () => IView<MakeHumanReadable<TReturn>>;
  destroy: () => void;
}

export class Statement<TSchema extends EntitySchema, TReturn>
  implements IStatement<TReturn>
{
  readonly #pipeline;
  readonly #ast;
  readonly #context;
  #materialization: IView<TReturn> | null = null;

  constructor(c: Context, q: IEntityQuery<TSchema, TReturn>) {
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

  materialize(): IView<MakeHumanReadable<TReturn>> {
    // TODO: invariants to throw if the statement is not completely bound before materialization.
    if (this.#materialization === null) {
      if (this.#ast.select === 'count') {
        // materialize primitive
        this.#materialization = new ValueView<number>(
          this.#context.materialite,
          this.#pipeline as DifferenceStream<number>,
          0,
        ) as unknown as IView<TReturn>;
      } else {
        this.#materialization = new PersistentTreeView<
          TReturn extends [] ? TReturn : never
        >(
          this.#context.materialite,
          this.#pipeline as DifferenceStream<
            TReturn extends [] ? TReturn[number] : never
          >,
          this.#ast.orderBy?.[1] === 'asc' ? ascComparator : descComparator,
          true, // TODO: since we're going to control everything we can make this so.
          this.#ast.limit,
        );
      }
    }

    return this.#materialization as IView<MakeHumanReadable<TReturn>>;
  }

  // For savvy users that want to subscribe directly to diffs.
  onDifference() {}

  destroy() {
    // destroy the entire pipeline by disconnecting it from the source.
  }
}

function ascComparator<T extends {[orderingProp]: Primitive[]}>(
  l: T,
  r: T,
): number {
  const leftVals = l[orderingProp];
  const rightVals = r[orderingProp];

  for (let i = 0; i < leftVals.length; i++) {
    const leftVal = leftVals[i];
    const rightVal = rightVals[i];
    if (leftVal === rightVal) {
      return 0;
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

function descComparator<T extends {[orderingProp]: Primitive[]}>(
  l: T,
  r: T,
): number {
  return ascComparator(r, l) * -1;
}
