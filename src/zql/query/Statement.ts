import {Comparator} from '@vlcn.io/ds-and-algos/types';
import {buildPipeline} from '../ast-to-ivm/pipelineBuilder.js';
import {IView} from '../ivm/view/IView.js';
import {PersistentTreeView} from '../ivm/view/PersistentTreeView.js';
import {EntitySchema} from '../schema/EntitySchema.js';
import {MakeHumanReadable, IEntityQuery} from './IEntityQuery.js';
import {Context} from './context/contextProvider.js';
import {Entity} from '../../generate.js';
import {DifferenceStream} from '../ivm/graph/DifferenceStream.js';
import {ValueView} from '../ivm/view/PrimitiveView.js';

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
    this.#pipeline = buildPipeline(
      sourceName => c.getSource(sourceName).stream,
      q._ast,
    );
    this.#ast = q._ast;
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
          makeComparator(this.#ast.orderBy),
          this.#ast.orderBy === undefined,
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

// Fk... this is an problem.
// The selection set may not include the columns ordered by
// but we apply ordering as the final step in the pipeline.
// Either:
// 1. Force-select order-by fields
// 2. Add data to the events to include the ordering fields
// ?
// We can't order-by before the view since the view must know how to order as it will receive single rows
// to be placed into the view in the correct position.
function makeComparator<T>(
  _ordering?: [string[], 'asc' | 'desc'],
): Comparator<T> {
  // if (ordering === undefined) {
  //   return idComparator;
  // }
  return idComparator as Comparator<T>;
}

function idComparator(a: Entity, b: Entity) {
  return a.id.localeCompare(b.id);
}
