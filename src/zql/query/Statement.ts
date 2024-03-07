import {buildPipeline} from '../ast-to-ivm/pipelineBuilder.js';
import {IView} from '../ivm/view/IView.js';
import {PersistentTreeView} from '../ivm/view/PersistentTreeView.js';
import {EntitySchema} from '../schema/EntitySchema.js';
import {MakeHumanReadable, IEntityQuery} from './IEntityQuery.js';
import {Context} from './context/contextProvider.js';

export interface IStatement<TReturn> {
  materialize: () => IView<MakeHumanReadable<TReturn>>;
  destroy: () => void;
}

export class Statement<TSchema extends EntitySchema, TReturn>
  implements IStatement<TReturn>
{
  readonly #pipeline;
  readonly #ast;
  #materialization: IView<MakeHumanReadable<TReturn>> | null = null;

  constructor(c: Context, q: IEntityQuery<TSchema, TReturn>) {
    this.#pipeline = buildPipeline(
      sourceName => c.getSource(sourceName).stream,
      q._ast,
    );
    this.#ast = q._ast;
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
      // 1. select correct view based on projection (count vs select)
      // 2. apply order
      // 3. apply limit
      this.#materialization = new PersistentTreeView();
    }

    return this.#materialization;
  }

  // For savvy users that want to subscribe directly to diffs.
  onDifference() {}

  destroy() {
    // destroy the entire pipeline by disconnecting it from the source.
  }
}
