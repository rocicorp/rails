import {Entity} from '../../generate.js';
import {buildPipeline, orderingProp} from '../ast-to-ivm/pipeline-builder.js';
import {Primitive} from '../ast/ast.js';
import {Context} from '../context/context.js';
import {invariant, must} from '../error/asserts.js';
import {compareEntityFields} from '../ivm/compare.js';
import {DifferenceStream} from '../ivm/graph/difference-stream.js';
import {ValueView} from '../ivm/view/primitive-view.js';
import {MutableTreeView} from '../ivm/view/tree-view.js';
import {View} from '../ivm/view/view.js';
import {EntitySchema} from '../schema/entity-schema.js';
import {EntityQuery, MakeHumanReadable} from './entity-query.js';

export interface IStatement<TReturn> {
  subscribe(cb: (value: MakeHumanReadable<TReturn>) => void): () => void;
  exec(): Promise<MakeHumanReadable<TReturn>>;
  view(): View<TReturn>;
  destroy(): void;
}

export class Statement<TReturn> implements IStatement<TReturn> {
  readonly #pipeline;
  readonly #ast;
  readonly #context;
  #materialization: View<
    TReturn extends [] ? TReturn[number] : TReturn
  > | null = null;

  constructor(c: Context, q: EntityQuery<EntitySchema, TReturn>) {
    this.#ast = q._ast;
    this.#pipeline = buildPipeline(
      <T extends Entity>(sourceName: string) =>
        c.getSource(sourceName, this.#ast.orderBy)
          .stream as DifferenceStream<T>,
      q._ast,
    );
    this.#context = c;
  }

  view(): View<TReturn> {
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
        this.#materialization = new MutableTreeView<
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

    // TODO: we'll want some ability to let a caller await
    // the response of historical data.
    this.#materialization.pullHistoricalData();

    return this.#materialization as View<TReturn>;
  }

  subscribe(cb: (value: MakeHumanReadable<TReturn>) => void) {
    if (this.#materialization === null) {
      this.view();
    }

    return must(this.#materialization).on(cb);
  }

  // Note: should we provide a version that takes a callback?
  // So it can resolve in the same micro task?
  // since, in the common case, the data will always be available.
  exec() {
    if (this.#materialization === null) {
      this.view();
    }

    if (this.#materialization?.hydrated) {
      return Promise.resolve(this.#materialization.value) as Promise<
        MakeHumanReadable<TReturn>
      >;
    }

    return new Promise<MakeHumanReadable<TReturn>>(resolve => {
      const cleanup = must(this.#materialization).on(value => {
        resolve(value as MakeHumanReadable<TReturn>);
        cleanup();
      });
    }) as Promise<MakeHumanReadable<TReturn>>;
  }

  // For savvy users that want to subscribe directly to diffs.
  // onDifference() {}

  destroy() {
    this.#pipeline.destroy();
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
    const comp = compareEntityFields(leftVal, rightVal);
    if (comp !== 0) {
      return comp;
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
