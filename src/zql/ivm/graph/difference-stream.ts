import {Primitive} from '../../ast/ast.js';
import {Multiset} from '../multiset.js';
import {Source} from '../source/source.js';
import {Version} from '../types.js';
import {
  DifferenceStreamWriter,
  RootDifferenceStreamWriter,
} from './difference-stream-writer.js';
import {
  AggregateOut,
  FullAvgOperator,
  FullCountOperator,
  FullSumOperator,
} from './operators/full-agg-operators.js';
import {DebugOperator} from './operators/debug-operator.js';
import {DifferenceEffectOperator} from './operators/difference-effect-operator.js';
import {FilterOperator} from './operators/filter-operator.js';
import {MapOperator} from './operators/map-operator.js';
import {ReduceOperator} from './operators/reduce-operator.js';
import {QueueEntry} from './queue.js';

/**
 * Holds a reference to a `DifferenceStreamWriter` and allows users
 * to add operators onto it.
 *
 * E.g.,
 * s = new DifferenceStream();
 *
 * mapped = s.map();
 * filtered = s.filter();
 *
 *       s
 *    /     \
 *  mapped filtered
 *
 * mappedAndFiltered = s.map().filter();
 *
 *     s
 *     |
 *    mapped
 *     |
 *   filtered
 */
// T extends object: I believe in the context of ZQL we only deal with object.
export class DifferenceStream<T extends object> {
  readonly #upstreamWriter;

  constructor(upstreamWriter?: DifferenceStreamWriter<T> | undefined) {
    this.#upstreamWriter = upstreamWriter ?? new DifferenceStreamWriter<T>();
  }

  map<O extends object>(f: (value: T) => O): DifferenceStream<O> {
    const ret = new DifferenceStream<O>();
    new MapOperator<T, O>(
      this.#upstreamWriter.newReader(),
      ret.#upstreamWriter,
      f,
    );
    return ret;
  }

  filter<S extends T>(f: (x: T) => x is S): DifferenceStream<S>;
  filter(f: (x: T) => boolean): DifferenceStream<T>;
  filter<S extends T>(f: (x: T) => boolean): DifferenceStream<S> {
    const ret = new DifferenceStream<S>();
    new FilterOperator<T>(
      this.#upstreamWriter.newReader(),
      ret.#upstreamWriter,
      f,
    );
    return ret;
  }

  reduce<K extends Primitive, O extends object>(
    getKey: (value: T) => K,
    getIdentity: (value: T) => string,
    f: (input: Iterable<T>) => O,
  ): DifferenceStream<O> {
    const ret = new DifferenceStream<O>();
    new ReduceOperator<K, T, O>(
      this.#upstreamWriter.newReader(),
      ret.#upstreamWriter,
      getIdentity,
      getKey,
      f,
    );
    return ret;
  }

  /**
   * This differs from count in that `size` just counts the entire
   * stream whereas `count` counts the number of times each key appears.
   * @returns returns the size of the stream
   */
  count<Alias extends string>(alias: Alias) {
    const ret = new DifferenceStream<AggregateOut<T, [[Alias, number]]>>();
    new FullCountOperator(
      this.#upstreamWriter.newReader(),
      ret.#upstreamWriter,
      alias,
    );
    return ret;
  }

  average<Field extends keyof T, Alias extends string>(
    field: Field,
    alias: Alias,
  ) {
    const ret = new DifferenceStream<AggregateOut<T, [[Alias, number]]>>();
    new FullAvgOperator(
      this.#upstreamWriter.newReader(),
      ret.#upstreamWriter,
      field,
      alias,
    );
    return ret;
  }

  sum<Field extends keyof T, Alias extends string>(field: Field, alias: Alias) {
    const ret = new DifferenceStream<AggregateOut<T, [[Alias, number]]>>();
    new FullSumOperator(
      this.#upstreamWriter.newReader(),
      ret.#upstreamWriter,
      field,
      alias,
    );
    return ret;
  }

  /**
   * Runs a side-effect for all events in the stream.
   * If `mult < 0` that means the value V was retracted `mult` times.
   * If `mult > 0` that means the value V was added `mult` times.
   * `mult === 0` is a no-op and can be ignored. Generally shouldn't happen.
   */
  effect(f: (i: T, mult: number) => void) {
    const ret = new DifferenceStream<T>();
    new DifferenceEffectOperator(
      this.#upstreamWriter.newReader(),
      ret.#upstreamWriter,
      f,
    );
    return ret;
  }

  debug(onMessage: (c: QueueEntry<T>) => void) {
    const ret = new DifferenceStream<T>();
    new DebugOperator(
      this.#upstreamWriter.newReader(),
      ret.#upstreamWriter,
      onMessage,
    );
    return ret;
  }

  queueData(data: [Version, Multiset<T>]) {
    this.#upstreamWriter.queueData(data);
  }

  notify(v: Version) {
    this.#upstreamWriter.notify(v);
  }

  notifyCommitted(v: Version) {
    this.#upstreamWriter.notifyCommitted(v);
  }

  newReader() {
    return this.#upstreamWriter.newReader();
  }

  destroy() {
    this.#upstreamWriter.destroy();
  }
}

export class RootDifferenceStream<
  T extends object,
> extends DifferenceStream<T> {
  constructor(source: Source<T>) {
    super(new RootDifferenceStreamWriter<T>(source));
  }

  get stream() {
    return this;
  }
}
