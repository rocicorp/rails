import {Entity} from '../../../generate.js';
import {Primitive} from '../../ast/ast.js';
import {Multiset} from '../multiset.js';
import {Source} from '../source/source.js';
import {Version} from '../types.js';
import {
  DifferenceStreamWriter,
  RootDifferenceStreamWriter,
} from './difference-stream-writer.js';
import {IDifferenceStream} from './idifference-stream.js';
import {LinearCountOperator} from './operators/count-operator.js';
import {DebugOperator} from './operators/debug-operator.js';
import {DifferenceEffectOperator} from './operators/difference-effect-operator.js';
import {FilterOperator} from './operators/filter-operator.js';
import {MapOperator} from './operators/map-operator.js';
import {ReduceOperator} from './operators/reduce-operator.js';
import {QueueEntry} from './queue.js';

/**
 * Used to build up a computation pipeline against a stream and then materialize it.
 * (Note: materialization of the stream is not yet implemented).
 *
 * Encapsulates all the details of wiring together operators, readers, and writers.
 */
export class DifferenceStream<T> implements IDifferenceStream<T> {
  readonly #upstreamWriter;

  constructor(upstreamWriter?: DifferenceStreamWriter<T> | undefined) {
    this.#upstreamWriter = upstreamWriter ?? new DifferenceStreamWriter<T>();
  }

  map<O>(f: (value: T) => O): DifferenceStream<O> {
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

  reduce<K extends Primitive, O extends Entity>(
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
  linearCount() {
    const ret = new DifferenceStream<number>();
    new LinearCountOperator(
      this.#upstreamWriter.newReader(),
      ret.#upstreamWriter,
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

export class RootDifferenceStream<T> extends DifferenceStream<T> {
  constructor(source: Source<T>) {
    super(new RootDifferenceStreamWriter<T>(source));
  }

  get stream() {
    return this;
  }
}
