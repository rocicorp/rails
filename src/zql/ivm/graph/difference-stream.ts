import {Multiset} from '../multiset.js';
import {Version} from '../types.js';
import {DifferenceStreamWriter} from './difference-stream-writer.js';
import {IDifferenceStream} from './idifference-stream.js';
import {LinearCountOperator} from './operators/count-operator.js';
import {DifferenceEffectOperator} from './operators/difference-effect-operator.js';
import {FilterOperator} from './operators/filter-operator.js';
import {MapOperator} from './operators/map-operator.js';

/**
 * Used to build up a computation pipeline against a stream and then materialize it.
 * (Note: materialization of the stream is not yet implemented).
 *
 * Encapsulates all the details of wiring together operators, readers, and writers.
 */
export class DifferenceStream<T> implements IDifferenceStream<T> {
  readonly #writer = new DifferenceStreamWriter<T>();

  newStream<X>(): DifferenceStream<X> {
    return new DifferenceStream();
  }

  map<O>(f: (value: T) => O): DifferenceStream<O> {
    const ret = this.newStream<O>();
    new MapOperator<T, O>(this.#writer.newReader(), ret.#writer, f);
    return ret;
  }

  filter<S extends T>(f: (x: T) => x is S): DifferenceStream<S>;
  filter(f: (x: T) => boolean): DifferenceStream<T>;
  filter<S extends T>(f: (x: T) => boolean): DifferenceStream<S> {
    const ret = this.newStream<S>();
    new FilterOperator<T>(this.#writer.newReader(), ret.#writer, f);
    return ret;
  }

  /**
   * This differs from count in that `size` just counts the entire
   * stream whereas `count` counts the number of times each key appears.
   * @returns returns the size of the stream
   */
  linearCount() {
    const ret = this.newStream<number>();
    new LinearCountOperator(this.#writer.newReader(), ret.#writer);
    return ret;
  }

  /**
   * Runs a side-effect for all events in the stream.
   * If `mult < 0` that means the value V was retracted `mult` times.
   * If `mult > 0` that means the value V was added `mult` times.
   * `mult === 0` is a no-op and can be ignored. Generally shouldn't happen.
   */
  effect(f: (i: T, mult: number) => void) {
    const ret = this.newStream<T>();
    new DifferenceEffectOperator(this.#writer.newReader(), ret.#writer, f);
    return ret;
  }

  queueData(data: [Version, Multiset<T>]) {
    this.#writer.queueData(data);
  }

  notify(v: Version) {
    this.#writer.notify(v);
  }

  notifyCommitted(v: Version) {
    this.#writer.notifyCommitted(v);
  }

  newReader() {
    return this.#writer.newReader();
  }
}
