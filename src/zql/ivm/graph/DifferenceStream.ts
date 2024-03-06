import {Multiset} from '../Multiset.js';
import {Version} from '../types.js';
import {DifferenceStreamWriter} from './DifferenceStreamWriter.js';
import {IDifferenceStream} from './IDifferenceStream.js';
import {LinearCountOperator} from './operators/CountOperator.js';
import {EffectOperator} from './operators/EffectOperator.js';
import {FilterOperator} from './operators/FilterOperator.js';
import {MapOperator} from './operators/MapOperator.js';

/**
 * Used to build up a computation pipeline against a stream and then materialize it.
 * (Note: materialization of the stream is not yet implemented).
 *
 * Encapsulates all the details of wiring together operators, readers, and writers.
 */
export class DifferenceStream<T> implements IDifferenceStream<T> {
  readonly #writer: DifferenceStreamWriter<T>;

  constructor() {
    this.#writer = new DifferenceStreamWriter<T>();
  }

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
   * Run some sort of side-effect against values in the stream.
   * e.g., I/O & logging
   */
  effect(f: (i: T) => void) {
    const ret = this.newStream<T>();
    new EffectOperator(this.#writer.newReader(), ret.#writer, f);
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
