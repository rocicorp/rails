import {Entity} from '../../../generate.js';
import {Multiset} from '../multiset.js';
import {Source} from '../source/source.js';
import {Version} from '../types.js';
import {DifferenceStreamReader} from './difference-stream-reader.js';
import {
  DifferenceStreamWriter,
  RootDifferenceStreamWriter,
} from './difference-stream-writer.js';
import {IDifferenceStream} from './idifference-stream.js';
import {BranchOperator} from './operators/branch-operator.js';
import {ConcatOperator} from './operators/concat-operator.js';
import {LinearCountOperator} from './operators/count-operator.js';
import {DebugOperator} from './operators/debug-operator.js';
import {DifferenceEffectOperator} from './operators/difference-effect-operator.js';
import {DistinctOperator} from './operators/distinct-operator.js';
import {FilterOperator} from './operators/filter-operator.js';
import {MapOperator} from './operators/map-operator.js';
import {QueueEntry} from './queue.js';

/**
 * Used to build up a computation pipeline against a stream and then materialize it.
 *
 * Encapsulates all the details of wiring together operators, readers, and writers.
 */
let id = 0;
export class DifferenceStream<T> implements IDifferenceStream<T> {
  readonly id = id++;
  readonly #upstreamWriter: DifferenceStreamWriter<T>;

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

  concat(...streams: DifferenceStream<T>[]): DifferenceStream<T> {
    const ret = new DifferenceStream<T>();
    const first = this.#upstreamWriter.newReader();
    const inputs = streams.map(s => s.#upstreamWriter.newReader());
    new ConcatOperator<T>([first, ...inputs], ret.#upstreamWriter);
    return ret;
  }

  distinct(): DifferenceStream<T> {
    const ret = new DifferenceStream<Entity>();
    new DistinctOperator(
      this.#upstreamWriter.newReader() as DifferenceStreamReader<Entity>,
      ret.#upstreamWriter,
    );
    // TODO(arv): T should really `extends Entity` but I'm not sure how to factor this.
    return ret as DifferenceStream<T>;
  }

  branch(branchCount: number): DifferenceStream<T>[] {
    const ret = Array.from(
      {length: branchCount},
      () => new DifferenceStream<T>(),
    );
    new BranchOperator(
      this.#upstreamWriter.newReader(),
      ret.map(s => s.#upstreamWriter),
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

  toString() {
    return `DifferenceStream(${this.id}) {
  writer: ${this.#upstreamWriter.toString(1)},
}`;
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
