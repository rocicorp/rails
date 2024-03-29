import {Entity} from '../../../../generate.js';
import {Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {UnaryOperator} from './unary-operator.js';

type State<T> = Map<string, [T, number]>;

let id = 0;

export class DistinctOperator<T extends Entity> extends UnaryOperator<T, T> {
  readonly id = id++;
  readonly #state = new Map<Version, State<T>>();
  readonly #emitted = new Map<Version, Set<string>>();

  constructor(
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
  ) {
    super(input, output, version => this.#run(version));
  }

  #run(version: Version) {
    function addValue(state: State<T>, value: T, multiplicity: number) {
      const {id} = value;
      const existing = state.get(id);
      if (!existing) {
        state.set(id, [value, multiplicity]);
        return;
      }
      // Mutate in place
      existing[1] += multiplicity;
    }

    // clear old states
    for (const v of this.#state.keys()) {
      if (v < version) {
        this.#state.delete(v);
      } else {
        break;
      }
    }

    // FIXME
    for (const v of this.#emitted.keys()) {
      if (v < version) {
        this.#emitted.delete(v);
      } else {
        break;
      }
    }

    let state = this.#state.get(version);
    if (!state) {
      state = new Map();
      this.#state.set(version, state);
    }

    for (const queueEntry of this.inputMessages(version)) {
      // TODO(arv): What about the Reply?
      const multiset = queueEntry[1];
      for (const entry of multiset.entries) {
        addValue(state, entry[0], entry[1]);
      }
    }

    if (state.size === 0) {
      return;
    }

    const values = [...state.values()];
    console.log(`values(${this.id})`, values);

    let emitted = this.#emitted.get(version);
    if (!emitted) {
      emitted = new Set();
      this.#emitted.set(version, emitted);
    }

    const mv = values
      .filter(v => v[1] !== 0)
      // .filter(([value]) => !emitted!.has(value.id))
      .map(([data, multiplicity]) => {
        console.log(
          `DistinctOperator(${this.id})`,
          'data',
          data,
          'multiplicity',
          multiplicity,
        );

        return [data, Math.sign(multiplicity)];
      });

    console.log(`mv(${this.id})`, mv);

    // Create a new Multiset with the distinct values.
    const distinctMultiset = new Multiset(mv as any);

    // Mark the values as emitted.
    for (const [value] of distinctMultiset.entries) {
      emitted.add(value.id);
    }

    console.log(`emitted(${this.id})`, [...emitted]);

    // TODO(arv): What about the Reply?
    this._output.queueData([version, distinctMultiset]);
  }
}
