import {filterIter, mapIter} from '../../../util/iterables.js';
import {Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {UnaryOperator} from './unary-operator.js';

export class DistinctOperator<T> extends UnaryOperator<T, T> {
  constructor(
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
  ) {
    super(input, output, version => this.#run(version));
  }

  #run(version: Version) {
    type State<T> = [T, number][];

    function addValue(state: State<T>, value: T, count: number) {
      // TODO(arv): Use json equality
      // TODO(arv): Use custom map impl where the key is json value?
      const existing = state.find(e => e[0] === value);
      if (!existing) {
        state.push([value, count]);
        return;
      }
      // Mutate in place
      existing[1] += count;
    }

    const state: State<T> = [];
    for (const queueEntry of this.inputMessages(version)) {
      // TODO(arv): What about the Reply?
      const multiset = queueEntry[1];
      for (const entry of multiset.entries) {
        addValue(state, entry[0], entry[1]);
      }
    }

    // Create a new Multiset with the distinct values.
    const distinctMultiset = new Multiset(
      mapIter(
        filterIter(state, e => e[1] !== 0),
        ([data, multiplicity]) => [data, Math.sign(multiplicity)],
      ),
    );

    // TODO(arv): What about the Reply?
    this._output.queueData([version, distinctMultiset]);
  }
}
