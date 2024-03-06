import {Multiset} from '../../Multiset.js';
import {DifferenceStreamReader} from '../DifferenceStreamReader.js';
import {DifferenceStreamWriter} from '../DifferenceStreamWriter.js';
import {LinearUnaryOperator} from './LinearUnaryOperator.js';

export class LinearCountOperator<V> extends LinearUnaryOperator<V, number> {
  #state: number = 0;
  constructor(
    input: DifferenceStreamReader<V>,
    output: DifferenceStreamWriter<number>,
  ) {
    const inner = (collection: Multiset<V>) => {
      for (const e of collection.entries) {
        this.#state += e[1];
      }
      return new Multiset([[this.#state, 1]]);
    };
    super(input, output, inner);
  }
}
