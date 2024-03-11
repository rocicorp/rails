import {Multiset} from '../../multiset.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {LinearUnaryOperator} from './linear-unary-operator.js';

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
