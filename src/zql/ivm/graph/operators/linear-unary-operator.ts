import {Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {UnaryOperator} from './unary-operator.js';

export class LinearUnaryOperator<I, O> extends UnaryOperator<I, O> {
  constructor(
    input: DifferenceStreamReader<I>,
    output: DifferenceStreamWriter<O>,
    f: (input: Multiset<I>) => Multiset<O>,
  ) {
    const inner = (v: Version) => {
      for (const collection of this.inputMessages(v)) {
        this._output.queueData([v, f(collection)]);
      }
    };
    super(input, output, inner);
  }
}
