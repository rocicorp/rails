import {Multiset} from '../../Multiset.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../DifferenceStreamReader.js';
import {DifferenceStreamWriter} from '../DifferenceStreamWriter.js';
import {UnaryOperator} from './UnaryOperator.js';

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
      this._output.notify(v);
    };
    super(input, output, inner);
  }
}
