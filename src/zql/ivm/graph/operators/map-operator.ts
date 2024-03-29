import {Multiset} from '../../multiset.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {LinearUnaryOperator} from './linear-unary-operator.js';

export class MapOperator<I, O> extends LinearUnaryOperator<I, O> {
  constructor(
    input: DifferenceStreamReader<I>,
    output: DifferenceStreamWriter<O>,
    f: (input: I) => O,
  ) {
    const inner = (collection: Multiset<I>) => {
      return collection.map(f);
    };
    super(input, output, inner);
  }
}
