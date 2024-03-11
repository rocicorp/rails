import {Multiset} from '../../multiset.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {LinearUnaryOperator} from './linear-unary-operator.js';

export class FilterOperator<I> extends LinearUnaryOperator<I, I> {
  constructor(
    input: DifferenceStreamReader<I>,
    output: DifferenceStreamWriter<I>,
    f: (input: I) => boolean,
  ) {
    const inner = (collection: Multiset<I>) => collection.filter(f);
    super(input, output, inner);
  }
}
