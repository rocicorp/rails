import {Multiset} from '../../Multiset.js';
import {DifferenceStreamReader} from '../DifferenceStreamReader.js';
import {DifferenceStreamWriter} from '../DifferenceStreamWriter.js';
import {LinearUnaryOperator} from './LinearUnaryOperator.js';

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
