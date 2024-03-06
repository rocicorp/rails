import {Multiset} from '../../Multiset.js';
import {DifferenceStreamReader} from '../DifferenceStreamReader.js';
import {DifferenceStreamWriter} from '../DifferenceStreamWriter.js';
import {LinearUnaryOperator} from './LinearUnaryOperator.js';

export class MapOperator<I, O> extends LinearUnaryOperator<I, O> {
  constructor(
    input: DifferenceStreamReader<I>,
    output: DifferenceStreamWriter<O>,
    f: (input: I) => O,
  ) {
    const inner = (collection: Multiset<I>) => collection.map(f);
    super(input, output, inner);
  }
}
