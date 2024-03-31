import {Entry, genFilter} from '../../multiset.js';
import {DifferenceStream} from '../difference-stream.js';
import {LinearUnaryOperator} from './linear-unary-operator.js';

export class FilterOperator<I extends object> extends LinearUnaryOperator<
  I,
  I
> {
  constructor(
    input: DifferenceStream<I>,
    output: DifferenceStream<I>,
    f: (input: I) => boolean,
  ) {
    super(input, output, (data: Iterable<Entry<I>>) =>
      genFilter(data, ([value, _]) => f(value)),
    );
  }
}
