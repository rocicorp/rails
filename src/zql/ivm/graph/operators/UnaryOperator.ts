import {Multiset} from '../../Multiset.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../DifferenceStreamReader.js';
import {DifferenceStreamWriter} from '../DifferenceStreamWriter.js';
import {Operator} from './Operator.js';

export class UnaryOperator<I, O> extends Operator<O> {
  constructor(
    input: DifferenceStreamReader<I>,
    output: DifferenceStreamWriter<O>,
    fn: (e: Version) => void,
  ) {
    super([input], output, fn);
  }

  inputMessages(version: Version) {
    return (this._inputs[0]?.drain(version) ?? []) as Multiset<I>[];
  }
}
