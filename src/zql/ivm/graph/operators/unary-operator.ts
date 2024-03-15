import {Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {Operator} from './operator.js';

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
