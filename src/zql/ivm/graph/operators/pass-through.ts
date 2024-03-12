import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {UnaryOperator} from './unary-operator.js';

export class PassThroughOperator<I> extends UnaryOperator<I, I> {
  constructor(
    input: DifferenceStreamReader<I>,
    output: DifferenceStreamWriter<I>,
  ) {
    const inner = (v: Version) => {
      for (const collection of this.inputMessages(v)) {
        this._output.queueData([v, collection]);
      }
    };
    super(input, output, inner);
  }
}
