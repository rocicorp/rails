import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {QueueEntry} from '../queue.js';
import {OperatorBase} from './operator.js';

export class BinaryOperator<I1, I2, O> extends OperatorBase<O> {
  constructor(
    input1: DifferenceStreamReader<I1>,
    input2: DifferenceStreamReader<I2>,
    output: DifferenceStreamWriter<O>,
    fn: (v: Version) => void,
  ) {
    super([input1, input2], output, fn);
  }

  inputAMessages(version: Version) {
    return (this._inputs[0]?.drain(version) ?? []) as QueueEntry<I1>[];
  }

  inputBMessages(version: Version) {
    return (this._inputs[1]?.drain(version) ?? []) as QueueEntry<I2>[];
  }
}
