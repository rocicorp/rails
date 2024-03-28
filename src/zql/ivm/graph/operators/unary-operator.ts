import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {QueueEntry} from '../queue.js';
import {OperatorBase} from './operator.js';

export class UnaryOperator<I, O> extends OperatorBase<O> {
  constructor(
    input: DifferenceStreamReader<I>,
    output: DifferenceStreamWriter<O>,
    fn: (e: Version) => void,
  ) {
    super([input], output, fn);
  }

  inputMessages(version: Version): QueueEntry<I>[] {
    return (this._inputs[0]?.drain(version) ?? []) as QueueEntry<I>[];
  }
}
