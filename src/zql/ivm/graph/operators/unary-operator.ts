import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {OperatorBase} from './operator.js';
import {QueueEntry} from '../queue.js';

/**
 * Operator that only takes a single argument
 */
export class UnaryOperator<I, O> extends OperatorBase<O> {
  constructor(
    input: DifferenceStreamReader<I>,
    output: DifferenceStreamWriter<O>,
    fn: (e: Version) => void,
  ) {
    super([input], output, fn);
  }

  inputMessages(version: Version) {
    return (this._inputs[0]?.drain(version) ?? []) as QueueEntry<I>[];
  }
}
