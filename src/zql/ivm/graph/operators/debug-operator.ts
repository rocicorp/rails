import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {QueueEntry} from '../queue.js';
import {UnaryOperator} from './unary-operator.js';

/**
 * Runs an effect _after_ a transaction has been committed.
 */
export class DebugOperator<T> extends UnaryOperator<T, T> {
  constructor(
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    onMessage: (c: QueueEntry<T>) => void,
  ) {
    const inner = (version: Version) => {
      for (const entry of this.inputMessages(version)) {
        onMessage(entry);
        this._output.queueData(entry);
      }
    };
    super(input, output, inner);
  }
}
