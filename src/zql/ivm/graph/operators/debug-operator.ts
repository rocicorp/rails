import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {QueueEntry} from '../queue.js';
import {UnaryOperator} from './unary-operator.js';

/**
 * Allows someone to observe all data flowing through a spot
 * in a pipeline. Forwards the data with no changes made to it.
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
