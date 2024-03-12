import {Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {UnaryOperator} from './unary-operator.js';

/**
 * Runs an effect _after_ a transaction has been committed.
 */
export class DebugOperator<T> extends UnaryOperator<T, T> {
  constructor(
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    onMessage: (c: Multiset<T>) => void,
  ) {
    const inner = (version: Version) => {
      for (const collection of this.inputMessages(version)) {
        onMessage(collection);
        this._output.queueData([version, collection]);
      }
    };
    super(input, output, inner);
  }
}
