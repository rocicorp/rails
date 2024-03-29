import {Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {OperatorBase} from './operator.js';

/**
 * A dataflow operator (node) that has many incoming edges (read handles) and
 * one outgoing edge (write handle). It just puts all the input messages from
 * all the incoming streams into the output stream.
 */
export class ConcatOperator<T> extends OperatorBase<T> {
  constructor(
    inputs: DifferenceStreamReader<T>[],
    output: DifferenceStreamWriter<T>,
  ) {
    const inner = (version: Version) => {
      const output = new Multiset<T>([]);
      for (const input of this._inputs) {
        for (const entry of (input as DifferenceStreamReader<T>).drain(
          version,
        )) {
          output.extend(entry[1]);
        }
      }

      this._output.queueData([version, output]);
    };
    super(inputs, output, inner);
  }
}
