import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {QueueEntry} from '../queue.js';
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
      // console.log('ConcatOperator inner', {version});
      for (const input of this._inputs) {
        for (const entry of input.drain(version)) {
          // console.log('ConcatOperator entry', 'version', entry[0], 'entries', [
          //   ...entry[1].entries,
          // ]);
          this._output.queueData(
            entry.length === 3
              ? ([version, entry[1], entry[2]] as QueueEntry<T>)
              : ([version, entry[1]] as QueueEntry<T>),
          );
        }
      }
    };
    super(inputs, output, inner);
  }
}
