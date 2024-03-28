import {Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {UnaryOperator} from './unary-operator.js';

/**
 * Linear operator in that:
 * L(a + b) = L(a) + L(b)
 *
 * In other words, we can compute L(b - a) without computing L(a) first. `a`
 * represents some prior state of the database. `b - a` is a diff to the database.
 *
 * - map, filter, concat are linear
 *
 * Reduce and join are non-linear since they must use the prior state `a`, not just the diff `b - a`, in their
 * computations.
 */
export class LinearUnaryOperator<I, O> extends UnaryOperator<I, O> {
  constructor(
    input: DifferenceStreamReader<I>,
    output: DifferenceStreamWriter<O>,
    f: (input: Multiset<I>) => Multiset<O>,
  ) {
    const inner = (v: Version) => {
      for (const entry of this.inputMessages(v)) {
        if (entry.length === 3) {
          this._output.queueData([v, f(entry[1]), entry[2]]);
        } else {
          this._output.queueData([v, f(entry[1])]);
        }
      }
    };
    super(input, output, inner);
  }
}
