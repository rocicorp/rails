import {Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {UnaryOperator} from './unary-operator.js';

/**
 * Runs an effect _after_ a transaction has been committed.
 *
 * This is intended to let users introduce side-effects
 * to be run on changes to a query without having to materialize the query
 * results.
 */
export class DifferenceEffectOperator<T> extends UnaryOperator<T, T> {
  readonly #f: (input: T, mult: number) => void;
  #collected: Multiset<T>[] = [];

  constructor(
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    f: (input: T, mult: number) => void,
  ) {
    const inner = (version: Version) => {
      this.#collected = [];
      const entry = this.inputMessages(version);
      this.#collected.push(entry[1]);
      this._output.queueData(entry);
    };
    super(input, output, inner);
    this.#f = f;
  }

  notifyCommitted(v: number): void {
    for (const collection of this.#collected) {
      for (const [val, mult] of collection.entries) {
        this.#f(val, mult);
      }
    }
    this._output.notifyCommitted(v);
  }
}
