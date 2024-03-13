import {Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {UnaryOperator} from './unary-operator.js';

/**
 * Runs an effect _after_ a transaction has been committed.
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
      for (const entry of this.inputMessages(version)) {
        this.#collected.push(entry[1]);
        this._output.queueData(entry);
      }
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
