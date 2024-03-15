import {Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {UnaryOperator} from './unary-operator.js';

/**
 * Runs an effect _after_ a transaction has been committed.
 * Does not observe deleted values.
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
      for (const collection of this.inputMessages(version)) {
        this.#collected.push(collection);
        this._output.queueData([version, collection]);
      }
      this._output.notify(version);
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
