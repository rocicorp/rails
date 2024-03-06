import {Multiset} from '../../Multiset.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../DifferenceStreamReader.js';
import {DifferenceStreamWriter} from '../DifferenceStreamWriter.js';
import {UnaryOperator} from './UnaryOperator.js';

/**
 * Runs an effect _after_ a transaction has been committed.
 * Does not observe deleted values.
 */
export class EffectOperator<T> extends UnaryOperator<T, T> {
  readonly #f: (input: T) => void;
  #collected: Multiset<T>[] = [];

  constructor(
    input: DifferenceStreamReader<T>,
    output: DifferenceStreamWriter<T>,
    f: (input: T) => void,
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
        if (mult > 0) {
          this.#f(val);
        }
      }
    }
    this._output.notifyCommitted(v);
  }
}
