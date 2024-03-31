import {Version} from '../../types.js';
import {OperatorBase} from './operator.js';
import {DifferenceStream, Listener} from '../difference-stream.js';
import {Entry} from '../../multiset.js';
import {Request} from '../message.js';

/**
 * Operator that only takes a single argument
 */
export class UnaryOperator<
  I extends object,
  O extends object,
> extends OperatorBase<O> {
  readonly #listener: Listener<I>;
  readonly #input: DifferenceStream<I>;

  constructor(
    input: DifferenceStream<I>,
    output: DifferenceStream<O>,
    fn: (version: Version, data: Iterable<Entry<I>>) => Iterable<Entry<O>>,
  ) {
    super([input], output);
    this.#listener = {
      newData: (version, data) => {
        output.newData(version, fn(version, data));
      },
      commit: version => {
        this.commit(version);
      },
    };
    input.addDownstream(this.#listener);
    this.#input = input;
  }

  messageUpstream(message: Request): void {
    this.#input.messageUpstream(message, this.#listener);
  }

  destroy() {
    this.#input.removeDownstream(this.#listener);
  }
}
