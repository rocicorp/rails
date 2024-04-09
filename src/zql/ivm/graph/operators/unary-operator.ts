import {Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStream, Listener} from '../difference-stream.js';
import {Request} from '../message.js';
import {OperatorBase} from './operator.js';

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
    fn: (version: Version, data: Multiset<I>) => Multiset<O>,
  ) {
    super(output);
    this.#listener = {
      newDifference: (version, data) => {
        output.newDifference(version, fn(version, data));
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
