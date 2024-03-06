import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../DifferenceStreamReader.js';
import {DifferenceStreamWriter} from '../DifferenceStreamWriter.js';

export interface IOperator {
  run(version: Version): void;
  notifyCommitted(v: Version): void;
}

export class NoOp implements IOperator {
  run(_version: Version) {}

  notifyCommitted(_v: Version): void {}
}

/**
 * A dataflow operator (node) that has many incoming edges (read handles) and one outgoing edge (write handle).
 */
export class Operator<O> implements IOperator {
  readonly #fn;

  constructor(
    protected readonly _inputs: DifferenceStreamReader[],
    protected readonly _output: DifferenceStreamWriter<O>,
    fn: (v: Version) => void,
  ) {
    this.#fn = fn;
    for (const input of this._inputs) {
      input.setOperator(this);
    }
  }

  run(v: Version) {
    this.#fn(v);
  }

  notifyCommitted(v: Version) {
    this._output.notifyCommitted(v);
  }
}
