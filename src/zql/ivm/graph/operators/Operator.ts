import {invariant} from '../../../error/InvariantViolation.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../DifferenceStreamReader.js';
import {DifferenceStreamWriter} from '../DifferenceStreamWriter.js';

export interface IOperator {
  run(version: Version): void;
  notify(v: Version): void;
  notifyCommitted(v: Version): void;
}

export class NoOp implements IOperator {
  run(_version: Version) {}
  notify(_v: Version) {}
  notifyCommitted(_v: Version): void {}
}

/**
 * A dataflow operator (node) that has many incoming edges (read handles) and one outgoing edge (write handle).
 */
export class Operator<O> implements IOperator {
  readonly #fn;
  #lastRunVersion: Version = -1;
  #lastNotifyVersion: Version = -1;

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
    this.#lastRunVersion = v;
    this.#fn(v);
  }

  notify(v: Version) {
    invariant(v === this.#lastRunVersion, 'notify called out of order');
    if (v === this.#lastNotifyVersion) {
      // Don't double-notify an operator.
      // It will have run and pulled values on the first notification at this version.
      return;
    }
    this._output.notify(v);
  }

  notifyCommitted(v: Version) {
    this._output.notifyCommitted(v);
  }
}
