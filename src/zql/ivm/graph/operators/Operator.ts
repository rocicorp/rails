import {invariant} from '../../../error/InvariantViolation.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../DifferenceStreamReader.js';
import {DifferenceStreamWriter} from '../DifferenceStreamWriter.js';

/**
 * We have to run the graph breadth-first to ensure
 * that operators with multiple inputs have all of their inputs
 * ready before they are run.
 *
 * To do this, we split running of an operator into `run` and `notify` phases.
 *
 * `run` runs the operators and enqueues its output to the next level of
 * the graph.
 *
 * `notify` notifies the next level of the graph.
 *
 * Operators will pull from their queues in the `notify` phase.
 * If an operator with many inputs is notified, it will ignore subsequent notifications
 * for the same round (version).
 *
 * Version is incremented once per transaction.
 *
 * A single MultiSet is sent per transaction and contains all the values
 * accumulated in that transaction.
 */
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
