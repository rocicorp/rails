import {invariant} from '../../../error/asserts.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {Request} from '../message.js';

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
export interface Operator {
  /**
   * Run the operator
   */
  run(version: Version): void;
  /**
   * Notify along the output edges of the operator
   * that the operator has run
   */
  notify(v: Version): void;
  /**
   * Notify along the graph that the transaction
   * has been comitted
   */
  notifyCommitted(v: Version): void;
  messageUpstream(message: Request): void;
  destroy(): void;
}

export class NoOp implements Operator {
  readonly #inputs;

  constructor(...input: DifferenceStreamReader<unknown>[]) {
    this.#inputs = input;
  }
  run(version: Version) {
    for (const input of this.#inputs) {
      input.drain(version);
    }
  }
  notify(_v: Version) {}
  notifyCommitted(_v: Version): void {}
  messageUpstream(): void {}
  destroy() {}
}

/**
 * A dataflow operator (node) that has many incoming edges (read handles) and one outgoing edge (write handle).
 */
export abstract class OperatorBase<O> implements Operator {
  readonly #fn;
  #lastRunVersion: Version = -1;
  #lastNotifyVersion: Version = -1;
  // upstream inputs
  protected readonly _inputs: DifferenceStreamReader[];
  // downstream output
  protected readonly _output: DifferenceStreamWriter<O>;

  constructor(
    inputs: DifferenceStreamReader[],
    output: DifferenceStreamWriter<O>,
    fn: (v: Version) => void,
  ) {
    this.#fn = fn;
    this._inputs = inputs;
    this._output = output;
    for (const input of this._inputs) {
      input.setOperator(this);
    }
    this._output.setOperator(this);
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

  destroy() {
    for (const input of this._inputs) {
      input.destroy();
    }
  }

  messageUpstream(message: Request): void {
    for (const input of this._inputs) {
      input.messageUpstream(message);
    }
  }
}
