import {Version} from '../../types.js';
import {DifferenceStream} from '../difference-stream.js';
import {Request} from '../message.js';

export interface Operator {
  /**
   * Notify along the graph that the transaction
   * has been comitted
   */
  commit(version: Version): void;
  messageUpstream(message: Request): void;
  destroy(): void;
}

export class NoOp implements Operator {
  constructor() {}
  commit(_v: Version): void {}
  messageUpstream(): void {}
  destroy() {}
}

/**
 * A dataflow operator (node) that has many incoming edges (stream) and one outgoing edge (stream).
 */
export abstract class OperatorBase<O extends object> implements Operator {
  // upstream inputs
  protected readonly _inputs: DifferenceStream<object>[];
  // downstream output
  protected readonly _output: DifferenceStream<O>;

  constructor(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    inputs: DifferenceStream<any>[],
    output: DifferenceStream<O>,
  ) {
    this._inputs = inputs;
    this._output = output;
  }

  commit(v: Version) {
    this._output.commit(v);
  }

  abstract messageUpstream(message: Request): void;
  abstract destroy(): void;
}
