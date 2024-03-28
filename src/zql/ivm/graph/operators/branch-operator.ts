import {invariant} from '../../../error/asserts.js';
import {Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {Request} from '../message.js';
import {QueueEntry} from '../queue.js';
import {Operator} from './operator.js';

/**
 * A dataflow operator (node) that has a single incoming edge (read handle) and
 * many outgoing edges (write handles). It just puts all the input messages from
 * the incoming stream into all the outgoing streams.
 */
export class BranchOperator<T> implements Operator {
  #lastRunVersion: Version = -1;
  #lastNotifyVersion: Version = -1;
  protected readonly _input: DifferenceStreamReader<T>;
  protected readonly _outputs: DifferenceStreamWriter<T>[];

  constructor(
    input: DifferenceStreamReader<T>,
    outputs: DifferenceStreamWriter<T>[],
  ) {
    this._input = input;
    this._outputs = outputs;
    this._input.setOperator(this);
    for (const output of this._outputs) {
      output.setOperator(this);
    }
  }

  run(version: Version) {
    this.#lastRunVersion = version;

    for (const entry of this._input.drain(version)) {
      const data: QueueEntry<T> =
        entry.length === 3
          ? [version, entry[1] as Multiset<T>, entry[2]]
          : [version, entry[1] as Multiset<T>];

      for (const output of this._outputs) {
        output.queueData(data);
      }
    }
  }

  notify(version: Version) {
    invariant(version === this.#lastRunVersion, 'notify called out of order');
    if (version === this.#lastNotifyVersion) {
      // Don't double-notify an operator.
      // It will have run and pulled values on the first notification at this version.
      return;
    }
    for (const output of this._outputs) {
      output.notify(version);
    }
  }

  notifyCommitted(version: Version) {
    for (const output of this._outputs) {
      output.notifyCommitted(version);
    }
  }

  destroy() {
    this._input.destroy();
  }

  messageUpstream(message: Request): void {
    this._input.messageUpstream(message);
  }
}
