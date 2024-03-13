import {DifferenceStreamReader} from '../graph/difference-stream-reader.js';
import {DifferenceStream} from '../graph/difference-stream.js';
import {Version} from '../types.js';
import {Request} from '../graph/message.js';

export interface Source<T> {
  readonly stream: DifferenceStream<T>;
  add(value: T): this;
  delete(value: T): this;
  processMessage(message: Request, downstream: DifferenceStreamReader<T>): void;
  seed(values: Iterable<T>): this;
}

export interface SourceInternal {
  // Add values to queues
  onCommitEnqueue(version: Version): void;
  // Drain queues and run the reactive graph
  // TODO: we currently can't rollback a transaction in phase 2
  // as in, if an operator fails in the graph we're in a partial state that can't be reverted.
  // We can resolve this in a few ways:
  // 1. Record the last set of values that made it into a view so we can invert them on rollback
  // 2. Have all views backed by persistent data structures so we can roll back to the prior version
  // 3. What about operators that have memory? E.g., join and reduce?
  onCommitRun(version: Version): void;
  // Now that the graph has computed itself fully, notify effects / listeners
  onCommitted(version: Version): void;
  onRollback(): void;
}
