import {DifferenceStream} from '../graph/DifferenceStream.js';
import {Version} from '../types.js';

export interface Source<T> {
  readonly stream: DifferenceStream<T>;
  add(value: T): this;
  delete(value: T): this;
}

export interface SourceInternal {
  // Add values to queues
  onCommitPhase1(version: Version): void;
  // Drain queues and run the reactive graph
  // TODO: we currently can't rollback a transaction in phase 2
  // as in, if an operator fails in the graph we're in a partial state that can't be reverted.
  onCommitPhase2(version: Version): void;
  // Now that the graph has computed itself fully, notify effects / listeners
  onCommitted(version: Version): void;
  onRollback(): void;
}
