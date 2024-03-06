import {Version} from '../types.js';

export interface ISourceInternal {
  // Add values to queues
  onCommitPhase1(version: Version): void;
  // Drain queues and run the reactive graph
  // TODO: we currently can't rollback a transaction in phase 2
  onCommitPhase2(version: Version): void;
  // Now that the graph has computed itself fully, notify effects / listeners
  onCommitted(version: Version): void;
  onRollback(): void;
}
