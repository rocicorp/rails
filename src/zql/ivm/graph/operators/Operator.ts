import {Version} from '../../types.js';

export interface IOperator {
  run(version: Version): void;
  notifyCommitted(v: Version): void;
}

export class NoOp implements IOperator {
  run(_version: Version) {}

  notifyCommitted(_v: Version): void {}
}
