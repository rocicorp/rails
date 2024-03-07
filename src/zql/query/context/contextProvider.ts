import {Materialite} from '../../ivm/Materialite.js';
import {ISource} from '../../ivm/source/ISource.js';
import {ReadTransaction} from '../../../generate.js';

export type Context = {
  materialite: Materialite;
  getSource: (name: string) => ISource<unknown>;
  destroy: () => void;
};

export function makeTestContext() {
  const materialite = new Materialite();
  const sources = new Map<string, ISource<unknown>>();
  const getSource = (name: string) => {
    if (!sources.has(name)) {
      sources.set(name, materialite.newStatelessSource());
    }
    return sources.get(name)!;
  };
  return {materialite, getSource, destroy() {}};
}

const replicacheContexts = new Map<string, Context>();
export function getReplicacheContext(tx: ReadTransaction): Context {
  let existing = replicacheContexts.get(tx.clientID);
  if (!existing) {
    existing = {
      materialite: new Materialite(),
      getSource: name => {
        throw new Error(`Source not found: ${name}`);
      },
      destroy() {
        replicacheContexts.delete(tx.clientID);
      },
    };
    replicacheContexts.set(tx.clientID, existing);
  }

  return existing;
}
